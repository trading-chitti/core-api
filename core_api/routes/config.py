"""Configuration management routes."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

def get_db_connection():
    """Get PostgreSQL connection."""
    dsn = os.getenv(
        "TRADING_CHITTI_PG_DSN",
        "postgresql://hariprasath@localhost:6432/trading_chitti"
    )
    return psycopg2.connect(dsn)


class IntradayModeRequest(BaseModel):
    enabled: bool


@router.get("")
async def get_config() -> Dict[str, Any]:
    """Get all configuration settings."""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT key, value, updated_at
        FROM md.system_config
    """)

    results = cur.fetchall()
    cur.close()
    conn.close()

    config = {}
    for row in results:
        key = row[0]
        value = row[1]

        # Try to parse boolean values
        if value.lower() in ('true', 'false'):
            config[key] = value.lower() == 'true'
        else:
            config[key] = value

    return {
        "intraday_mode": config.get("ITS_OUR_INTRADAY", False),
        "data_sources": {
            "nse_market_data": True,
            "news_rss_feeds": True,
            "historical_data": True,
        },
        "performance": {
            "mojo_acceleration": True,
            "max_engine": True,
            "bert_model": True,
        }
    }


@router.post("/intraday-mode")
async def toggle_intraday_mode(request: IntradayModeRequest) -> Dict[str, Any]:
    """Toggle intraday mode."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO md.system_config (key, value, updated_at)
            VALUES ('ITS_OUR_INTRADAY', %s, NOW())
            ON CONFLICT (key)
            DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """, (str(request.enabled),))

        conn.commit()

        return {
            "success": True,
            "intraday_mode": request.enabled,
            "message": f"Intraday mode {'enabled' if request.enabled else 'disabled'}"
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()


# ==================================================================
# Smart Stock Selection Configuration Endpoints
# ==================================================================

class SmartSelectionToggleRequest(BaseModel):
    enabled: bool


class SmartSelectionToggleResponse(BaseModel):
    success: bool
    enabled: bool
    message: str
    updated_at: Optional[str] = None


class SmartSelectionStatusResponse(BaseModel):
    enabled: bool
    stock_count: int = 200
    updated_at: Optional[str] = None


class StockCountsResponse(BaseModel):
    total_enabled: int
    zerodha_count: int
    indmoney_count: int
    ml_selected: int
    wildcard_count: int
    manual_count: int


class SmartSelectionStockCountRequest(BaseModel):
    count: int


class SmartSelectionStockCountResponse(BaseModel):
    success: bool
    count: int
    message: str


def get_smart_selection_stock_count() -> int:
    """Get configured number of stocks to select in Smart Mode"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT config_value
            FROM md.system_config
            WHERE config_key = 'smart_selection_stock_count';
        """)
        row = cur.fetchone()
        conn.close()
        if row:
            return int(row[0])
        return 200
    except Exception as e:
        logger.error(f"Error getting stock count: {e}")
        return 200


def is_smart_selection_enabled() -> bool:
    """Check if smart selection is enabled"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT config_value
            FROM md.system_config
            WHERE config_key = 'smart_stock_selection_enabled';
        """)
        row = cur.fetchone()
        conn.close()

        if row:
            return row[0].lower() == 'true'
        return False

    except Exception as e:
        logger.error(f"Error checking smart selection status: {e}")
        return False


def apply_manual_marketcap_mode():
    """
    Manual Mode: Enable ALL stocks based on market cap (Large + Mid + Small)
    and split equally between Zerodha and IndMoney fetchers.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        logger.info("Applying Manual Mode (market cap based selection)...")

        # Step 1: Disable ALL stocks first
        cur.execute("""
            UPDATE md.stock_config
            SET intraday_enabled = FALSE
            WHERE active = TRUE;
        """)

        # Step 2: Get all stocks with Large, Mid, Small market cap
        cur.execute("""
            SELECT symbol, fetcher
            FROM md.stock_config
            WHERE active = TRUE
              AND market_cap_category IN ('Large', 'Mid', 'Small')
              AND fetcher IN ('ZERODHA', 'INDMONEY', 'PAYTM')
            ORDER BY symbol;
        """)
        stocks = cur.fetchall()

        # Step 3: Enable them (preserves existing fetcher allocation)
        symbols = [s[0] for s in stocks]

        cur.execute("""
            UPDATE md.stock_config
            SET intraday_enabled = TRUE
            WHERE symbol = ANY(%s);
        """, (symbols,))

        conn.commit()

        # Get counts
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE fetcher = 'ZERODHA') as zerodha_count,
                COUNT(*) FILTER (WHERE fetcher = 'INDMONEY') as indmoney_count,
                COUNT(*) as total
            FROM md.stock_config
            WHERE active = TRUE
              AND intraday_enabled = TRUE;
        """)
        counts = cur.fetchone()

        conn.close()

        logger.info(f"✅ Manual Mode applied: {counts[2]} stocks enabled")
        logger.info(f"   Zerodha: {counts[0]}, IndMoney: {counts[1]}")

        return {
            'total': counts[2],
            'zerodha': counts[0],
            'indmoney': counts[1]
        }

    except Exception as e:
        logger.error(f"Error applying manual mode: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise


def run_smart_stock_selection(target_count: int = None):
    """
    Run ML-based stock selection immediately.
    Scores all stocks and picks top N (split equally between fetchers).
    Called when Smart Mode is toggled ON (and also by 8:45 AM CRON).
    """
    if target_count is None:
        target_count = get_smart_selection_stock_count()
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        logger.info("Running smart stock selection (on-demand)...")

        # Step 1: Calculate ML scores for all eligible stocks
        cur.execute("""
            WITH recent_sentiment AS (
                SELECT
                    symbol,
                    AVG(avg_sentiment) as sentiment_7d,
                    SUM(article_count) as article_count_7d
                FROM news.daily_sentiment
                WHERE date >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY symbol
            ),
            signal_success AS (
                SELECT
                    symbol,
                    COUNT(CASE WHEN status = 'HIT_TARGET' THEN 1 END)::float /
                    NULLIF(COUNT(*), 0) * 100 as success_rate_30d
                FROM intraday.signals
                WHERE generated_at >= CURRENT_DATE - INTERVAL '30 days'
                  AND status IN ('HIT_TARGET', 'HIT_STOPLOSS')
                GROUP BY symbol
            )
            SELECT
                s.symbol,
                s.fetcher,
                COALESCE(
                    (CASE
                        WHEN COALESCE(rs.sentiment_7d, 0) > 0.5 THEN 40
                        WHEN COALESCE(rs.sentiment_7d, 0) > 0.3 THEN 30
                        WHEN COALESCE(rs.sentiment_7d, 0) > 0.1 THEN 20
                        WHEN COALESCE(rs.sentiment_7d, 0) > -0.1 THEN 15
                        WHEN COALESCE(rs.sentiment_7d, 0) > -0.3 THEN 10
                        ELSE 5
                    END) +
                    (CASE
                        WHEN s.market_cap_category = 'Large' THEN 30
                        WHEN s.market_cap_category = 'Mid' THEN 20
                        WHEN s.market_cap_category = 'Small' THEN 10
                        ELSE 5
                    END) +
                    (COALESCE(ss.success_rate_30d, 50) * 0.3),
                    50.0
                ) as ml_score
            FROM md.stock_config s
            LEFT JOIN recent_sentiment rs ON s.symbol = rs.symbol
            LEFT JOIN signal_success ss ON s.symbol = ss.symbol
            WHERE s.active = TRUE
              AND s.market_cap_category IN ('Large', 'Mid', 'Small')
              AND s.fetcher IN ('ZERODHA', 'INDMONEY', 'PAYTM')
            ORDER BY ml_score DESC, s.symbol ASC;
        """)
        results = cur.fetchall()

        if not results:
            conn.close()
            logger.warning("No stocks available for ML scoring")
            return {'total': 0, 'zerodha': 0, 'indmoney': 0}

        # Step 2: Select top N (split equally between fetchers)
        zerodha_stocks = []
        indmoney_stocks = []
        target_per_fetcher = target_count // 2

        for symbol, fetcher, score in results:
            if len(zerodha_stocks) >= target_per_fetcher and len(indmoney_stocks) >= target_per_fetcher:
                break
            if fetcher == 'ZERODHA' and len(zerodha_stocks) < target_per_fetcher:
                zerodha_stocks.append(symbol)
            elif fetcher == 'INDMONEY' and len(indmoney_stocks) < target_per_fetcher:
                indmoney_stocks.append(symbol)
            elif fetcher == 'PAYTM':
                if len(zerodha_stocks) <= len(indmoney_stocks):
                    if len(zerodha_stocks) < target_per_fetcher:
                        zerodha_stocks.append(symbol)
                else:
                    if len(indmoney_stocks) < target_per_fetcher:
                        indmoney_stocks.append(symbol)

        # Step 3: Clear existing AI picks and apply new ones
        cur.execute("""
            UPDATE md.stock_config
            SET intraday_ai_picked = FALSE, selection_type = NULL
            WHERE intraday_ai_picked = TRUE;
        """)

        if zerodha_stocks:
            cur.execute("""
                UPDATE md.stock_config
                SET intraday_ai_picked = TRUE, selection_type = 'MORNING_ML'
                WHERE symbol = ANY(%s) AND fetcher = 'ZERODHA';
            """, (zerodha_stocks,))

        if indmoney_stocks:
            cur.execute("""
                UPDATE md.stock_config
                SET intraday_ai_picked = TRUE, selection_type = 'MORNING_ML'
                WHERE symbol = ANY(%s) AND fetcher = 'INDMONEY';
            """, (indmoney_stocks,))

        conn.commit()
        conn.close()

        total = len(zerodha_stocks) + len(indmoney_stocks)
        logger.info(f"Smart selection complete: {total} stocks (Zerodha: {len(zerodha_stocks)}, IndMoney: {len(indmoney_stocks)})")

        return {
            'total': total,
            'zerodha': len(zerodha_stocks),
            'indmoney': len(indmoney_stocks)
        }

    except Exception as e:
        logger.error(f"Error running smart stock selection: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise


def clear_ai_selections():
    """Smart Mode → Manual Mode transition: Clear all AI selections"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        logger.info("Clearing AI selections (toggle OFF)...")

        # Clear intraday_ai_picked and selection_type
        cur.execute("""
            UPDATE md.stock_config
            SET intraday_ai_picked = FALSE,
                selection_type = NULL,
                wildcard_expires_at = NULL,
                wildcard_added_at = NULL,
                wildcard_news_id = NULL,
                wildcard_news_headline = NULL
            WHERE intraday_ai_picked = TRUE;
        """)

        conn.commit()
        conn.close()

        logger.info("✅ AI selections cleared")

    except Exception as e:
        logger.error(f"Error clearing AI selections: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise


@router.get("/smart-selection", response_model=SmartSelectionStatusResponse)
async def get_smart_selection_status():
    """
    Get current smart selection toggle state

    Returns:
        enabled: boolean indicating if smart selection is ON
        updated_at: timestamp of last update
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT config_value, updated_at
            FROM md.system_config
            WHERE config_key = 'smart_stock_selection_enabled';
        """)
        row = cur.fetchone()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Configuration not found")

        return SmartSelectionStatusResponse(
            enabled=row[0].lower() == 'true',
            stock_count=get_smart_selection_stock_count(),
            updated_at=row[1].isoformat() if row[1] else None
        )

    except psycopg2.Error as e:
        logger.error(f"Database error in get_smart_selection_status: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.post("/smart-selection", response_model=SmartSelectionToggleResponse)
async def toggle_smart_selection(request: SmartSelectionToggleRequest):
    """
    Toggle smart selection ON or OFF

    - Toggle ON: Enables Smart Mode (ML selects 200 stocks)
    - Toggle OFF: Enables Manual Mode (all market cap stocks)
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Update system configuration
        cur.execute("""
            UPDATE md.system_config
            SET config_value = %s,
                updated_at = NOW(),
                updated_by = 'api'
            WHERE config_key = 'smart_stock_selection_enabled';
        """, ('true' if request.enabled else 'false',))

        conn.commit()

        # Get updated timestamp
        cur.execute("""
            SELECT updated_at
            FROM md.system_config
            WHERE config_key = 'smart_stock_selection_enabled';
        """)
        row = cur.fetchone()
        updated_at = row[0].isoformat() if row and row[0] else None

        conn.close()

        # Apply mode changes
        if request.enabled:
            # Toggle ON: Smart Mode — immediately run stock selection
            logger.info("Smart selection toggled ON - running stock selection now...")
            counts = run_smart_stock_selection()
            message = f"Smart selection enabled. {counts['total']} stocks selected " \
                     f"(Zerodha: {counts['zerodha']}, IndMoney: {counts['indmoney']})"

        else:
            # Toggle OFF: Manual Mode
            logger.info("Smart selection toggled OFF - Manual Mode activated")

            # Clear AI selections
            clear_ai_selections()

            # Apply manual market cap mode
            counts = apply_manual_marketcap_mode()

            message = f"Manual mode enabled. {counts['total']} stocks enabled " \
                     f"(Zerodha: {counts['zerodha']}, IndMoney: {counts['indmoney']})"

        return SmartSelectionToggleResponse(
            success=True,
            enabled=request.enabled,
            message=message,
            updated_at=updated_at
        )

    except psycopg2.Error as e:
        logger.error(f"Database error in toggle_smart_selection: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    except Exception as e:
        logger.error(f"Error in toggle_smart_selection: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/smart-selection/stock-count", response_model=SmartSelectionStockCountResponse)
async def get_smart_selection_stock_count_endpoint():
    """Get the configured number of stocks for Smart Mode selection"""
    count = get_smart_selection_stock_count()
    return SmartSelectionStockCountResponse(
        success=True,
        count=count,
        message=f"Smart Mode selects {count} stocks ({count // 2} per fetcher)"
    )


@router.put("/smart-selection/stock-count", response_model=SmartSelectionStockCountResponse)
async def set_smart_selection_stock_count(request: SmartSelectionStockCountRequest):
    """
    Update the number of stocks to select in Smart Mode.
    If Smart Mode is active, immediately re-runs selection with new count.
    """
    if request.count < 50 or request.count > 2000:
        raise HTTPException(status_code=400, detail="Stock count must be between 50 and 2000")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            UPDATE md.system_config
            SET config_value = %s, updated_at = NOW(), updated_by = 'api'
            WHERE config_key = 'smart_selection_stock_count';
        """, (str(request.count),))
        conn.commit()
        conn.close()

        logger.info(f"Smart selection stock count updated to {request.count}")

        # If Smart Mode is ON, re-run selection with new count
        if is_smart_selection_enabled():
            counts = run_smart_stock_selection(target_count=request.count)
            message = f"Stock count set to {request.count}. Re-selected {counts['total']} stocks " \
                     f"(Zerodha: {counts['zerodha']}, IndMoney: {counts['indmoney']})"
        else:
            message = f"Stock count set to {request.count}. Will apply when Smart Mode is enabled."

        return SmartSelectionStockCountResponse(
            success=True,
            count=request.count,
            message=message
        )

    except psycopg2.Error as e:
        logger.error(f"Database error setting stock count: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    except Exception as e:
        logger.error(f"Error setting stock count: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stock-counts", response_model=StockCountsResponse)
async def get_stock_counts():
    """
    Get current stock configuration statistics
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                COUNT(*) FILTER (
                    WHERE intraday_enabled = TRUE
                ) as total_enabled,

                COUNT(*) FILTER (
                    WHERE intraday_enabled = TRUE
                    AND fetcher = 'ZERODHA'
                ) as zerodha_count,

                COUNT(*) FILTER (
                    WHERE intraday_enabled = TRUE
                    AND fetcher = 'INDMONEY'
                ) as indmoney_count,

                COUNT(*) FILTER (
                    WHERE intraday_ai_picked = TRUE
                ) as ml_selected,

                COUNT(*) FILTER (
                    WHERE selection_type = 'WILDCARD_NEWS'
                    AND wildcard_expires_at > NOW()
                ) as wildcard_count,

                COUNT(*) FILTER (
                    WHERE intraday_enabled = TRUE
                    AND (intraday_ai_picked = FALSE OR intraday_ai_picked IS NULL)
                ) as manual_count

            FROM md.stock_config
            WHERE active = TRUE;
        """)

        row = cur.fetchone()
        conn.close()

        if not row:
            raise HTTPException(status_code=500, detail="Failed to fetch stock counts")

        return StockCountsResponse(
            total_enabled=row[0] or 0,
            zerodha_count=row[1] or 0,
            indmoney_count=row[2] or 0,
            ml_selected=row[3] or 0,
            wildcard_count=row[4] or 0,
            manual_count=row[5] or 0
        )

    except psycopg2.Error as e:
        logger.error(f"Database error in get_stock_counts: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
