"""Routes for stock data and analysis."""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any, Optional
import psycopg2
from datetime import datetime, timedelta
import os

router = APIRouter()


def get_db_connection():
    """Get PostgreSQL connection."""
    dsn = os.getenv(
        "TRADING_CHITTI_PG_DSN",
        "postgresql://hariprasath@localhost:5432/trading_chitti"
    )
    return psycopg2.connect(dsn)


@router.get("/top-gainers")
async def get_top_gainers(limit: int = Query(20, le=100)) -> List[Dict[str, Any]]:
    """Get top predicted gainers with ML confidence scores.

    Returns stocks with highest predicted gains based on ML analysis.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if trades.analysis table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'trades'
                AND table_name = 'analysis'
            )
        """)
        table_exists = cur.fetchone()[0]

        if not table_exists:
            # Database schema not initialized - return empty
            cur.close()
            conn.close()
            return []

        # Get latest analysis with positive predictions
        cur.execute("""
            WITH latest_analysis AS (
                SELECT DISTINCT ON (a.symbol)
                    a.symbol,
                    s.name,
                    a.trend_direction,
                    a.trend_slope,
                    a.confidence,
                    e.close as price,
                    -- Calculate predicted change based on trend slope and confidence
                    (a.trend_slope * a.confidence * 100) as predicted_change
                FROM trades.analysis a
                INNER JOIN md.symbols s ON a.symbol = s.symbol
                INNER JOIN LATERAL (
                    SELECT close
                    FROM md.eod_prices ep
                    WHERE ep.symbol = a.symbol
                    AND ep.exchange = 'NSE'
                    ORDER BY trade_date DESC
                    LIMIT 1
                ) e ON true
                WHERE a.trend_direction IN ('up', 'strong_up')
                AND a.confidence IS NOT NULL
                AND s.active = TRUE
                ORDER BY a.symbol, a.analysis_date DESC
            )
            SELECT
                symbol,
                name,
                predicted_change,
                confidence,
                price
            FROM latest_analysis
            WHERE predicted_change > 0
            ORDER BY predicted_change DESC, confidence DESC
            LIMIT %s
        """, (limit,))

        results = cur.fetchall()
        cur.close()
        conn.close()

        if not results:
            # Return empty list if no predictions available
            return []

        return [
            {
                "symbol": row[0],
                "name": row[1] or row[0],
                "change": round(float(row[2]) if row[2] else 0.0, 2),
                "confidence": round(float(row[3]) if row[3] else 0.0, 2),
                "price": round(float(row[4]) if row[4] else 0.0, 2),
            }
            for row in results
        ]
    except Exception as e:
        # Return empty on error - no mock data
        return []


@router.get("/top-losers")
async def get_top_losers(limit: int = Query(20, le=100)) -> List[Dict[str, Any]]:
    """Get top predicted losers with ML confidence scores.

    Returns stocks with highest predicted losses based on ML analysis.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if trades.analysis table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'trades'
                AND table_name = 'analysis'
            )
        """)
        table_exists = cur.fetchone()[0]

        if not table_exists:
            # Database schema not initialized - return empty
            cur.close()
            conn.close()
            return []

        # Get latest analysis with negative predictions
        cur.execute("""
            WITH latest_analysis AS (
                SELECT DISTINCT ON (a.symbol)
                    a.symbol,
                    s.name,
                    a.trend_direction,
                    a.trend_slope,
                    a.confidence,
                    e.close as price,
                    -- Calculate predicted change (negative for losers)
                    (a.trend_slope * a.confidence * 100) as predicted_change
                FROM trades.analysis a
                INNER JOIN md.symbols s ON a.symbol = s.symbol
                INNER JOIN LATERAL (
                    SELECT close
                    FROM md.eod_prices ep
                    WHERE ep.symbol = a.symbol
                    AND ep.exchange = 'NSE'
                    ORDER BY trade_date DESC
                    LIMIT 1
                ) e ON true
                WHERE a.trend_direction IN ('down', 'strong_down')
                AND a.confidence IS NOT NULL
                AND s.active = TRUE
                ORDER BY a.symbol, a.analysis_date DESC
            )
            SELECT
                symbol,
                name,
                predicted_change,
                confidence,
                price
            FROM latest_analysis
            WHERE predicted_change < 0
            ORDER BY predicted_change ASC, confidence DESC
            LIMIT %s
        """, (limit,))

        results = cur.fetchall()
        cur.close()
        conn.close()

        if not results:
            return []

        return [
            {
                "symbol": row[0],
                "name": row[1] or row[0],
                "change": round(float(row[2]) if row[2] else 0.0, 2),
                "confidence": round(float(row[3]) if row[3] else 0.0, 2),
                "price": round(float(row[4]) if row[4] else 0.0, 2),
            }
            for row in results
        ]
    except Exception as e:
        # Return empty on error - no mock data
        return []


@router.get("/{symbol}")
async def get_stock_data(symbol: str) -> Dict[str, Any]:
    """Get current stock data including price and change."""
    conn = get_db_connection()
    cur = conn.cursor()

    # Get latest price and previous day's price
    cur.execute("""
        WITH latest_prices AS (
            SELECT
                e.symbol,
                s.name,
                e.close,
                e.volume,
                s.market_cap,
                e.trade_date,
                LAG(e.close) OVER (ORDER BY e.trade_date) as prev_close
            FROM md.eod_prices e
            INNER JOIN md.symbols s ON e.symbol = s.symbol AND e.exchange = s.exchange
            WHERE e.symbol = %s
            AND e.exchange = 'NSE'
            ORDER BY e.trade_date DESC
            LIMIT 2
        )
        SELECT
            symbol,
            name,
            close as price,
            COALESCE(close - prev_close, 0) as change,
            CASE
                WHEN prev_close > 0 THEN ((close - prev_close) / prev_close * 100)
                ELSE 0
            END as change_percent,
            volume,
            market_cap
        FROM latest_prices
        ORDER BY trade_date DESC
        LIMIT 1
    """, (symbol,))

    result = cur.fetchone()
    cur.close()
    conn.close()

    if not result:
        raise HTTPException(status_code=404, detail=f"Stock {symbol} not found")

    return {
        "symbol": result[0],
        "name": result[1] or result[0],
        "price": round(float(result[2]) if result[2] else 0.0, 2),
        "change": round(float(result[3]) if result[3] else 0.0, 2),
        "changePercent": round(float(result[4]) if result[4] else 0.0, 2),
        "volume": int(result[5]) if result[5] else 0,
        "marketCap": int(result[6]) if result[6] else 0,
    }


@router.get("/{symbol}/history")
async def get_stock_history(
    symbol: str,
    days: int = Query(365, le=730, description="Number of days of history (max 730)")
) -> List[Dict[str, Any]]:
    """Get historical stock data for the specified number of days.

    Returns OHLCV data for charting and analysis.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            trade_date,
            open,
            high,
            low,
            close,
            volume
        FROM md.eod_prices
        WHERE symbol = %s
        AND exchange = 'NSE'
        AND trade_date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY trade_date ASC
    """, (symbol, days))

    results = cur.fetchall()
    cur.close()
    conn.close()

    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"No historical data found for {symbol}"
        )

    return [
        {
            "date": row[0].isoformat(),
            "open": round(float(row[1]) if row[1] else 0.0, 2),
            "high": round(float(row[2]) if row[2] else 0.0, 2),
            "low": round(float(row[3]) if row[3] else 0.0, 2),
            "close": round(float(row[4]) if row[4] else 0.0, 2),
            "volume": int(row[5]) if row[5] else 0,
        }
        for row in results
    ]


@router.get("/search")
async def search_stocks(
    q: str = Query(..., min_length=1, description="Search query")
) -> List[Dict[str, Any]]:
    """Search for stocks by symbol or name."""
    conn = get_db_connection()
    cur = conn.cursor()

    search_term = f"%{q}%"
    cur.execute("""
        SELECT
            symbol,
            name,
            exchange
        FROM md.symbols
        WHERE active = TRUE
        AND (
            symbol ILIKE %s
            OR name ILIKE %s
        )
        ORDER BY
            -- Exact matches first
            CASE WHEN symbol = UPPER(%s) THEN 0 ELSE 1 END,
            -- Then prefix matches
            CASE WHEN symbol LIKE UPPER(%s) || '%%' THEN 0 ELSE 1 END,
            symbol
        LIMIT 20
    """, (search_term, search_term, q.upper(), q.upper()))

    results = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "symbol": row[0],
            "name": row[1] or row[0],
            "exchange": row[2],
        }
        for row in results
    ]
