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
        "postgresql://hariprasath@localhost:6432/trading_chitti"
    )
    return psycopg2.connect(dsn)


@router.get("/top-gainers")
async def get_top_gainers(limit: int = Query(20, le=100)) -> List[Dict[str, Any]]:
    """Get top gainers based on real-time price changes from Zerodha Kite WebSocket.

    Returns stocks with highest positive price changes today.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get real-time gainers from WebSocket data
        # Calculate change_percent from close vs last_price
        cur.execute("""
            SELECT
                i.tradingsymbol as symbol,
                i.name,
                r.last_price::float as price,
                CASE
                    WHEN r.close > 0 THEN
                        ((r.last_price - r.close) / r.close * 100)::float
                    ELSE 0
                END as change,
                CASE
                    WHEN r.close > 0 THEN
                        LEAST(ABS((r.last_price - r.close) / r.close * 100) / 10.0, 1.0)
                    ELSE 0.5
                END as confidence
            FROM md.realtime_prices r
            JOIN md.instrument_tokens i ON i.instrument_token = r.instrument_token
            WHERE i.exchange = 'NSE'
              AND i.instrument_type = 'EQ'
              AND r.updated_at > NOW() - INTERVAL '10 minutes'
              AND r.close > 0
              AND r.last_price > r.close
            ORDER BY change DESC
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
                "change": round(float(row[3]) if row[3] else 0.0, 2),
                "confidence": round(float(row[4]) if row[4] else 0.0, 2),
                "price": round(float(row[2]) if row[2] else 0.0, 2),
            }
            for row in results
        ]
    except Exception as e:
        return []


@router.get("/top-losers")
async def get_top_losers(limit: int = Query(20, le=100)) -> List[Dict[str, Any]]:
    """Get top losers based on real-time price changes from Zerodha Kite WebSocket.

    Returns stocks with highest negative price changes today.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get real-time losers from WebSocket data
        # Calculate change_percent from close vs last_price
        cur.execute("""
            SELECT
                i.tradingsymbol as symbol,
                i.name,
                r.last_price::float as price,
                CASE
                    WHEN r.close > 0 THEN
                        ((r.last_price - r.close) / r.close * 100)::float
                    ELSE 0
                END as change,
                CASE
                    WHEN r.close > 0 THEN
                        LEAST(ABS((r.last_price - r.close) / r.close * 100) / 10.0, 1.0)
                    ELSE 0.5
                END as confidence
            FROM md.realtime_prices r
            JOIN md.instrument_tokens i ON i.instrument_token = r.instrument_token
            WHERE i.exchange = 'NSE'
              AND i.instrument_type = 'EQ'
              AND r.updated_at > NOW() - INTERVAL '10 minutes'
              AND r.close > 0
              AND r.last_price < r.close
            ORDER BY change ASC
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
                "change": round(float(row[3]) if row[3] else 0.0, 2),
                "confidence": round(float(row[4]) if row[4] else 0.0, 2),
                "price": round(float(row[2]) if row[2] else 0.0, 2),
            }
            for row in results
        ]
    except Exception as e:
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
    days: int = Query(365, le=730, description="Number of days of history (max 730)"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get historical stock data for the specified number of days.

    Returns OHLCV data for charting and analysis.
    If start_date/end_date are provided, they take precedence over days.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    if start_date or end_date:
        filters = ["symbol = %s", "exchange = 'NSE'"]
        params: List[Any] = [symbol]

        if start_date:
            filters.append("trade_date >= %s")
            params.append(start_date)
        if end_date:
            filters.append("trade_date <= %s")
            params.append(end_date)

        where_sql = " AND ".join(filters)
        cur.execute(
            f"""
            SELECT
                trade_date,
                open,
                high,
                low,
                close,
                volume
            FROM md.eod_prices
            WHERE {where_sql}
            ORDER BY trade_date ASC
            """,
            tuple(params),
        )
    else:
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


@router.get("/realtime/all")
async def get_realtime_prices(limit: int = Query(50, le=200)) -> List[Dict[str, Any]]:
    """Get real-time prices for all tracked symbols from Zerodha Kite WebSocket."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                i.tradingsymbol as symbol,
                r.last_price::float as last_price,
                r.volume::bigint as volume,
                r.open::float as open,
                r.high::float as high,
                r.low::float as low,
                r.close::float as close,
                r.change_percent::float as change_percent,
                r.updated_at::text as updated_at
            FROM md.realtime_prices r
            JOIN md.instrument_tokens i ON i.instrument_token = r.instrument_token
            WHERE i.exchange = 'NSE'
              AND i.instrument_type = 'EQ'
              AND r.updated_at > NOW() - INTERVAL '5 minutes'
            ORDER BY r.updated_at DESC
            LIMIT %s
        """, (limit,))

        results = cur.fetchall()
        cur.close()
        conn.close()

        return [
            {
                "symbol": row[0],
                "last_price": float(row[1]) if row[1] else 0.0,
                "volume": int(row[2]) if row[2] else None,
                "open": float(row[3]) if row[3] else 0.0,
                "high": float(row[4]) if row[4] else 0.0,
                "low": float(row[5]) if row[5] else 0.0,
                "close": float(row[6]) if row[6] else 0.0,
                "change_percent": float(row[7]) if row[7] else None,
                "updated_at": row[8],
            }
            for row in results
        ]
    except Exception as e:
        # Return empty list on error
        return []


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
