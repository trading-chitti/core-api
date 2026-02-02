"""Market data routes."""

from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List
import psycopg2
import os
from datetime import datetime, timedelta

router = APIRouter()

def get_db_connection():
    """Get PostgreSQL connection."""
    dsn = os.getenv(
        "TRADING_CHITTI_PG_DSN",
        "postgresql://hariprasath@localhost:5432/trading_chitti"
    )
    return psycopg2.connect(dsn)


@router.get("/price-history/{symbol}")
async def get_price_history(symbol: str, days: int = 90) -> List[Dict[str, Any]]:
    """Get price history for a symbol."""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT trade_date, open, high, low, close, volume
        FROM md.eod_prices
        WHERE symbol = %s
          AND trade_date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY trade_date DESC
    """, (symbol, days))

    results = cur.fetchall()
    cur.close()
    conn.close()

    if not results:
        raise HTTPException(status_code=404, detail=f"No data found for symbol {symbol}")

    return [
        {
            "date": row[0].isoformat(),
            "open": float(row[1]) if row[1] else None,
            "high": float(row[2]) if row[2] else None,
            "low": float(row[3]) if row[3] else None,
            "close": float(row[4]) if row[4] else None,
            "volume": int(row[5]) if row[5] else None,
        }
        for row in results
    ]


@router.get("/intraday/{symbol}")
async def get_intraday_data(
    symbol: str,
    timeframe: str = "1min",
    limit: int = 1000
) -> List[Dict[str, Any]]:
    """Get intraday bar data for a symbol."""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT bar_timestamp, open, high, low, close, volume
        FROM md.intraday_bars
        WHERE symbol = %s
          AND timeframe = %s
        ORDER BY bar_timestamp DESC
        LIMIT %s
    """, (symbol, timeframe, limit))

    results = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "timestamp": row[0].isoformat(),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": int(row[5]) if row[5] else None,
        }
        for row in results
    ]


@router.get("/indices")
async def get_market_indices() -> List[Dict[str, Any]]:
    """Get major market indices (NIFTY 50, SENSEX, etc.).

    Returns current values and changes for major Indian market indices.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # List of major Indian indices
    indices = [
        ("NIFTY 50", "^NSEI"),
        ("NIFTY BANK", "^NSEBANK"),
        ("SENSEX", "^BSESN"),
        ("NIFTY IT", "^CNXIT"),
        ("NIFTY MIDCAP", "^NSEMDCP50"),
    ]

    results = []

    for index_name, index_symbol in indices:
        # Get latest and previous day's close for each index
        cur.execute("""
            WITH latest_data AS (
                SELECT
                    close,
                    trade_date,
                    LAG(close) OVER (ORDER BY trade_date) as prev_close
                FROM md.eod_prices
                WHERE symbol = %s
                AND exchange = 'NSE'
                ORDER BY trade_date DESC
                LIMIT 2
            )
            SELECT
                close as value,
                COALESCE(close - prev_close, 0) as change,
                CASE
                    WHEN prev_close > 0 THEN ((close - prev_close) / prev_close * 100)
                    ELSE 0
                END as change_percent
            FROM latest_data
            ORDER BY trade_date DESC
            LIMIT 1
        """, (index_symbol,))

        result = cur.fetchone()

        if result:
            results.append({
                "index": index_name,
                "value": round(float(result[0]) if result[0] else 0.0, 2),
                "change": round(float(result[1]) if result[1] else 0.0, 2),
                "changePercent": round(float(result[2]) if result[2] else 0.0, 2),
            })
        else:
            # If no data, return default values
            results.append({
                "index": index_name,
                "value": 0.0,
                "change": 0.0,
                "changePercent": 0.0,
            })

    cur.close()
    conn.close()

    # Return empty list if no data (no mock data)
    return [r for r in results if r["value"] > 0]
