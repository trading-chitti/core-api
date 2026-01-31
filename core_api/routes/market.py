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
