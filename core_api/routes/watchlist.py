"""Routes for user watchlist management."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
import psycopg2
import os

router = APIRouter()


def get_db_connection():
    """Get PostgreSQL connection."""
    dsn = os.getenv(
        "TRADING_CHITTI_PG_DSN",
        "postgresql://hariprasath@localhost:5432/trading_chitti"
    )
    return psycopg2.connect(dsn)


class AddToWatchlistRequest(BaseModel):
    """Request model for adding symbol to watchlist."""
    symbol: str


@router.get("")
async def get_watchlist(user_id: str = "default") -> List[Dict[str, Any]]:
    """Get user's watchlist with current prices and changes."""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            w.symbol,
            s.name,
            e.close as price,
            COALESCE(e.close - prev.close, 0) as change,
            CASE
                WHEN prev.close > 0
                THEN ((e.close - prev.close) / prev.close * 100)
                ELSE 0
            END as change_percent
        FROM md.watchlist w
        INNER JOIN md.symbols s
            ON w.symbol = s.symbol AND w.exchange = s.exchange
        LEFT JOIN LATERAL (
            SELECT close, trade_date
            FROM md.eod_prices ep
            WHERE ep.symbol = w.symbol
            AND ep.exchange = w.exchange
            ORDER BY trade_date DESC
            LIMIT 1
        ) e ON true
        LEFT JOIN LATERAL (
            SELECT close
            FROM md.eod_prices ep
            WHERE ep.symbol = w.symbol
            AND ep.exchange = w.exchange
            AND ep.trade_date < e.trade_date
            ORDER BY trade_date DESC
            LIMIT 1
        ) prev ON true
        WHERE w.user_id = %s
        ORDER BY w.added_at DESC
    """, (user_id,))

    results = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "symbol": row[0],
            "name": row[1] or row[0],
            "price": round(float(row[2]) if row[2] else 0.0, 2),
            "change": round(float(row[3]) if row[3] else 0.0, 2),
            "changePercent": round(float(row[4]) if row[4] else 0.0, 2),
        }
        for row in results
    ]


@router.post("")
async def add_to_watchlist(
    request: AddToWatchlistRequest,
    user_id: str = "default"
) -> Dict[str, Any]:
    """Add a symbol to user's watchlist."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Check if symbol exists
        cur.execute("""
            SELECT exchange, symbol
            FROM md.symbols
            WHERE symbol = %s
            AND active = TRUE
            LIMIT 1
        """, (request.symbol,))

        result = cur.fetchone()
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Symbol {request.symbol} not found"
            )

        exchange, symbol = result

        # Add to watchlist
        cur.execute("""
            INSERT INTO md.watchlist (user_id, exchange, symbol)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, exchange, symbol) DO NOTHING
            RETURNING id
        """, (user_id, exchange, symbol))

        new_id = cur.fetchone()
        conn.commit()

        if new_id:
            return {
                "status": "success",
                "message": f"Added {symbol} to watchlist",
                "symbol": symbol,
            }
        else:
            return {
                "status": "exists",
                "message": f"{symbol} already in watchlist",
                "symbol": symbol,
            }

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add to watchlist: {str(e)}"
        )
    finally:
        cur.close()
        conn.close()


@router.delete("/{symbol}")
async def remove_from_watchlist(
    symbol: str,
    user_id: str = "default"
) -> Dict[str, Any]:
    """Remove a symbol from user's watchlist."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            DELETE FROM md.watchlist
            WHERE user_id = %s
            AND symbol = %s
            RETURNING id
        """, (user_id, symbol))

        deleted_id = cur.fetchone()
        conn.commit()

        if deleted_id:
            return {
                "status": "success",
                "message": f"Removed {symbol} from watchlist",
                "symbol": symbol,
            }
        else:
            raise HTTPException(
                status_code=404,
                detail=f"{symbol} not found in watchlist"
            )

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to remove from watchlist: {str(e)}"
        )
    finally:
        cur.close()
        conn.close()
