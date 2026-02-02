"""Routes for portfolio statistics and performance."""

from fastapi import APIRouter, HTTPException
from typing import Dict, Any
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


@router.get("/stats")
async def get_portfolio_stats() -> Dict[str, Any]:
    """Get portfolio statistics including value, P&L, and win rate.

    Returns:
        - value: Current portfolio value
        - dailyPnL: Today's profit/loss
        - dailyPnLPercent: Today's P&L as percentage
        - winRate: Percentage of winning trades
        - totalTrades: Total number of trades
        - winningTrades: Number of winning trades
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if trades.executions table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'trades'
                AND table_name = 'executions'
            )
        """)
        table_exists = cur.fetchone()[0]

        if not table_exists:
            # Database schema not initialized - return zero stats
            cur.close()
            conn.close()
            return {
                "value": 0,
                "dailyPnL": 0,
                "dailyPnLPercent": 0,
                "winRate": 0,
                "totalTrades": 0,
                "winningTrades": 0
            }

        # Get overall portfolio stats
        cur.execute("""
        WITH trade_stats AS (
            SELECT
                COUNT(*) as total_trades,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                COALESCE(SUM(pnl), 0) as total_pnl
            FROM trades.executions
            WHERE status = 'CLOSED'
            AND dry_run = FALSE
        ),
        today_stats AS (
            SELECT
                COALESCE(SUM(pnl), 0) as today_pnl
            FROM trades.executions
            WHERE status = 'CLOSED'
            AND dry_run = FALSE
            AND exit_date >= CURRENT_DATE
        ),
        portfolio_value AS (
            -- Calculate current portfolio value from open positions
            SELECT
                COALESCE(SUM(
                    CASE
                        WHEN e.action = 'BUY' THEN e.quantity * COALESCE(c.last_price, e.entry_price)
                        ELSE 0
                    END
                ), 0) as current_value
            FROM trades.executions e
            LEFT JOIN trades.market_data_cache c
                ON e.symbol = c.symbol AND e.exchange = c.exchange
            WHERE e.status = 'OPEN'
            AND e.dry_run = FALSE
        ),
        starting_capital AS (
            SELECT COALESCE(
                (SELECT starting_capital
                 FROM trades.performance
                 ORDER BY date DESC
                 LIMIT 1),
                100000.0  -- Default starting capital
            ) as capital
        )
        SELECT
            pv.current_value as portfolio_value,
            ts.today_pnl,
            CASE
                WHEN sc.capital > 0
                THEN (ts.today_pnl / sc.capital * 100)
                ELSE 0
            END as today_pnl_pct,
            CASE
                WHEN ts.total_trades > 0
                THEN (ts.winning_trades::NUMERIC / ts.total_trades * 100)
                ELSE 0
            END as win_rate,
            ts.total_trades,
            ts.winning_trades
        FROM trade_stats ts
        CROSS JOIN today_stats tod
        CROSS JOIN portfolio_value pv
        CROSS JOIN starting_capital sc
    """)

        result = cur.fetchone()
        cur.close()
        conn.close()

        if not result:
            # Return default values if no data
            return {
                "value": 0.0,
                "dailyPnL": 0.0,
                "dailyPnLPercent": 0.0,
                "winRate": 0.0,
                "totalTrades": 0,
                "winningTrades": 0,
            }

        return {
            "value": round(float(result[0]) if result[0] else 0.0, 2),
            "dailyPnL": round(float(result[1]) if result[1] else 0.0, 2),
            "dailyPnLPercent": round(float(result[2]) if result[2] else 0.0, 2),
            "winRate": round(float(result[3]) if result[3] else 0.0, 2),
            "totalTrades": int(result[4]) if result[4] else 0,
            "winningTrades": int(result[5]) if result[5] else 0,
        }
    except Exception as e:
        # Return zero stats on error - no mock data
        return {
            "value": 0,
            "dailyPnL": 0,
            "dailyPnLPercent": 0,
            "winRate": 0,
            "totalTrades": 0,
            "winningTrades": 0
        }


@router.get("/positions")
async def get_positions() -> list[Dict[str, Any]]:
    """Get current open positions."""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            e.symbol,
            e.exchange,
            e.action,
            e.quantity,
            e.entry_price,
            e.stop_loss,
            e.take_profit,
            e.executed_at,
            e.confidence,
            e.strategy,
            COALESCE(c.last_price, e.entry_price) as current_price,
            -- Calculate unrealized P&L
            CASE
                WHEN e.action = 'BUY' THEN
                    (COALESCE(c.last_price, e.entry_price) - e.entry_price) * e.quantity
                WHEN e.action = 'SELL' THEN
                    (e.entry_price - COALESCE(c.last_price, e.entry_price)) * e.quantity
            END as unrealized_pnl
        FROM trades.executions e
        LEFT JOIN trades.market_data_cache c
            ON e.symbol = c.symbol AND e.exchange = c.exchange
        WHERE e.status = 'OPEN'
        AND e.dry_run = FALSE
        ORDER BY e.executed_at DESC
    """)

    results = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "symbol": row[0],
            "exchange": row[1],
            "action": row[2],
            "quantity": int(row[3]),
            "entryPrice": round(float(row[4]), 2),
            "stopLoss": round(float(row[5]), 2) if row[5] else None,
            "takeProfit": round(float(row[6]), 2) if row[6] else None,
            "executedAt": row[7].isoformat(),
            "confidence": round(float(row[8]), 2) if row[8] else None,
            "strategy": row[9],
            "currentPrice": round(float(row[10]), 2),
            "unrealizedPnL": round(float(row[11]), 2) if row[11] else 0.0,
        }
        for row in results
    ]


@router.get("/history")
async def get_trade_history(limit: int = 50) -> list[Dict[str, Any]]:
    """Get closed trade history."""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            symbol,
            exchange,
            action,
            quantity,
            entry_price,
            exit_price,
            executed_at,
            exit_date,
            pnl,
            pnl_pct,
            strategy,
            confidence
        FROM trades.executions
        WHERE status = 'CLOSED'
        AND dry_run = FALSE
        ORDER BY exit_date DESC
        LIMIT %s
    """, (limit,))

    results = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "symbol": row[0],
            "exchange": row[1],
            "action": row[2],
            "quantity": int(row[3]),
            "entryPrice": round(float(row[4]), 2),
            "exitPrice": round(float(row[5]), 2) if row[5] else None,
            "executedAt": row[6].isoformat(),
            "exitDate": row[7].isoformat() if row[7] else None,
            "pnl": round(float(row[8]), 2) if row[8] else 0.0,
            "pnlPercent": round(float(row[9]), 2) if row[9] else 0.0,
            "strategy": row[10],
            "confidence": round(float(row[11]), 2) if row[11] else None,
        }
        for row in results
    ]
