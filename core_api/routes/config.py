"""Configuration management routes."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
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
