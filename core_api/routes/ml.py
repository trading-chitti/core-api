"""Routes for ML analysis and predictions."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
import psycopg2
from datetime import datetime
import os

router = APIRouter()


def get_db_connection():
    """Get PostgreSQL connection."""
    dsn = os.getenv(
        "TRADING_CHITTI_PG_DSN",
        "postgresql://hariprasath@localhost:5432/trading_chitti"
    )
    return psycopg2.connect(dsn)


class MLAnalysisRequest(BaseModel):
    """Request model for storing ML analysis results."""
    symbol: str = Field(..., description="Stock symbol")
    period_days: int = Field(52, description="Analysis period in days")
    trend_direction: Optional[str] = Field(None, description="Trend direction (up/down/neutral)")
    trend_slope: Optional[float] = Field(None, description="Trend slope coefficient")
    trend_r_squared: Optional[float] = Field(None, description="R-squared of trend fit")
    volatility: Optional[float] = Field(None, description="Historical volatility")
    atr: Optional[float] = Field(None, description="Average True Range")
    rsi: Optional[float] = Field(None, description="Relative Strength Index")
    macd: Optional[float] = Field(None, description="MACD value")
    sma_20: Optional[float] = Field(None, description="20-day Simple Moving Average")
    sma_50: Optional[float] = Field(None, description="50-day Simple Moving Average")
    confidence: Optional[float] = Field(None, description="ML model confidence (0-1)")
    signals_count: int = Field(0, description="Number of signals generated")
    analysis_json: Optional[Dict[str, Any]] = Field(None, description="Full analysis JSON")


class SignalRequest(BaseModel):
    """Request model for creating trading signals."""
    symbol: str = Field(..., description="Stock symbol")
    signal_type: str = Field(..., description="BUY or SELL")
    strategy: str = Field(..., description="Strategy name")
    confidence: float = Field(..., ge=0, le=1, description="Signal confidence (0-1)")
    entry_price: float = Field(..., gt=0, description="Entry price")
    stop_loss: Optional[float] = Field(None, gt=0, description="Stop loss price")
    take_profit: Optional[float] = Field(None, gt=0, description="Take profit price")
    reason: Optional[str] = Field(None, description="Signal reason/rationale")
    expires_at: Optional[datetime] = Field(None, description="Signal expiration time")


@router.post("/analysis")
async def store_ml_analysis(analysis: MLAnalysisRequest) -> Dict[str, Any]:
    """Store ML analysis results in the database.

    This endpoint is used by ML services to persist their analysis results.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO trades.analysis (
                symbol,
                period_days,
                trend_direction,
                trend_slope,
                trend_r_squared,
                volatility,
                atr,
                rsi,
                macd,
                sma_20,
                sma_50,
                confidence,
                signals_count,
                analysis_json
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING analysis_id, analysis_date
        """, (
            analysis.symbol,
            analysis.period_days,
            analysis.trend_direction,
            analysis.trend_slope,
            analysis.trend_r_squared,
            analysis.volatility,
            analysis.atr,
            analysis.rsi,
            analysis.macd,
            analysis.sma_20,
            analysis.sma_50,
            analysis.confidence,
            analysis.signals_count,
            analysis.analysis_json,
        ))

        result = cur.fetchone()
        conn.commit()

        return {
            "analysis_id": result[0],
            "analysis_date": result[1].isoformat(),
            "symbol": analysis.symbol,
            "status": "success",
            "message": "Analysis stored successfully"
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to store analysis: {str(e)}")
    finally:
        cur.close()
        conn.close()


@router.post("/signals")
async def create_signal(signal: SignalRequest) -> Dict[str, Any]:
    """Create a trading signal.

    This endpoint is used by ML/signal generation services to create trading signals.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Get the latest analysis_id for this symbol
        cur.execute("""
            SELECT analysis_id
            FROM trades.analysis
            WHERE symbol = %s
            ORDER BY analysis_date DESC
            LIMIT 1
        """, (signal.symbol,))

        analysis_result = cur.fetchone()
        analysis_id = analysis_result[0] if analysis_result else None

        # Insert the signal
        cur.execute("""
            INSERT INTO trades.signals (
                analysis_id,
                symbol,
                signal_type,
                strategy,
                confidence,
                entry_price,
                stop_loss,
                take_profit,
                reason,
                expires_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING signal_id, generated_at
        """, (
            analysis_id,
            signal.symbol,
            signal.signal_type,
            signal.strategy,
            signal.confidence,
            signal.entry_price,
            signal.stop_loss,
            signal.take_profit,
            signal.reason,
            signal.expires_at,
        ))

        result = cur.fetchone()
        conn.commit()

        return {
            "signal_id": result[0],
            "generated_at": result[1].isoformat(),
            "symbol": signal.symbol,
            "signal_type": signal.signal_type,
            "status": "success",
            "message": "Signal created successfully"
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create signal: {str(e)}")
    finally:
        cur.close()
        conn.close()


@router.get("/analysis/{symbol}")
async def get_ml_analysis(symbol: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Get ML analysis history for a symbol."""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            analysis_id,
            analysis_date,
            period_days,
            trend_direction,
            trend_slope,
            trend_r_squared,
            volatility,
            atr,
            rsi,
            macd,
            sma_20,
            sma_50,
            confidence,
            signals_count,
            analysis_json
        FROM trades.analysis
        WHERE symbol = %s
        ORDER BY analysis_date DESC
        LIMIT %s
    """, (symbol, limit))

    results = cur.fetchall()
    cur.close()
    conn.close()

    if not results:
        return []

    return [
        {
            "analysisId": row[0],
            "analysisDate": row[1].isoformat(),
            "periodDays": row[2],
            "trendDirection": row[3],
            "trendSlope": float(row[4]) if row[4] else None,
            "trendRSquared": float(row[5]) if row[5] else None,
            "volatility": float(row[6]) if row[6] else None,
            "atr": float(row[7]) if row[7] else None,
            "rsi": float(row[8]) if row[8] else None,
            "macd": float(row[9]) if row[9] else None,
            "sma20": float(row[10]) if row[10] else None,
            "sma50": float(row[11]) if row[11] else None,
            "confidence": float(row[12]) if row[12] else None,
            "signalsCount": row[13],
            "analysisJson": row[14],
        }
        for row in results
    ]


@router.get("/predictions")
async def get_predictions(
    limit: int = 50,
    min_confidence: float = 0.6
) -> List[Dict[str, Any]]:
    """Get latest ML predictions across all stocks.

    Returns the most recent predictions with high confidence scores.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        WITH latest_predictions AS (
            SELECT DISTINCT ON (a.symbol)
                a.symbol,
                s.name,
                a.trend_direction,
                a.trend_slope,
                a.confidence,
                a.rsi,
                a.macd,
                a.analysis_date,
                e.close as current_price
            FROM trades.analysis a
            INNER JOIN md.symbols s ON a.symbol = s.symbol
            LEFT JOIN LATERAL (
                SELECT close
                FROM md.eod_prices ep
                WHERE ep.symbol = a.symbol
                AND ep.exchange = 'NSE'
                ORDER BY trade_date DESC
                LIMIT 1
            ) e ON true
            WHERE a.confidence >= %s
            AND s.active = TRUE
            ORDER BY a.symbol, a.analysis_date DESC
        )
        SELECT
            symbol,
            name,
            trend_direction,
            trend_slope,
            confidence,
            rsi,
            macd,
            analysis_date,
            current_price
        FROM latest_predictions
        ORDER BY confidence DESC, analysis_date DESC
        LIMIT %s
    """, (min_confidence, limit))

    results = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "symbol": row[0],
            "name": row[1] or row[0],
            "trendDirection": row[2],
            "trendSlope": float(row[3]) if row[3] else None,
            "confidence": float(row[4]) if row[4] else None,
            "rsi": float(row[5]) if row[5] else None,
            "macd": float(row[6]) if row[6] else None,
            "analysisDate": row[7].isoformat(),
            "currentPrice": float(row[8]) if row[8] else None,
        }
        for row in results
    ]
