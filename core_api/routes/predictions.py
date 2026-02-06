"""Routes for ML predictions."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta, date
import psycopg2
from psycopg2.extras import RealDictCursor
import os

router = APIRouter()

PG_DSN = os.getenv('TRADING_CHITTI_PG_DSN', 'postgresql://hariprasath@localhost:6432/trading_chitti')


class PredictionResponse(BaseModel):
    symbol: str
    current_price: float
    predicted_price: float
    predicted_change_pct: float
    stop_loss: float
    target: float
    confidence: float
    trend: str
    reasoning: str
    technical_summary: str


@router.get("/top-gainers")
async def get_top_gainers(limit: int = 10) -> List[PredictionResponse]:
    """Get top predicted gainers for today."""
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Get today's top gainers from premarket.predictions
    cur.execute("""
        SELECT
            symbol,
            base_price as current_price,
            target_price as predicted_price,
            predicted_move_pct as predicted_change_pct,
            base_price * 0.98 as stop_loss,
            target_price as target,
            confidence_score as confidence,
            signal_type as trend,
            'ML-predicted based on technical indicators and fundamentals' as reasoning,
            CONCAT('RSI: ', COALESCE(rsi::text, 'N/A'), ', MACD: ', COALESCE(macd::text, 'N/A')) as technical_summary
        FROM premarket.predictions
        WHERE prediction_date = CURRENT_DATE
          AND category = 'TOP_GAINER'
        ORDER BY rank_in_category
        LIMIT %s
    """, (limit,))

    predictions = []
    for row in cur.fetchall():
        predictions.append(PredictionResponse(
            symbol=row['symbol'],
            current_price=float(row['current_price']),
            predicted_price=float(row['predicted_price']),
            predicted_change_pct=float(row['predicted_change_pct']),
            stop_loss=float(row['stop_loss']),
            target=float(row['target']),
            confidence=float(row['confidence']),
            trend=row['trend'],
            reasoning=row['reasoning'],
            technical_summary=row['technical_summary']
        ))
    
    cur.close()
    conn.close()
    
    return predictions


@router.get("/top-losers")
async def get_top_losers(limit: int = 10) -> List[PredictionResponse]:
    """Get top predicted losers for today."""
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Get today's top losers from premarket.predictions
    cur.execute("""
        SELECT
            symbol,
            base_price as current_price,
            target_price as predicted_price,
            predicted_move_pct as predicted_change_pct,
            base_price * 1.02 as stop_loss,
            target_price as target,
            confidence_score as confidence,
            signal_type as trend,
            'ML-predicted based on technical indicators and fundamentals' as reasoning,
            CONCAT('RSI: ', COALESCE(rsi::text, 'N/A'), ', MACD: ', COALESCE(macd::text, 'N/A')) as technical_summary
        FROM premarket.predictions
        WHERE prediction_date = CURRENT_DATE
          AND category = 'TOP_LOSER'
        ORDER BY rank_in_category
        LIMIT %s
    """, (limit,))

    predictions = []
    for row in cur.fetchall():
        predictions.append(PredictionResponse(
            symbol=row['symbol'],
            current_price=float(row['current_price']),
            predicted_price=float(row['predicted_price']),
            predicted_change_pct=float(row['predicted_change_pct']),
            stop_loss=float(row['stop_loss']),
            target=float(row['target']),
            confidence=float(row['confidence']),
            trend=row['trend'],
            reasoning=row['reasoning'],
            technical_summary=row['technical_summary']
        ))
    
    cur.close()
    conn.close()
    
    return predictions


@router.get("/symbol/{symbol}")
async def get_symbol_prediction(symbol: str) -> PredictionResponse:
    """Get prediction for a specific symbol."""
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            symbol, current_price, predicted_price, predicted_change_pct,
            stop_loss, target, confidence, trend, reasoning, technical_summary
        FROM predictions.daily_predictions
        WHERE symbol = %s
          AND prediction_date = CURRENT_DATE
    """, (symbol.upper(),))
    
    row = cur.fetchone()
    cur.close()
    conn.close()
    
    if not row:
        raise HTTPException(status_code=404, detail=f"No prediction found for {symbol}")
    
    return PredictionResponse(
        symbol=row[0],
        current_price=float(row[1]),
        predicted_price=float(row[2]),
        predicted_change_pct=float(row[3]),
        stop_loss=float(row[4]),
        target=float(row[5]),
        confidence=float(row[6]),
        trend=row[7],
        reasoning=row[8] or "",
        technical_summary=row[9] or ""
    )


# ============================================================================
# Pre-Market Predictions (New Schema)
# ============================================================================

class PreMarketPrediction(BaseModel):
    symbol: str
    stock_name: str
    signal_type: str
    base_price: float
    target_price: float
    predicted_move_pct: float
    confidence_score: float
    rsi: Optional[float]
    rank_in_category: int
    prediction_correct: Optional[bool] = None
    actual_move_pct: Optional[float] = None
    # Enhanced news analysis fields
    news_sentiment: Optional[float] = 0.0
    news_article_count: Optional[int] = 0
    has_breaking_news: Optional[bool] = False


class PreMarketStats(BaseModel):
    top_gainers_success_rate: Optional[float]
    top_losers_success_rate: Optional[float]
    overall_success_rate: Optional[float]
    avg_gainers_success_rate_30d: Optional[float]
    avg_losers_success_rate_30d: Optional[float]


class PreMarketResponse(BaseModel):
    gainers: List[PreMarketPrediction]
    losers: List[PreMarketPrediction]
    stats: PreMarketStats


@router.get("/premarket/today", response_model=PreMarketResponse)
async def get_premarket_predictions():
    """
    Get today's pre-market predictions with success rates.

    Returns top 20 gainers and top 20 losers with historical success rates.
    """
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # Get today's predictions
        cur.execute("""
            SELECT
                symbol, stock_name, signal_type,
                base_price, target_price, predicted_move_pct,
                confidence_score, rsi, rank_in_category, category,
                prediction_correct, actual_move_pct,
                news_sentiment, news_article_count, has_breaking_news
            FROM premarket.predictions
            WHERE prediction_date = CURRENT_DATE
            ORDER BY category, rank_in_category
        """)

        predictions = cur.fetchall()

        gainers = []
        losers = []

        for pred in predictions:
            p = PreMarketPrediction(
                symbol=pred['symbol'],
                stock_name=pred['stock_name'],
                signal_type=pred['signal_type'],
                base_price=float(pred['base_price']),
                target_price=float(pred['target_price']),
                predicted_move_pct=float(pred['predicted_move_pct']),
                confidence_score=float(pred['confidence_score']),
                rsi=float(pred['rsi']) if pred['rsi'] else None,
                rank_in_category=pred['rank_in_category'],
                prediction_correct=pred['prediction_correct'],
                actual_move_pct=float(pred['actual_move_pct']) if pred['actual_move_pct'] else None,
                news_sentiment=float(pred['news_sentiment']) if pred.get('news_sentiment') is not None else 0.0,
                news_article_count=pred.get('news_article_count', 0),
                has_breaking_news=pred.get('has_breaking_news', False)
            )

            if pred['category'] == 'TOP_GAINER':
                gainers.append(p)
            else:
                losers.append(p)

        # Get today's stats
        cur.execute("""
            SELECT
                top_gainers_success_rate,
                top_losers_success_rate,
                overall_success_rate
            FROM premarket.daily_stats
            WHERE prediction_date = CURRENT_DATE
        """)

        today_stats = cur.fetchone()

        # Get 30-day average
        cur.execute("""
            SELECT * FROM premarket.v_success_rate_summary
        """)

        hist_stats = cur.fetchone()

        stats = PreMarketStats(
            top_gainers_success_rate=float(today_stats['top_gainers_success_rate']) if (today_stats and today_stats['top_gainers_success_rate'] is not None) else None,
            top_losers_success_rate=float(today_stats['top_losers_success_rate']) if (today_stats and today_stats['top_losers_success_rate'] is not None) else None,
            overall_success_rate=float(today_stats['overall_success_rate']) if (today_stats and today_stats['overall_success_rate'] is not None) else None,
            avg_gainers_success_rate_30d=float(hist_stats['avg_gainers_success_rate']) if (hist_stats and hist_stats['avg_gainers_success_rate'] is not None) else None,
            avg_losers_success_rate_30d=float(hist_stats['avg_losers_success_rate']) if (hist_stats and hist_stats['avg_losers_success_rate'] is not None) else None
        )

        return PreMarketResponse(
            gainers=gainers,
            losers=losers,
            stats=stats
        )

    finally:
        cur.close()
        conn.close()


@router.get("/premarket/stats")
async def get_premarket_stats():
    """Get historical pre-market prediction success rates."""
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT * FROM premarket.v_success_rate_summary
        """)

        stats = cur.fetchone()

        if not stats:
            return {
                "message": "No historical data available",
                "total_days": 0
            }

        return dict(stats)

    finally:
        cur.close()
        conn.close()
