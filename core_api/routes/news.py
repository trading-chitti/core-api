"""Routes for news-nlp service (Mojo)."""

from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

router = APIRouter()


class AnalyzeTextRequest(BaseModel):
    """Request model for text analysis."""

    text: str = Field(..., min_length=1, max_length=10000)


class IngestRSSRequest(BaseModel):
    """Request model for RSS ingestion."""

    url: str = Field(..., min_length=1, max_length=2000)


@router.get("/articles")
async def get_articles(
    request: Request,
    limit: int = 100,
    offset: int = 0,
    symbol: Optional[str] = None,
    sector: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """Get articles from news-nlp database.

    Args:
        limit: Maximum number of articles
        offset: Offset for pagination
        symbol: Filter by symbol (optional)
        sector: Filter by sector (optional)
        start_date: Filter by start date ISO format (optional)
        end_date: Filter by end date ISO format (optional)
    """
    try:
        client = request.app.state.news_nlp
        response = await client.get_articles(
            limit=limit,
            offset=offset,
            symbol=symbol,
            sector=sector,
            start_date=start_date,
            end_date=end_date,
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"News NLP error: {str(e)}")


@router.get("")
async def get_news(
    page: int = 1,
    limit: int = 20
):
    """Get latest news with sentiment analysis.

    Returns paginated news articles with sentiment scores and impact assessment.
    This endpoint matches the dashboard API expectations.
    """
    import psycopg2
    import os
    from datetime import datetime

    dsn = os.getenv(
        "TRADING_CHITTI_PG_DSN",
        "postgresql://hariprasath@localhost:5432/trading_chitti"
    )
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    offset = (page - 1) * limit

    # Get total count
    cur.execute("SELECT COUNT(*) FROM news.articles")
    total = cur.fetchone()[0]

    # Get articles with entities
    cur.execute("""
        SELECT
            a.id,
            a.title,
            a.source,
            a.published_at,
            a.sentiment_score,
            a.sentiment_label,
            a.url,
            COALESCE(
                (
                    SELECT json_agg(ae.symbol)
                    FROM news.article_entities ae
                    WHERE ae.article_id = a.id
                    AND ae.confidence > 0.5
                    LIMIT 5
                ),
                '[]'::json
            ) as affected_stocks
        FROM news.articles a
        ORDER BY a.published_at DESC
        LIMIT %s OFFSET %s
    """, (limit, offset))

    results = cur.fetchall()
    cur.close()
    conn.close()

    articles = []
    for row in results:
        # Convert sentiment score to -1 to 1 range
        sentiment = float(row[4]) if row[4] else 0.0

        # Determine impact based on sentiment magnitude
        sentiment_magnitude = abs(sentiment)
        if sentiment_magnitude > 0.7:
            impact = "high"
        elif sentiment_magnitude > 0.4:
            impact = "medium"
        else:
            impact = "low"

        # Calculate approximate price movement expectation
        price_movement = sentiment * 2.0  # Scale sentiment to approximate % movement

        articles.append({
            "id": row[0],
            "title": row[1],
            "source": row[2],
            "time": row[3].isoformat() if row[3] else datetime.utcnow().isoformat(),
            "sentiment": sentiment,
            "impact": impact,
            "category": row[5] or "general",
            "affectedStocks": row[7] if row[7] else [],
            "priceMovement": round(price_movement, 2),
            "confidence": min(sentiment_magnitude, 1.0),
        })

    return {
        "articles": articles,
        "total": total,
        "page": page,
        "hasMore": (offset + limit) < total
    }


@router.post("/analyze/sentiment")
async def analyze_sentiment(request_model: AnalyzeTextRequest, request: Request):
    """Analyze sentiment of text using Mojo NLP."""
    try:
        client = request.app.state.news_nlp
        response = await client.analyze_sentiment(text=request_model.text)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"News NLP error: {str(e)}")


@router.post("/analyze/entities")
async def extract_entities(request_model: AnalyzeTextRequest, request: Request):
    """Extract named entities (symbols, sectors) from text."""
    try:
        client = request.app.state.news_nlp
        response = await client.extract_entities(text=request_model.text)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"News NLP error: {str(e)}")


@router.post("/analyze/direction")
async def classify_direction(request_model: AnalyzeTextRequest, request: Request):
    """Classify market direction from text."""
    try:
        client = request.app.state.news_nlp
        response = await client.classify_direction(text=request_model.text)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"News NLP error: {str(e)}")


@router.post("/ingest")
async def ingest_rss(request_model: IngestRSSRequest, request: Request):
    """Trigger RSS feed ingestion (admin only).

    Note: This should be protected with authentication in production.
    """
    try:
        client = request.app.state.news_nlp
        response = await client.ingest_rss(url=request_model.url)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"News NLP error: {str(e)}")
