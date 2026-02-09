"""Routes for news-nlp service (Mojo)."""

from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import os

import logging
import re

import psycopg2
import psycopg2.errors
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter()

PROJECT_ROOT = Path(__file__).resolve().parents[3]
NEWS_IMPACT_PATH = Path(os.getenv("NEWS_IMPACT_PATH", PROJECT_ROOT / "data" / "news_impact.json"))
NEWS_IMPACT_CACHE: deque[dict] = deque(maxlen=5000)


def _load_impact_cache() -> None:
    if NEWS_IMPACT_CACHE or not NEWS_IMPACT_PATH.exists():
        return
    try:
        with NEWS_IMPACT_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            NEWS_IMPACT_CACHE.extend(data)
    except Exception:
        # best-effort cache load
        pass


def _persist_impact_cache() -> None:
    try:
        NEWS_IMPACT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with NEWS_IMPACT_PATH.open("w", encoding="utf-8") as f:
            json.dump(list(NEWS_IMPACT_CACHE), f, indent=2)
    except Exception:
        # best-effort persistence
        pass


def _db_connect():
    dsn = os.getenv(
        "TRADING_CHITTI_PG_DSN",
        "postgresql://hariprasath@localhost:5432/trading_chitti"
    )
    return psycopg2.connect(dsn)


class AnalyzeTextRequest(BaseModel):
    """Request model for text analysis."""

    text: str = Field(..., min_length=1, max_length=10000)


class IngestRSSRequest(BaseModel):
    """Request model for RSS ingestion."""

    url: str = Field(..., min_length=1, max_length=2000)


class ImpactBatchRequest(BaseModel):
    analyses: List[Dict[str, Any]] = Field(default_factory=list)


class ArticleIngestRequest(BaseModel):
    """Request model for direct article ingestion."""

    title: str = Field(..., min_length=1, max_length=500)
    url: str = Field(..., min_length=1, max_length=2000)
    summary: Optional[str] = Field(None, max_length=1000)
    published_at: str
    source: str
    raw: Optional[Dict[str, Any]] = None


@router.post("/ingest-article")
async def ingest_article(article: ArticleIngestRequest):
    """Ingest a single news article directly into the database.

    This endpoint bypasses news-nlp processing and directly inserts
    the article into the news.articles table.
    """
    try:
        import hashlib

        # Generate article ID
        article_id = hashlib.sha1(f"{article.url}{article.title}".encode()).hexdigest()

        conn = _db_connect()
        cur = conn.cursor()

        # Insert article (handle both id and url unique constraints)
        try:
            cur.execute("""
                INSERT INTO news.articles (id, published_at, source, title, url, summary, raw)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                article_id,
                article.published_at,
                article.source,
                article.title,
                article.url,
                article.summary,
                json.dumps(article.raw) if article.raw else None
            ))
            inserted = cur.rowcount > 0
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            cur.close()
            conn.close()
            return {
                "status": "duplicate",
                "article_id": article_id,
                "inserted": False
            }

        conn.commit()
        cur.close()
        conn.close()

        # Broadcast new article via WebSocket
        if inserted:
            try:
                from core_api.websocket import manager
                await manager.broadcast_to_channel("news", {
                    "type": "new_article",
                    "article": {
                        "id": article_id,
                        "title": article.title,
                        "source": article.source,
                        "time": article.published_at,
                        "url": article.url,
                        "summary": article.summary or "",
                        "sentiment": 0.0,
                        "sentimentLabel": "neutral",
                        "impact": "low",
                        "category": "general",
                        "affectedStocks": [],
                        "priceMovement": 0.0,
                        "confidence": 0.5,
                    }
                })
            except Exception as ws_err:
                logger.warning(f"WebSocket broadcast failed: {ws_err}")

        return {
            "status": "ok" if inserted else "duplicate",
            "article_id": article_id,
            "inserted": inserted
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to ingest article: {str(e)}")


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


@router.get("/stock/{symbol}")
async def get_stock_news(
    symbol: str,
    limit: int = 200,
    offset: int = 0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """Get news articles linked to a stock symbol."""
    conn = _db_connect()
    cur = conn.cursor()

    filters = ["e.exchange = 'NSE'", "e.symbol = %s"]
    params: List[Any] = [symbol.strip().upper()]

    if start_date:
        filters.append("a.published_at >= %s")
        params.append(start_date)
    if end_date:
        filters.append("a.published_at <= %s")
        params.append(end_date)

    where_sql = " AND ".join(filters)

    cur.execute(
        f"""
        SELECT
            a.id,
            a.published_at::text as published_at,
            a.source,
            a.title,
            a.url,
            a.summary,
            a.sentiment_label,
            a.sentiment_score::float as sentiment_score
        FROM news.article_entities e
        JOIN news.articles a ON a.id = e.article_id
        WHERE {where_sql}
        ORDER BY a.published_at DESC NULLS LAST
        LIMIT %s OFFSET %s
        """,
        (*params, int(limit), int(offset)),
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "id": row[0],
            "published_at": row[1],
            "source": row[2],
            "title": row[3],
            "url": row[4],
            "summary": row[5],
            "sentiment_label": row[6],
            "sentiment_score": row[7],
        }
        for row in rows
    ]


@router.get("")
async def get_news(
    page: int = 1,
    limit: int = 20,
    offset: Optional[int] = None,
    sentiment: Optional[str] = None,
    search: Optional[str] = None,
    symbol: Optional[str] = None,
):
    """Get latest news with sentiment analysis.

    Returns paginated news articles with sentiment scores and impact assessment.
    This endpoint matches the dashboard API expectations.
    """

    # Clamp inputs
    limit = max(1, min(limit, 100))
    page = max(1, page)
    if offset is not None:
        offset = max(0, offset)

    conn = _db_connect()
    cur = conn.cursor()

    try:
        # Use explicit offset if provided, otherwise compute from page
        effective_offset = offset if offset is not None else (page - 1) * limit

        # Build dynamic WHERE clause
        where_clauses: list[str] = []
        params: list = []

        if sentiment and sentiment in ("positive", "negative", "neutral"):
            where_clauses.append("a.sentiment_label = %s")
            params.append(sentiment)

        if search:
            # Escape LIKE metacharacters
            escaped = re.sub(r'([%_\\])', r'\\\1', search)
            where_clauses.append("(a.title ILIKE %s OR a.summary ILIKE %s)")
            search_pattern = f"%{escaped}%"
            params.extend([search_pattern, search_pattern])

        if symbol:
            where_clauses.append("""
                EXISTS (
                    SELECT 1 FROM news.article_entities ae
                    WHERE ae.article_id = a.id AND ae.symbol = %s
                )
            """)
            params.append(symbol.strip().upper())

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # Get total count with filters
        cur.execute(f"SELECT COUNT(*) FROM news.articles a {where_sql}", params)
        total = cur.fetchone()[0]

        # Get articles with entities and analysis
        cur.execute(f"""
            SELECT
                a.id,
                a.title,
                a.source,
                a.published_at,
                a.sentiment_score,
                a.sentiment_label,
                a.url,
                a.summary,
                COALESCE(
                    (
                        SELECT json_agg(ae.symbol ORDER BY ae.confidence DESC)
                        FROM news.article_entities ae
                        WHERE ae.article_id = a.id
                        AND ae.confidence > 0.5
                        LIMIT 10
                    ),
                    '[]'::json
                ) as affected_stocks,
                a.raw
            FROM news.articles a
            {where_sql}
            ORDER BY a.published_at DESC
            LIMIT %s OFFSET %s
        """, (*params, limit, effective_offset))

        results = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    articles = []
    for row in results:
        # Extract analysis data from raw JSON if available
        raw_data = row[9] if row[9] else {}
        analysis = raw_data.get('analysis', {})

        # Use analyzed sentiment if available, otherwise fall back to old data
        if analysis:
            sentiment_label = analysis.get('sentiment', 'neutral')
            sentiment_confidence = float(analysis.get('sentiment_confidence', 0.5))
            impact_score = float(analysis.get('impact_score', 0.0))
            affected_sectors = analysis.get('affected_sectors', [])
            analysis_summary = analysis.get('analysis_summary', '')

            # Convert sentiment label to numeric for compatibility
            sentiment_map = {'positive': 0.7, 'negative': -0.7, 'neutral': 0.0}
            sentiment_numeric = sentiment_map.get(sentiment_label, 0.0)
        else:
            # Fallback to old sentiment data
            sentiment_numeric = float(row[4]) if row[4] else 0.0
            sentiment_label = row[5] or "neutral"
            sentiment_confidence = abs(sentiment_numeric)
            impact_score = abs(sentiment_numeric)
            affected_sectors = []
            analysis_summary = ""

        # Determine impact category
        if impact_score > 0.7:
            impact = "high"
        elif impact_score > 0.4:
            impact = "medium"
        else:
            impact = "low"

        # Calculate price movement expectation
        price_movement = sentiment_numeric * 2.5  # Scale sentiment to approximate % movement

        articles.append({
            "id": row[0],
            "title": row[1],
            "source": row[2],
            "time": row[3].isoformat() if row[3] else datetime.utcnow().isoformat(),
            "summary": row[7] or "",
            "sentiment": sentiment_numeric,
            "sentimentLabel": sentiment_label,
            "impact": impact,
            "impactScore": round(impact_score, 2),
            "category": row[5] or "general",
            "affectedStocks": row[8] if row[8] else [],
            "affectedSectors": affected_sectors,
            "priceMovement": round(price_movement, 2),
            "confidence": round(sentiment_confidence, 2),
            "analysisSummary": analysis_summary,
            "url": row[6],
        })

    return {
        "articles": articles,
        "total": total,
        "page": page,
        "hasMore": (effective_offset + limit) < total
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


@router.post("/impact")
async def store_news_impact(batch: ImpactBatchRequest):
    """Store news impact analyses from background workers."""
    _load_impact_cache()
    now = datetime.utcnow().isoformat() + "Z"
    for item in batch.analyses:
        if "ingested_at" not in item:
            item["ingested_at"] = now
        NEWS_IMPACT_CACHE.append(item)
    _persist_impact_cache()
    return {"status": "ok", "stored": len(batch.analyses)}


@router.get("/impact")
async def list_news_impact(limit: int = 100):
    """Return recent news impact analyses."""
    _load_impact_cache()
    limit = max(1, min(int(limit), 1000))
    items = list(NEWS_IMPACT_CACHE)[-limit:]
    return {"items": items, "count": len(items)}
