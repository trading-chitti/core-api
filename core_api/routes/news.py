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
