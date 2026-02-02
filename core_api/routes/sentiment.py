"""
Sentiment analysis API endpoints.
Provides on-demand BERT sentiment analysis for text.
"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
import sys
import os

# Add scripts directory to path to import sentiment_analyzer
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..', 'scripts'))

try:
    from sentiment_analyzer import FinancialSentimentAnalyzer
    BERT_AVAILABLE = True
except ImportError:
    BERT_AVAILABLE = False

router = APIRouter(prefix="/sentiment", tags=["Sentiment"])

# Singleton analyzer instance (loaded once)
_analyzer = None


def get_analyzer():
    """Get or create analyzer instance."""
    global _analyzer
    if not BERT_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="FinBERT not available. Install: pip3 install transformers torch"
        )

    if _analyzer is None:
        _analyzer = FinancialSentimentAnalyzer()

    return _analyzer


class TextAnalysisRequest(BaseModel):
    """Request model for text sentiment analysis."""
    text: str
    return_all_scores: bool = False


class TextAnalysisResponse(BaseModel):
    """Response model for sentiment analysis."""
    label: str
    score: float
    all_scores: Optional[dict] = None


class BatchAnalysisRequest(BaseModel):
    """Request model for batch sentiment analysis."""
    texts: List[str]
    batch_size: int = 8


@router.post("/analyze", response_model=TextAnalysisResponse)
async def analyze_text(request: TextAnalysisRequest):
    """
    Analyze sentiment of a single text using FinBERT.

    Returns:
        - label: positive, negative, or neutral
        - score: confidence score (0-1)
        - all_scores: all label scores (if requested)
    """
    try:
        analyzer = get_analyzer()

        result = analyzer.analyze(
            request.text,
            return_all_scores=request.return_all_scores
        )

        if 'error' in result:
            raise HTTPException(status_code=500, detail=result['error'])

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze/batch")
async def analyze_batch(request: BatchAnalysisRequest):
    """
    Analyze sentiment of multiple texts efficiently.

    Returns list of sentiment results in same order as input.
    """
    try:
        analyzer = get_analyzer()

        results = analyzer.analyze_batch(
            request.texts,
            batch_size=request.batch_size
        )

        return JSONResponse({
            "results": results,
            "count": len(results)
        })

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def sentiment_health():
    """Check if sentiment analysis is available."""
    if not BERT_AVAILABLE:
        return JSONResponse({
            "status": "unavailable",
            "message": "FinBERT not installed"
        })

    try:
        analyzer = get_analyzer()
        # Test with simple text
        result = analyzer.analyze("Stock market rally")
        return JSONResponse({
            "status": "healthy",
            "model": "ProsusAI/finbert",
            "test_result": result
        })
    except Exception as e:
        return JSONResponse({
            "status": "unhealthy",
            "error": str(e)
        })
