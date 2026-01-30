"""Client for news-nlp service (Mojo)."""

from typing import List, Optional

from .base import MojoSocketClient


class NewsNLPClient(MojoSocketClient):
    """Client for communicating with news-nlp service (Mojo)."""

    async def ingest_rss(self, url: str) -> dict:
        """Trigger RSS feed ingestion.

        Args:
            url: RSS feed URL

        Returns:
            Response with ingestion results
        """
        request = {
            "action": "ingest_rss",
            "url": url,
        }
        return await self.send_request(request)

    async def analyze_sentiment(self, text: str) -> dict:
        """Analyze sentiment of text.

        Args:
            text: Text to analyze (article title + summary)

        Returns:
            Response with sentiment score and classification
        """
        request = {
            "action": "analyze_sentiment",
            "text": text,
        }
        return await self.send_request(request)

    async def extract_entities(self, text: str) -> dict:
        """Extract named entities (symbols, sectors) from text.

        Args:
            text: Text to analyze

        Returns:
            Response with extracted entities
        """
        request = {
            "action": "extract_entities",
            "text": text,
        }
        return await self.send_request(request)

    async def classify_direction(
        self, text: str, sentiment: Optional[float] = None
    ) -> dict:
        """Classify market direction from text.

        Args:
            text: Article text
            sentiment: Pre-computed sentiment score (optional)

        Returns:
            Response with direction (bullish/bearish/neutral/unclear)
        """
        request = {
            "action": "classify_direction",
            "text": text,
        }

        if sentiment is not None:
            request["sentiment"] = sentiment

        return await self.send_request(request)

    async def get_articles(
        self,
        limit: int = 100,
        offset: int = 0,
        symbol: Optional[str] = None,
        sector: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> dict:
        """Get articles from database.

        Args:
            limit: Maximum number of articles
            offset: Offset for pagination
            symbol: Filter by symbol (optional)
            sector: Filter by sector (optional)
            start_date: Filter by start date (ISO format, optional)
            end_date: Filter by end date (ISO format, optional)

        Returns:
            Response with articles list
        """
        request = {
            "action": "get_articles",
            "limit": limit,
            "offset": offset,
        }

        if symbol:
            request["symbol"] = symbol
        if sector:
            request["sector"] = sector
        if start_date:
            request["start_date"] = start_date
        if end_date:
            request["end_date"] = end_date

        return await self.send_request(request)
