"""Client for mojo-compute service."""

from typing import List, Optional

from .base import MojoSocketClient


class MojoComputeClient(MojoSocketClient):
    """Client for communicating with mojo-compute service."""

    async def compute_sma(
        self, symbol: str, prices: List[float], period: int
    ) -> dict:
        """Compute Simple Moving Average.

        Args:
            symbol: Stock symbol
            prices: List of closing prices
            period: SMA period

        Returns:
            Response with SMA values
        """
        request = {
            "action": "compute_sma",
            "symbol": symbol,
            "prices": prices,
            "period": period,
        }
        return await self.send_request(request)

    async def compute_rsi(
        self, symbol: str, prices: List[float], period: int = 14
    ) -> dict:
        """Compute Relative Strength Index.

        Args:
            symbol: Stock symbol
            prices: List of closing prices
            period: RSI period (default 14)

        Returns:
            Response with RSI values
        """
        request = {
            "action": "compute_rsi",
            "symbol": symbol,
            "prices": prices,
            "period": period,
        }
        return await self.send_request(request)

    async def compute_ema(
        self, symbol: str, prices: List[float], period: int
    ) -> dict:
        """Compute Exponential Moving Average.

        Args:
            symbol: Stock symbol
            prices: List of closing prices
            period: EMA period

        Returns:
            Response with EMA values
        """
        request = {
            "action": "compute_ema",
            "symbol": symbol,
            "prices": prices,
            "period": period,
        }
        return await self.send_request(request)

    async def compute_macd(
        self,
        symbol: str,
        prices: List[float],
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
    ) -> dict:
        """Compute MACD indicator.

        Args:
            symbol: Stock symbol
            prices: List of closing prices
            fast_period: Fast EMA period (default 12)
            slow_period: Slow EMA period (default 26)
            signal_period: Signal line period (default 9)

        Returns:
            Response with MACD line, signal line, histogram
        """
        request = {
            "action": "compute_macd",
            "symbol": symbol,
            "prices": prices,
            "fast_period": fast_period,
            "slow_period": slow_period,
            "signal_period": signal_period,
        }
        return await self.send_request(request)

    async def compute_bollinger(
        self,
        symbol: str,
        prices: List[float],
        period: int = 20,
        std_dev: float = 2.0,
    ) -> dict:
        """Compute Bollinger Bands.

        Args:
            symbol: Stock symbol
            prices: List of closing prices
            period: BB period (default 20)
            std_dev: Standard deviations (default 2.0)

        Returns:
            Response with upper, middle, lower bands
        """
        request = {
            "action": "compute_bollinger",
            "symbol": symbol,
            "prices": prices,
            "period": period,
            "std_dev": std_dev,
        }
        return await self.send_request(request)

    async def backtest(
        self,
        strategy_id: str,
        symbol: str,
        data: dict,
        parameters: dict,
    ) -> dict:
        """Run a backtest.

        Args:
            strategy_id: Strategy identifier
            symbol: Stock symbol
            data: OHLCV data
            parameters: Strategy parameters

        Returns:
            Backtest results
        """
        request = {
            "action": "backtest",
            "strategy_id": strategy_id,
            "symbol": symbol,
            "data": data,
            "parameters": parameters,
        }
        return await self.send_request(request)
