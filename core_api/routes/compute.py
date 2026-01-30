"""Routes for mojo-compute service."""

from typing import List

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

router = APIRouter()


# Request/Response models
class SMARequest(BaseModel):
    """Request model for SMA calculation."""

    symbol: str = Field(..., min_length=1, max_length=20)
    prices: List[float] = Field(..., min_length=1, max_length=1_000_000)
    period: int = Field(..., ge=2, le=500)


class RSIRequest(BaseModel):
    """Request model for RSI calculation."""

    symbol: str = Field(..., min_length=1, max_length=20)
    prices: List[float] = Field(..., min_length=1, max_length=1_000_000)
    period: int = Field(14, ge=2, le=100)


class MACDRequest(BaseModel):
    """Request model for MACD calculation."""

    symbol: str = Field(..., min_length=1, max_length=20)
    prices: List[float] = Field(..., min_length=1, max_length=1_000_000)
    fast_period: int = Field(12, ge=2, le=100)
    slow_period: int = Field(26, ge=2, le=200)
    signal_period: int = Field(9, ge=2, le=50)


class BollingerRequest(BaseModel):
    """Request model for Bollinger Bands calculation."""

    symbol: str = Field(..., min_length=1, max_length=20)
    prices: List[float] = Field(..., min_length=1, max_length=1_000_000)
    period: int = Field(20, ge=2, le=200)
    std_dev: float = Field(2.0, ge=0.1, le=5.0)


@router.post("/sma")
async def compute_sma(request_model: SMARequest, request: Request):
    """Calculate Simple Moving Average via mojo-compute."""
    try:
        client = request.app.state.mojo_compute
        response = await client.compute_sma(
            symbol=request_model.symbol,
            prices=request_model.prices,
            period=request_model.period,
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Mojo compute error: {str(e)}")


@router.post("/rsi")
async def compute_rsi(request_model: RSIRequest, request: Request):
    """Calculate Relative Strength Index via mojo-compute."""
    try:
        client = request.app.state.mojo_compute
        response = await client.compute_rsi(
            symbol=request_model.symbol,
            prices=request_model.prices,
            period=request_model.period,
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Mojo compute error: {str(e)}")


@router.post("/macd")
async def compute_macd(request_model: MACDRequest, request: Request):
    """Calculate MACD indicator via mojo-compute."""
    try:
        client = request.app.state.mojo_compute
        response = await client.compute_macd(
            symbol=request_model.symbol,
            prices=request_model.prices,
            fast_period=request_model.fast_period,
            slow_period=request_model.slow_period,
            signal_period=request_model.signal_period,
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Mojo compute error: {str(e)}")


@router.post("/bollinger")
async def compute_bollinger(request_model: BollingerRequest, request: Request):
    """Calculate Bollinger Bands via mojo-compute."""
    try:
        client = request.app.state.mojo_compute
        response = await client.compute_bollinger(
            symbol=request_model.symbol,
            prices=request_model.prices,
            period=request_model.period,
            std_dev=request_model.std_dev,
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Mojo compute error: {str(e)}")
