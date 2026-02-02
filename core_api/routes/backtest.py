"""
Backtesting API routes.

Endpoints for running backtests, retrieving results, and managing strategies.
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from datetime import datetime, date
import asyncio
import uuid

router = APIRouter(prefix="/api/backtest", tags=["Backtesting"])


# Request/Response models

class BacktestRequest(BaseModel):
    """Request to run a backtest."""
    strategy_id: str = Field(..., description="Strategy identifier (e.g., 'ma_crossover')")
    symbols: List[str] = Field(..., description="Symbols to backtest")
    start_date: date = Field(..., description="Backtest start date")
    end_date: date = Field(..., description="Backtest end date")
    initial_capital: float = Field(100000.0, description="Initial capital")
    parameters: Dict = Field(default_factory=dict, description="Strategy parameters")


class BacktestResponse(BaseModel):
    """Backtest result."""
    run_id: str
    status: str  # pending, running, completed, failed
    strategy_id: str
    symbols: List[str]
    start_date: date
    end_date: date
    initial_capital: float
    parameters: Dict

    # Results (null if not completed)
    metrics: Optional[Dict] = None
    equity_curve: Optional[List[Dict]] = None
    trades: Optional[List[Dict]] = None
    error: Optional[str] = None

    created_at: datetime
    completed_at: Optional[datetime] = None


class StrategyInfo(BaseModel):
    """Strategy metadata."""
    strategy_id: str
    name: str
    category: str
    description: str
    parameters: List[Dict]
    implemented: bool


# In-memory storage (replace with database in production)
backtest_runs: Dict[str, BacktestResponse] = {}


# Routes

@router.post("/run", response_model=BacktestResponse)
async def run_backtest(
    request: BacktestRequest,
    background_tasks: BackgroundTasks
):
    """
    Run a backtest for a strategy.

    Returns immediately with run_id. Use GET /runs/{run_id} to check status.
    """
    # Generate run ID
    run_id = str(uuid.uuid4())

    # Create response object
    response = BacktestResponse(
        run_id=run_id,
        status="pending",
        strategy_id=request.strategy_id,
        symbols=request.symbols,
        start_date=request.start_date,
        end_date=request.end_date,
        initial_capital=request.initial_capital,
        parameters=request.parameters,
        created_at=datetime.now()
    )

    # Store in memory
    backtest_runs[run_id] = response

    # Schedule background task
    background_tasks.add_task(_run_backtest_task, run_id, request)

    return response


async def _run_backtest_task(run_id: str, request: BacktestRequest):
    """
    Background task to run backtest.

    This would call the mojo-compute service or run locally.
    """
    try:
        # Update status
        backtest_runs[run_id].status = "running"

        # Call mojo-compute backtesting engine (not yet implemented)
        # Raise error since backtest engine is not ready
        raise NotImplementedError(
            "Backtesting engine not yet implemented. "
            "The Mojo-based backtesting engine is under development."
        )

    except Exception as e:
        backtest_runs[run_id].status = "failed"
        backtest_runs[run_id].error = str(e)
        backtest_runs[run_id].completed_at = datetime.now()


@router.get("/runs", response_model=List[BacktestResponse])
async def list_backtest_runs(
    status: Optional[str] = None,
    strategy_id: Optional[str] = None,
    limit: int = 100
):
    """List backtest runs with optional filters."""
    runs = list(backtest_runs.values())

    # Apply filters
    if status:
        runs = [r for r in runs if r.status == status]
    if strategy_id:
        runs = [r for r in runs if r.strategy_id == strategy_id]

    # Sort by created_at descending
    runs.sort(key=lambda r: r.created_at, reverse=True)

    return runs[:limit]


@router.get("/runs/{run_id}", response_model=BacktestResponse)
async def get_backtest_run(run_id: str):
    """Get backtest run details."""
    if run_id not in backtest_runs:
        raise HTTPException(status_code=404, detail="Backtest run not found")

    return backtest_runs[run_id]


@router.delete("/runs/{run_id}")
async def delete_backtest_run(run_id: str):
    """Delete a backtest run."""
    if run_id not in backtest_runs:
        raise HTTPException(status_code=404, detail="Backtest run not found")

    del backtest_runs[run_id]
    return {"message": "Backtest run deleted"}


@router.get("/strategies", response_model=List[StrategyInfo])
async def list_strategies():
    """List all available strategies."""
    # This would query the strategy registry
    strategies = [
        StrategyInfo(
            strategy_id="ma_crossover",
            name="Moving Average Crossover",
            category="Trend-following",
            description="Buy when fast MA crosses above slow MA",
            parameters=[
                {"name": "fast_period", "type": "int", "default": 20},
                {"name": "slow_period", "type": "int", "default": 50},
                {"name": "position_size", "type": "float", "default": 0.1}
            ],
            implemented=True
        ),
        StrategyInfo(
            strategy_id="rsi_reversal",
            name="RSI Reversal",
            category="Mean Reversion",
            description="Buy oversold, sell overbought",
            parameters=[
                {"name": "period", "type": "int", "default": 14},
                {"name": "oversold", "type": "int", "default": 30},
                {"name": "overbought", "type": "int", "default": 70}
            ],
            implemented=True
        ),
        StrategyInfo(
            strategy_id="bollinger_reversion",
            name="Bollinger Bands Reversion",
            category="Mean Reversion",
            description="Buy at lower band, sell at upper band",
            parameters=[
                {"name": "period", "type": "int", "default": 20},
                {"name": "std_dev", "type": "float", "default": 2.0}
            ],
            implemented=True
        ),
        StrategyInfo(
            strategy_id="orb",
            name="Opening Range Breakout",
            category="Breakout",
            description="Trade breakouts from first N minutes",
            parameters=[
                {"name": "range_minutes", "type": "int", "default": 15},
                {"name": "use_stop_loss", "type": "bool", "default": True}
            ],
            implemented=True
        ),
        StrategyInfo(
            strategy_id="donchian_breakout",
            name="Donchian Channel Breakout",
            category="Breakout",
            description="Buy N-period highs, sell N-period lows",
            parameters=[
                {"name": "entry_period", "type": "int", "default": 20},
                {"name": "exit_period", "type": "int", "default": 10}
            ],
            implemented=True
        ),
    ]

    return strategies


@router.get("/strategies/{strategy_id}", response_model=StrategyInfo)
async def get_strategy(strategy_id: str):
    """Get strategy details."""
    strategies = await list_strategies()
    for strategy in strategies:
        if strategy.strategy_id == strategy_id:
            return strategy

    raise HTTPException(status_code=404, detail="Strategy not found")
