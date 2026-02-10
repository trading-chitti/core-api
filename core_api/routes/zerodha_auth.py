"""
Zerodha Kite Authentication Routes
Handles token management for Zerodha trading API.
Tokens are stored in PostgreSQL (brokers.config table).
"""

from fastapi import APIRouter, Query, HTTPException, Request
from fastapi.responses import JSONResponse
from kiteconnect import KiteConnect
from pydantic import BaseModel
from typing import Optional
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json
import httpx
from datetime import datetime, timedelta, timezone, time as dtime

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Zerodha Auth"])

# Database connection string
PG_DSN = os.getenv('TRADING_CHITTI_PG_DSN', 'postgresql://hariprasath@localhost:6432/trading_chitti')

# IST timezone offset
IST = timezone(timedelta(hours=5, minutes=30))

# Market-bridge URL for token reload
MARKET_BRIDGE_URL = os.getenv('MARKET_BRIDGE_URL', 'http://localhost:6005')


def get_db_connection():
    """Get PostgreSQL database connection."""
    return psycopg2.connect(PG_DSN)


def get_broker_config(broker_name: str = "zerodha"):
    """Get broker configuration from database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM brokers.config WHERE broker_name = %s",
                    (broker_name,)
                )
                return cur.fetchone()
    except Exception as e:
        logger.error(f"Error fetching broker config: {e}")
        return None


def update_broker_config(
    broker_name: str,
    access_token: Optional[str] = None,
    user_id: Optional[str] = None,
    user_name: Optional[str] = None,
    enabled: Optional[bool] = None,
    token_expires_at: Optional[datetime] = None,
):
    """Update broker configuration in database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                update_fields = []
                params = []

                if access_token is not None:
                    update_fields.append("access_token = %s")
                    params.append(access_token)

                if user_id is not None:
                    update_fields.append("user_id = %s")
                    params.append(user_id)

                if enabled is not None:
                    update_fields.append("enabled = %s")
                    params.append(enabled)

                if token_expires_at is not None:
                    update_fields.append("token_expires_at = %s")
                    params.append(token_expires_at)

                update_fields.append("last_authenticated_at = %s")
                params.append(datetime.now())

                params.append(broker_name)

                query = f"""
                    UPDATE brokers.config
                    SET {', '.join(update_fields)}
                    WHERE broker_name = %s
                """

                cur.execute(query, params)
                conn.commit()
                return True
    except Exception as e:
        logger.error(f"Error updating broker config: {e}")
        return False


def get_zerodha_expiry_today() -> datetime:
    """Get today's Zerodha token expiry time (3:30 PM IST)."""
    now_ist = datetime.now(IST)
    expiry = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    return expiry


async def notify_market_bridge_reload():
    """Notify market-bridge to reload token from database."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(f"{MARKET_BRIDGE_URL}/api/reload-token")
            if response.status_code == 200:
                logger.info("✅ Market-bridge token reloaded successfully")
                return True
            else:
                logger.warning(f"⚠️ Market-bridge reload returned {response.status_code}: {response.text}")
                return False
    except Exception as e:
        logger.warning(f"⚠️ Could not notify market-bridge to reload token: {e}")
        return False


class TokenRequest(BaseModel):
    """Request model for storing access token"""
    access_token: str


class RequestTokenRequest(BaseModel):
    """Request model for exchanging request_token for access_token"""
    request_token: str


@router.get("/login-url")
async def get_login_url():
    """
    Get the Zerodha Kite login URL for generating a request token.
    The API key is read from the broker config in the database.
    """
    config = get_broker_config("zerodha")
    if not config or not config.get("api_key"):
        raise HTTPException(
            status_code=404,
            detail="Zerodha API key not configured. Please add broker config first."
        )

    api_key = config["api_key"]
    login_url = f"https://kite.zerodha.com/connect/login?v=3&api_key={api_key}"

    return {
        "login_url": login_url,
        "api_key": api_key[:8] + "...",
    }


@router.post("/request-token")
async def exchange_request_token(data: RequestTokenRequest):
    """
    Exchange a Zerodha request_token for an access_token.
    The request_token is obtained after logging in via the login URL.
    """
    request_token = data.request_token.strip()
    if not request_token:
        raise HTTPException(status_code=400, detail="Request token is required")

    config = get_broker_config("zerodha")
    if not config:
        raise HTTPException(status_code=404, detail="Zerodha broker not configured")

    api_key = config.get("api_key", "")
    api_secret = config.get("api_secret", "")

    if not api_key or not api_secret:
        raise HTTPException(
            status_code=400,
            detail="Zerodha API key and secret must be configured in broker config."
        )

    try:
        kite = KiteConnect(api_key=api_key)
        session_data = kite.generate_session(request_token, api_secret=api_secret)
    except Exception as e:
        logger.error(f"Request token exchange failed: {e}")
        raise HTTPException(
            status_code=401,
            detail=f"Failed to exchange request token: {str(e)}. Token may be expired or already used."
        )

    access_token = session_data["access_token"]
    user_id = session_data["user_id"]
    user_name = session_data.get("user_name", user_id)
    token_expiry = get_zerodha_expiry_today()

    success = update_broker_config(
        broker_name="zerodha",
        access_token=access_token,
        user_id=user_id,
        enabled=True,
        token_expires_at=token_expiry,
    )

    if not success:
        raise HTTPException(status_code=500, detail="Failed to store access token")

    # Notify market-bridge to reload
    await notify_market_bridge_reload()

    logger.info(f"✅ Request token exchanged for user {user_id} ({user_name})")

    return {
        "status": "success",
        "message": f"Connected! Welcome, {user_name}",
        "user_id": user_id,
        "user_name": user_name,
        "token_expires_at": token_expiry.isoformat(),
    }


@router.post("/token")
async def store_token(token_data: TokenRequest):
    """
    Store Zerodha access token in database.
    Validates the token by calling kite.profile() before storing.

    Body:
    {
        "access_token": "your_access_token_here"
    }
    """
    try:
        access_token = token_data.access_token.strip()

        if not access_token or len(access_token) < 10:
            raise HTTPException(
                status_code=400,
                detail="Invalid access token. Token must be at least 10 characters."
            )

        # Get API credentials from existing broker config
        config = get_broker_config("zerodha")
        if not config:
            raise HTTPException(
                status_code=404,
                detail="Zerodha broker not configured in database. Please add broker config first."
            )

        api_key = config.get("api_key", "")

        # Validate token by calling Kite API
        try:
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            profile = kite.profile()
        except Exception as e:
            logger.error(f"Token validation failed: {e}")
            raise HTTPException(
                status_code=401,
                detail=f"Invalid access token. Kite API validation failed: {str(e)}"
            )

        user_id = profile.get("user_id", "")
        user_name = profile.get("user_name", user_id)
        token_expiry = get_zerodha_expiry_today()

        # Store token in database
        success = update_broker_config(
            broker_name="zerodha",
            access_token=access_token,
            user_id=user_id,
            enabled=True,
            token_expires_at=token_expiry,
        )

        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to store token in database"
            )

        logger.info(f"✅ Zerodha token stored for user: {user_id} (expires {token_expiry.strftime('%I:%M %p IST')})")

        # Notify market-bridge to reload token
        bridge_reloaded = await notify_market_bridge_reload()

        return {
            "status": "success",
            "message": "Access token validated and stored successfully",
            "user_id": user_id,
            "user_name": user_name,
            "token_expires_at": token_expiry.isoformat(),
            "stored_at": datetime.now(IST).isoformat(),
            "market_bridge_reloaded": bridge_reloaded,
            "profile": {
                "user_id": profile.get("user_id"),
                "user_name": profile.get("user_name"),
                "email": profile.get("email"),
                "broker": profile.get("broker"),
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error storing Zerodha token: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to store token: {str(e)}"
        )


@router.get("/status")
async def check_auth_status():
    """Check Zerodha authentication status from database."""
    config = get_broker_config("zerodha")

    if not config or not config.get("enabled") or not config.get("access_token"):
        return {
            "status": "not_authenticated",
            "message": "Zerodha not connected. Please add your access token.",
        }

    # Check if token is expired
    token_expires_at = config.get("token_expires_at")
    now_ist = datetime.now(IST)
    is_expired = False
    if token_expires_at:
        # Make naive datetime aware if needed
        if token_expires_at.tzinfo is None:
            token_expires_at = token_expires_at.replace(tzinfo=IST)
        is_expired = now_ist > token_expires_at

    return {
        "status": "authenticated",
        "user_id": config.get("user_id"),
        "user_name": config.get("user_id"),  # Zerodha uses user_id as display
        "authenticated_at": config.get("last_authenticated_at").isoformat() if config.get("last_authenticated_at") else None,
        "token_expires_at": token_expires_at.isoformat() if token_expires_at else None,
        "is_expired": is_expired,
        "enabled": config.get("enabled"),
        "broker": "Zerodha Kite",
    }


@router.get("/token/{user_id}")
async def get_user_token(user_id: str):
    """Get stored access token from database."""
    config = get_broker_config("zerodha")

    if not config or not config.get("access_token"):
        raise HTTPException(
            status_code=404,
            detail="No active session. Please store a token first with POST /auth/zerodha/token"
        )

    return {
        "user_id": config.get("user_id"),
        "access_token": config["access_token"],
        "token_expires_at": config.get("token_expires_at").isoformat() if config.get("token_expires_at") else None,
        "expires": "Token valid until 3:30 PM IST today",
    }


@router.get("/callback")
async def zerodha_callback(
    request_token: str = Query(..., description="Request token from Zerodha"),
    action: str = Query(None, description="Action (usually 'login')"),
    status: str = Query(None, description="Status (usually 'success')"),
):
    """
    Legacy OAuth callback endpoint (kept for backward compatibility).
    Exchanges request_token for access_token and stores in database.
    """
    try:
        config = get_broker_config("zerodha")
        if not config:
            raise HTTPException(status_code=404, detail="Zerodha broker not configured")

        api_key = config.get("api_key", "")
        api_secret = config.get("api_secret", "")

        kite = KiteConnect(api_key=api_key)
        data = kite.generate_session(request_token, api_secret=api_secret)

        access_token = data["access_token"]
        user_id = data["user_id"]
        token_expiry = get_zerodha_expiry_today()

        # Store in database
        update_broker_config(
            broker_name="zerodha",
            access_token=access_token,
            user_id=user_id,
            enabled=True,
            token_expires_at=token_expiry,
        )

        # Notify market-bridge
        await notify_market_bridge_reload()

        return {
            "status": "success",
            "message": "Authentication successful! Token stored in database.",
            "user_id": user_id,
            "user_name": data.get("user_name", user_id),
            "access_token": access_token,
            "token_expires_at": token_expiry.isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in OAuth callback: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Authentication failed: {str(e)}"
        )


@router.delete("/logout/{user_id}")
async def logout(user_id: str):
    """Logout user and clear access token from database."""
    config = get_broker_config("zerodha")

    if not config:
        raise HTTPException(status_code=404, detail="Zerodha broker not configured")

    success = update_broker_config(
        broker_name="zerodha",
        access_token=None,
        enabled=False,
    )

    if success:
        logger.info(f"✅ Zerodha user {user_id} logged out, token cleared from database")
        return {
            "status": "success",
            "message": f"User {user_id} logged out. Token cleared from database.",
        }
    else:
        raise HTTPException(status_code=500, detail="Failed to clear token")


@router.post("/postback")
async def zerodha_postback(request: Request):
    """Postback endpoint for order/trade updates from Zerodha."""
    try:
        payload = await request.json()
        logger.info(f"Received postback: {payload}")
        return {"status": "received", "message": "Postback processed successfully"}
    except Exception as e:
        logger.error(f"Error processing postback: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/test")
async def test_api_credentials():
    """Test if Zerodha credentials are configured and token is valid."""
    config = get_broker_config("zerodha")

    if not config:
        return {
            "status": "not_configured",
            "message": "Zerodha broker not configured in database",
        }

    result = {
        "api_key": config.get("api_key", "")[:8] + "..." if config.get("api_key") else None,
        "api_secret_configured": bool(config.get("api_secret")),
        "access_token_configured": bool(config.get("access_token")),
        "enabled": config.get("enabled"),
        "token_expires_at": config.get("token_expires_at").isoformat() if config.get("token_expires_at") else None,
    }

    # Test token validity if configured
    if config.get("access_token") and config.get("api_key"):
        try:
            kite = KiteConnect(api_key=config["api_key"])
            kite.set_access_token(config["access_token"])
            profile = kite.profile()
            result["status"] = "valid"
            result["user_id"] = profile.get("user_id")
            result["message"] = "✅ Token is valid and working"
        except Exception as e:
            result["status"] = "invalid"
            result["message"] = f"❌ Token validation failed: {str(e)}"
    else:
        result["status"] = "incomplete"
        result["message"] = "Missing API key or access token"

    return result
