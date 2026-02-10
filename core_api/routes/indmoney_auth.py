"""
Ind Money (IndStocks) API Authentication Routes
Handles token management and authentication for Ind Money trading API
"""

from fastapi import APIRouter, HTTPException, Request, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import logging
from datetime import datetime, timedelta, timezone
import httpx

# IST timezone offset
IST = timezone(timedelta(hours=5, minutes=30))
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json
from ..config import settings

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Ind Money Auth"])

# Database connection string
PG_DSN = os.getenv('TRADING_CHITTI_PG_DSN', 'postgresql://hariprasath@localhost:6432/trading_chitti')

# Base URL for Ind Money API
INDMONEY_BASE_URL = "https://api.indstocks.com"


def get_db_connection():
    """Get PostgreSQL database connection."""
    return psycopg2.connect(PG_DSN)


def get_broker_config(broker_name: str = "indmoney"):
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
    additional_config: Optional[dict] = None,
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

                if user_name is not None:
                    update_fields.append("user_name = %s")
                    params.append(user_name)

                if enabled is not None:
                    update_fields.append("enabled = %s")
                    params.append(enabled)

                if additional_config is not None:
                    update_fields.append("additional_config = %s")
                    params.append(json.dumps(additional_config))

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


class TokenRequest(BaseModel):
    """Request model for storing access token"""
    access_token: str
    user_id: Optional[str] = None
    user_name: Optional[str] = None


class TestCredentialsRequest(BaseModel):
    """Request model for testing API credentials"""
    access_token: str


@router.get("/login")
async def indmoney_login():
    """
    Return instructions for Ind Money authentication.
    Unlike Zerodha, Ind Money uses direct API token authentication.
    """
    return {
        "status": "info",
        "message": "Ind Money Authentication Instructions",
        "authentication_type": "API Token",
        "steps": [
            "1. Login to your Ind Money account at https://indmoney.com",
            "2. Navigate to Developer/API Settings in your account",
            "3. Generate or retrieve your API access token",
            "4. Use POST /auth/indmoney/token to save your token",
            "5. Test connection with GET /auth/indmoney/test"
        ],
        "documentation": "https://api-docs.indstocks.com/introduction/",
        "next_endpoint": "POST /auth/indmoney/token"
    }


@router.post("/token")
async def store_token(token_data: TokenRequest):
    """
    Store Ind Money access token in database.

    Body:
    {
        "access_token": "your_token_here",
        "user_id": "optional_user_id",
        "user_name": "optional_user_name"
    }
    """
    try:
        access_token = token_data.access_token

        if not access_token or len(access_token) < 10:
            raise HTTPException(
                status_code=400,
                detail="Invalid access token. Token must be at least 10 characters."
            )

        # Validate token by making a test API call
        async with httpx.AsyncClient(timeout=10.0) as client:
            headers = {
                "Authorization": access_token,
                "Content-Type": "application/json"
            }

            try:
                # Test with profile endpoint
                response = await client.get(
                    f"{INDMONEY_BASE_URL}/user/profile",
                    headers=headers
                )

                if response.status_code == 200:
                    profile_data = response.json()
                    user_id = token_data.user_id or profile_data.get("user_id", "default")
                    user_name = token_data.user_name or profile_data.get("name", user_id)

                    # Calculate token expiry (tomorrow 7:00 AM IST)
                    now_ist = datetime.now(IST)
                    tomorrow = now_ist + timedelta(days=1)
                    indmoney_expiry = tomorrow.replace(hour=7, minute=0, second=0, microsecond=0)

                    # Store token in database
                    success = update_broker_config(
                        broker_name="indmoney",
                        access_token=access_token,
                        user_id=user_id,
                        user_name=user_name,
                        enabled=True,
                        additional_config={"profile": profile_data},
                        token_expires_at=indmoney_expiry,
                    )

                    if not success:
                        raise HTTPException(
                            status_code=500,
                            detail="Failed to store token in database"
                        )

                    logger.info(f"Successfully stored Ind Money token for user: {user_id}")

                    return {
                        "status": "success",
                        "message": "Token validated and stored successfully in database",
                        "user_id": user_id,
                        "user_name": user_name,
                        "stored_at": datetime.now().isoformat(),
                        "profile": profile_data,
                        "next_steps": [
                            "Token is now active and ready to use",
                            "Check status with GET /auth/indmoney/status",
                            "Test API calls with GET /auth/indmoney/test"
                        ]
                    }
                elif response.status_code == 401:
                    raise HTTPException(
                        status_code=401,
                        detail="Invalid access token. Please check your token and try again."
                    )
                else:
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=f"Token validation failed: {response.text}"
                    )

            except httpx.TimeoutException:
                raise HTTPException(
                    status_code=504,
                    detail="Timeout connecting to Ind Money API. Please try again."
                )
            except httpx.RequestError as e:
                raise HTTPException(
                    status_code=503,
                    detail=f"Network error: {str(e)}"
                )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error storing Ind Money token: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to store token: {str(e)}"
        )


@router.get("/status")
async def check_auth_status():
    """Check if Ind Money authentication is configured."""
    config = get_broker_config("indmoney")

    if not config or not config.get("enabled") or not config.get("access_token"):
        return {
            "status": "not_authenticated",
            "message": "Ind Money not configured or token not set",
            "next_steps": [
                "Get instructions: GET /auth/indmoney/login",
                "Store token: POST /auth/indmoney/token"
            ]
        }

    # Check if token is expired
    token_expires_at = config.get("token_expires_at")
    is_expired = False
    if token_expires_at:
        now_ist = datetime.now(IST)
        if token_expires_at.tzinfo is None:
            token_expires_at = token_expires_at.replace(tzinfo=IST)
        is_expired = now_ist > token_expires_at

    return {
        "status": "authenticated",
        "enabled": config["enabled"],
        "user_id": config.get("user_id"),
        "user_name": config.get("user_name"),
        "authenticated_at": config.get("last_authenticated_at").isoformat() if config.get("last_authenticated_at") else None,
        "token_configured": bool(config.get("access_token")),
        "token_length": len(config.get("access_token", "")),
        "token_expires_at": token_expires_at.isoformat() if token_expires_at else None,
        "is_expired": is_expired,
        "broker": "Ind Money (IndStocks)"
    }


@router.get("/token")
async def get_token():
    """Get stored access token from database."""
    config = get_broker_config("indmoney")

    if not config or not config.get("access_token"):
        raise HTTPException(
            status_code=404,
            detail="No active session. Please store token first with POST /auth/indmoney/token"
        )

    return {
        "user_id": config.get("user_id"),
        "user_name": config.get("user_name"),
        "access_token": config["access_token"],
        "enabled": config.get("enabled"),
        "authenticated_at": config.get("last_authenticated_at").isoformat() if config.get("last_authenticated_at") else None,
        "token_type": "Direct",
        "expires": "Check Ind Money documentation for token expiry"
    }


@router.post("/test")
async def test_api_connection(request: TestCredentialsRequest):
    """
    Test API connection with provided credentials.
    Makes test calls to verify token works.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            headers = {
                "Authorization": request.access_token,
                "Content-Type": "application/json"
            }

            results = {
                "tests": [],
                "overall_status": "unknown"
            }

            # Test 1: User Profile
            try:
                response = await client.get(
                    f"{INDMONEY_BASE_URL}/user/profile",
                    headers=headers
                )
                results["tests"].append({
                    "endpoint": "/user/profile",
                    "status": "success" if response.status_code == 200 else "failed",
                    "status_code": response.status_code,
                    "data": response.json() if response.status_code == 200 else None
                })
            except Exception as e:
                results["tests"].append({
                    "endpoint": "/user/profile",
                    "status": "error",
                    "error": str(e)
                })

            # Test 2: Funds/Margins
            try:
                response = await client.get(
                    f"{INDMONEY_BASE_URL}/funds",
                    headers=headers
                )
                results["tests"].append({
                    "endpoint": "/funds",
                    "status": "success" if response.status_code == 200 else "failed",
                    "status_code": response.status_code,
                    "data": response.json() if response.status_code == 200 else None
                })
            except Exception as e:
                results["tests"].append({
                    "endpoint": "/funds",
                    "status": "error",
                    "error": str(e)
                })

            # Test 3: Market Quote (RELIANCE)
            try:
                response = await client.get(
                    f"{INDMONEY_BASE_URL}/market/quotes/ltp",
                    params={"symbol": "NSE:RELIANCE"},
                    headers=headers
                )
                results["tests"].append({
                    "endpoint": "/market/quotes/ltp",
                    "status": "success" if response.status_code == 200 else "failed",
                    "status_code": response.status_code,
                    "data": response.json() if response.status_code == 200 else None
                })
            except Exception as e:
                results["tests"].append({
                    "endpoint": "/market/quotes/ltp",
                    "status": "error",
                    "error": str(e)
                })

            # Determine overall status
            success_count = sum(1 for t in results["tests"] if t.get("status") == "success")
            total_tests = len(results["tests"])

            if success_count == total_tests:
                results["overall_status"] = "all_passed"
                results["message"] = "✅ All API endpoints accessible"
            elif success_count > 0:
                results["overall_status"] = "partial"
                results["message"] = f"⚠️ {success_count}/{total_tests} tests passed"
            else:
                results["overall_status"] = "all_failed"
                results["message"] = "❌ All tests failed. Check your access token."

            results["success_rate"] = f"{success_count}/{total_tests}"

            return results

    except Exception as e:
        logger.error(f"Error testing Ind Money API: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Test failed: {str(e)}"
        )


@router.get("/test")
async def test_stored_credentials():
    """
    Test stored credentials from database.
    """
    config = get_broker_config("indmoney")

    if not config or not config.get("access_token"):
        return {
            "status": "not_configured",
            "message": "No token configured. Store a token first with POST /auth/indmoney/token"
        }

    access_token = config["access_token"]

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            headers = {
                "Authorization": access_token,
                "Content-Type": "application/json"
            }

            # Quick test with profile endpoint
            response = await client.get(
                f"{INDMONEY_BASE_URL}/user/profile",
                headers=headers
            )

            result = {
                "status": "valid" if response.status_code == 200 else "invalid",
                "status_code": response.status_code,
                "user_id": config.get("user_id"),
                "user_name": config.get("user_name"),
                "authenticated_at": config.get("last_authenticated_at").isoformat() if config.get("last_authenticated_at") else None,
                "enabled": config.get("enabled")
            }

            if response.status_code == 200:
                result["profile"] = response.json()

            return result

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "user_id": config.get("user_id")
        }


@router.delete("/logout")
async def logout():
    """Disable Ind Money integration and clear token."""
    try:
        success = update_broker_config(
            broker_name="indmoney",
            access_token=None,
            enabled=False
        )

        if success:
            logger.info("Disabled Ind Money integration and cleared token")
            return {
                "status": "success",
                "message": "Ind Money integration disabled and token cleared",
                "broker": "indmoney"
            }
        else:
            raise HTTPException(
                status_code=500,
                detail="Failed to update broker configuration"
            )
    except Exception as e:
        logger.error(f"Error during logout: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Logout failed: {str(e)}"
        )


@router.get("/info")
async def api_info():
    """Get information about Ind Money API integration."""
    return {
        "broker": "Ind Money (IndStocks)",
        "api_base_url": INDMONEY_BASE_URL,
        "authentication_type": "API Token (Direct)",
        "documentation": "https://api-docs.indstocks.com/introduction/",
        "endpoints": {
            "login_info": "GET /auth/indmoney/login",
            "store_token": "POST /auth/indmoney/token",
            "check_status": "GET /auth/indmoney/status",
            "test_connection": "POST /auth/indmoney/test",
            "test_stored": "GET /auth/indmoney/test",
            "get_token": "GET /auth/indmoney/token",
            "logout": "DELETE /auth/indmoney/logout",
            "api_info": "GET /auth/indmoney/info"
        },
        "supported_features": [
            "User profile and funds",
            "Market quotes (LTP, Full)",
            "Historical data (1min, 5min, 15min, 1hour, 1day)",
            "Portfolio (Positions, Holdings)",
            "Order management (Place, Modify, Cancel)",
            "Order book",
            "Instruments/Securities master"
        ],
        "integration_status": "active",
        "multi_broker_support": True
    }
