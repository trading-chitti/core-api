"""
Zerodha Kite OAuth Routes
Handles authentication flow for Zerodha trading API
"""

from fastapi import APIRouter, Query, HTTPException, Request
from fastapi.responses import RedirectResponse, JSONResponse
from kiteconnect import KiteConnect
import logging
from datetime import datetime
from ..config import settings

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Zerodha OAuth"])

# Get credentials from config
KITE_API_KEY = settings.KITE_API_KEY
KITE_API_SECRET = settings.KITE_API_SECRET

# Store for access tokens (in production, use database/Redis)
ACCESS_TOKENS = {}


@router.get("/login")
async def zerodha_login():
    """
    Initiate Zerodha OAuth login flow.
    Redirects user to Zerodha login page.
    """
    try:
        kite = KiteConnect(api_key=KITE_API_KEY)
        login_url = kite.login_url()
        
        logger.info(f"Generated Zerodha login URL: {login_url}")
        
        return {
            "status": "redirect",
            "login_url": login_url,
            "message": "Redirect user to this URL for Zerodha login"
        }
    except Exception as e:
        logger.error(f"Error generating login URL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/callback")
async def zerodha_callback(
    request_token: str = Query(..., description="Request token from Zerodha"),
    action: str = Query(None, description="Action (usually 'login')"),
    status: str = Query(None, description="Status (usually 'success')"),
):
    """
    OAuth callback endpoint.
    Zerodha redirects here after user logs in.
    
    Exchanges request_token for access_token.
    """
    try:
        logger.info(f"Received callback with request_token: {request_token[:20]}...")
        
        # Initialize Kite
        kite = KiteConnect(api_key=KITE_API_KEY)
        
        # Generate session (exchange request_token for access_token)
        data = kite.generate_session(request_token, api_secret=KITE_API_SECRET)
        
        # Extract tokens and user info
        access_token = data["access_token"]
        user_id = data["user_id"]
        refresh_token = data.get("refresh_token")
        user_name = data.get("user_name", user_id)
        
        # Store access token (in production, store in database with encryption)
        ACCESS_TOKENS[user_id] = {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "user_id": user_id,
            "user_name": user_name,
            "created_at": datetime.now().isoformat(),
        }
        
        logger.info(f"Successfully authenticated user: {user_id}")
        
        return {
            "status": "success",
            "message": "Authentication successful! Save this access token.",
            "user_id": user_id,
            "user_name": user_name,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "login_time": data.get("login_time"),
            "expires": "This token is valid until end of trading day (3:30 PM IST)",
            "next_steps": [
                "Save the access_token securely",
                "Use it for API calls: kite.set_access_token(access_token)",
                "Token expires at end of trading day, re-login next day"
            ]
        }
        
    except Exception as e:
        logger.error(f"Error in OAuth callback: {e}")
        raise HTTPException(
            status_code=400, 
            detail=f"Authentication failed: {str(e)}"
        )


@router.get("/status")
async def check_auth_status():
    """Check if any users are authenticated."""
    if not ACCESS_TOKENS:
        return {
            "status": "not_authenticated",
            "message": "No active sessions. Please login at /auth/zerodha/login"
        }
    
    users = []
    for user_id, data in ACCESS_TOKENS.items():
        users.append({
            "user_id": user_id,
            "user_name": data.get("user_name"),
            "authenticated_at": data.get("created_at"),
        })
    
    return {
        "status": "authenticated",
        "active_users": len(ACCESS_TOKENS),
        "users": users
    }


@router.get("/token/{user_id}")
async def get_user_token(user_id: str):
    """Get stored access token for a user."""
    if user_id not in ACCESS_TOKENS:
        raise HTTPException(
            status_code=404,
            detail=f"No active session for user: {user_id}. Please login first."
        )
    
    data = ACCESS_TOKENS[user_id]
    return {
        "user_id": user_id,
        "access_token": data["access_token"],
        "created_at": data["created_at"],
        "expires": "End of trading day (3:30 PM IST)"
    }


@router.post("/postback")
async def zerodha_postback(request: Request):
    """
    Postback endpoint for order/trade updates from Zerodha.
    Optional: Configure in Zerodha app for real-time order updates.
    """
    try:
        payload = await request.json()
        logger.info(f"Received postback: {payload}")
        
        # TODO: Process order/trade updates
        # - Store in database
        # - Trigger alerts
        # - Update positions
        
        return {
            "status": "received",
            "message": "Postback processed successfully"
        }
    except Exception as e:
        logger.error(f"Error processing postback: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/logout/{user_id}")
async def logout(user_id: str):
    """Logout user and invalidate access token."""
    if user_id in ACCESS_TOKENS:
        del ACCESS_TOKENS[user_id]
        return {
            "status": "success",
            "message": f"User {user_id} logged out successfully"
        }
    else:
        raise HTTPException(
            status_code=404,
            detail=f"User {user_id} not found"
        )


@router.get("/test")
async def test_api_credentials():
    """Test if API credentials are configured correctly."""
    return {
        "api_key": KITE_API_KEY,
        "api_secret_configured": bool(KITE_API_SECRET),
        "api_secret_length": len(KITE_API_SECRET) if KITE_API_SECRET else 0,
        "kiteconnect_version": "4.0+",
        "status": "ready"
    }
