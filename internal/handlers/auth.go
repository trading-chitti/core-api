package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// GetZerodhaLoginUrl returns the Zerodha Kite login URL with the configured API key
func (h *Handler) GetZerodhaLoginUrl(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	config, err := h.db.GetBrokerConfig(ctx, "zerodha")
	if err != nil {
		log.Printf("Failed to get broker config: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to fetch broker configuration"})
		return
	}

	if config == nil || config.APIKey == "" {
		c.JSON(http.StatusNotFound, gin.H{
			"detail": "Zerodha API key not configured. Add credentials to brokers.config table.",
		})
		return
	}

	loginURL := fmt.Sprintf("https://kite.zerodha.com/connect/login?v=3&api_key=%s", config.APIKey)

	c.JSON(http.StatusOK, gin.H{
		"login_url": loginURL,
		"api_key":   config.APIKey,
	})
}

// ExchangeRequestToken exchanges the Zerodha request token for an access token
func (h *Handler) ExchangeRequestToken(c *gin.Context) {
	var body struct {
		RequestToken string `json:"request_token"`
	}

	if err := c.ShouldBindJSON(&body); err != nil || body.RequestToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Missing or invalid request_token"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config, err := h.db.GetBrokerConfig(ctx, "zerodha")
	if err != nil || config == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Broker config not found"})
		return
	}

	if config.APIKey == "" || config.APISecret == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "API key or secret not configured"})
		return
	}

	// Generate checksum: SHA256(api_key + request_token + api_secret)
	checksumInput := config.APIKey + body.RequestToken + config.APISecret
	checksum := fmt.Sprintf("%x", sha256.Sum256([]byte(checksumInput)))

	// Call Kite session token API
	formData := url.Values{
		"api_key":       {config.APIKey},
		"request_token": {body.RequestToken},
		"checksum":      {checksum},
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "https://api.kite.trade/session/token",
		strings.NewReader(formData.Encode()))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create request"})
		return
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Kite-Version", "3")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Kite API error: %v", err)
		c.JSON(http.StatusBadGateway, gin.H{"detail": fmt.Sprintf("Kite API error: %v", err)})
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	var kiteResp struct {
		Status string `json:"status"`
		Data   struct {
			UserID      string `json:"user_id"`
			UserName    string `json:"user_name"`
			AccessToken string `json:"access_token"`
			LoginTime   string `json:"login_time"`
		} `json:"data"`
		Message   string `json:"message"`
		ErrorType string `json:"error_type"`
	}

	if err := json.Unmarshal(respBody, &kiteResp); err != nil {
		log.Printf("Failed to parse Kite response: %s", string(respBody))
		c.JSON(http.StatusBadGateway, gin.H{"detail": "Invalid response from Kite API"})
		return
	}

	if kiteResp.Status != "success" || kiteResp.Data.AccessToken == "" {
		log.Printf("Kite token exchange failed: %s - %s", kiteResp.ErrorType, kiteResp.Message)
		c.JSON(http.StatusBadRequest, gin.H{
			"detail":     kiteResp.Message,
			"error_type": kiteResp.ErrorType,
		})
		return
	}

	// Zerodha tokens expire at 3:30 PM IST same day (generated after 12 AM IST)
	ist, _ := time.LoadLocation("Asia/Kolkata")
	now := time.Now().In(ist)
	expiresAt := time.Date(now.Year(), now.Month(), now.Day(), 15, 30, 0, 0, ist)
	if now.After(expiresAt) {
		expiresAt = expiresAt.Add(24 * time.Hour)
	}

	// Store token in database
	if err := h.db.UpdateBrokerToken(ctx, "zerodha",
		kiteResp.Data.AccessToken, kiteResp.Data.UserID, expiresAt); err != nil {
		log.Printf("Failed to store token: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Token received but failed to store"})
		return
	}

	log.Printf("✅ Zerodha token exchanged for user %s", kiteResp.Data.UserID)

	c.JSON(http.StatusOK, gin.H{
		"status":           "success",
		"user_id":          kiteResp.Data.UserID,
		"user_name":        kiteResp.Data.UserName,
		"token_expires_at": expiresAt.Format(time.RFC3339),
		"authenticated":    true,
	})
}

// SaveAccessToken saves the Zerodha access token to the database (direct token mode)
func (h *Handler) SaveAccessToken(c *gin.Context) {
	var body struct {
		AccessToken string `json:"access_token"`
		UserID      string `json:"user_id"`
	}

	if err := c.ShouldBindJSON(&body); err != nil || body.AccessToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Missing or invalid access_token"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config, err := h.db.GetBrokerConfig(ctx, "zerodha")
	if err != nil || config == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Broker config not found"})
		return
	}

	// Validate token by calling Kite profile API
	profileReq, err := http.NewRequestWithContext(ctx, "GET", "https://api.kite.trade/user/profile", nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to create validation request"})
		return
	}
	profileReq.Header.Set("X-Kite-Version", "3")
	profileReq.Header.Set("Authorization", fmt.Sprintf("token %s:%s", config.APIKey, body.AccessToken))

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(profileReq)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"detail": fmt.Sprintf("Failed to validate token: %v", err)})
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	var profileResp struct {
		Status string `json:"status"`
		Data   struct {
			UserID   string `json:"user_id"`
			UserName string `json:"user_name"`
		} `json:"data"`
		Message   string `json:"message"`
		ErrorType string `json:"error_type"`
	}

	if err := json.Unmarshal(respBody, &profileResp); err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"detail": "Invalid response from Kite API"})
		return
	}

	if profileResp.Status != "success" {
		c.JSON(http.StatusBadRequest, gin.H{
			"detail":     fmt.Sprintf("Invalid token: %s", profileResp.Message),
			"error_type": profileResp.ErrorType,
		})
		return
	}

	userID := profileResp.Data.UserID
	if body.UserID != "" {
		userID = body.UserID
	}

	// Zerodha tokens expire at 3:30 PM IST same day (generated after 12 AM IST)
	ist, _ := time.LoadLocation("Asia/Kolkata")
	now := time.Now().In(ist)
	expiresAt := time.Date(now.Year(), now.Month(), now.Day(), 15, 30, 0, 0, ist)
	if now.After(expiresAt) {
		expiresAt = expiresAt.Add(24 * time.Hour)
	}

	if err := h.db.UpdateBrokerToken(ctx, "zerodha", body.AccessToken, userID, expiresAt); err != nil {
		log.Printf("Failed to store token: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store token"})
		return
	}

	log.Printf("✅ Zerodha access token saved for user %s", userID)

	c.JSON(http.StatusOK, gin.H{
		"status":           "success",
		"user_id":          userID,
		"user_name":        profileResp.Data.UserName,
		"token_expires_at": expiresAt.Format(time.RFC3339),
		"authenticated":    true,
	})
}

// GetZerodhaAuthStatus returns the current Zerodha authentication status
func (h *Handler) GetZerodhaAuthStatus(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	config, err := h.db.GetBrokerConfig(ctx, "zerodha")
	if err != nil {
		log.Printf("Failed to get broker config: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to check auth status"})
		return
	}

	if config == nil || config.AccessToken == "" {
		c.JSON(http.StatusOK, gin.H{
			"status":        "not_authenticated",
			"authenticated": false,
		})
		return
	}

	isExpired := false
	if config.TokenExpiresAt != nil {
		isExpired = time.Now().After(*config.TokenExpiresAt)
	}

	status := "authenticated"
	if isExpired {
		status = "expired"
	}

	result := gin.H{
		"status":        status,
		"authenticated": config.Enabled && !isExpired,
		"user_id":       config.UserID,
		"enabled":       config.Enabled,
		"is_expired":    isExpired,
	}

	if config.TokenExpiresAt != nil {
		result["token_expires_at"] = config.TokenExpiresAt.Format(time.RFC3339)
	}
	if config.LastAuthenticatedAt != nil {
		result["authenticated_at"] = config.LastAuthenticatedAt.Format(time.RFC3339)
	}

	c.JSON(http.StatusOK, result)
}

// LogoutZerodha logs out the user and invalidates the Zerodha token
func (h *Handler) LogoutZerodha(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := h.db.ClearBrokerToken(ctx, "zerodha"); err != nil {
		log.Printf("Failed to clear token: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to logout"})
		return
	}

	log.Println("✅ Zerodha token cleared")

	c.JSON(http.StatusOK, gin.H{
		"status":  "logged_out",
		"message": "Zerodha token cleared successfully",
	})
}

// parseJWTExpiry extracts the "exp" claim from a JWT token without verifying the signature.
// Returns the expiry time or a fallback if parsing fails.
func parseJWTExpiry(token string, fallback time.Time) time.Time {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return fallback
	}

	// Base64-decode the payload (second part), adding padding if needed
	payload := parts[1]
	if m := len(payload) % 4; m != 0 {
		payload += strings.Repeat("=", 4-m)
	}
	decoded, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return fallback
	}

	var claims struct {
		Exp      int64  `json:"exp"`
		ClientID string `json:"clientID"`
	}
	if err := json.Unmarshal(decoded, &claims); err != nil || claims.Exp == 0 {
		return fallback
	}

	return time.Unix(claims.Exp, 0)
}

// SaveIndMoneyToken saves the IndMoney access token to the database
func (h *Handler) SaveIndMoneyToken(c *gin.Context) {
	var body struct {
		AccessToken string `json:"access_token"`
		UserID      string `json:"user_id"`
	}

	if err := c.ShouldBindJSON(&body); err != nil || body.AccessToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "Missing or invalid access_token"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try to extract expiry from JWT; fall back to next-day 7 AM IST
	ist, _ := time.LoadLocation("Asia/Kolkata")
	now := time.Now().In(ist)
	fallbackExpiry := time.Date(now.Year(), now.Month(), now.Day()+1, 7, 0, 0, 0, ist)
	expiresAt := parseJWTExpiry(body.AccessToken, fallbackExpiry)

	userID := body.UserID
	clientID := ""
	// Try to extract clientID from JWT for user_id
	parts := strings.Split(body.AccessToken, ".")
	if len(parts) == 3 {
		payload := parts[1]
		if m := len(payload) % 4; m != 0 {
			payload += strings.Repeat("=", 4-m)
		}
		if decoded, err := base64.URLEncoding.DecodeString(payload); err == nil {
			var claims struct {
				ClientID string `json:"clientID"`
			}
			if json.Unmarshal(decoded, &claims) == nil && claims.ClientID != "" {
				clientID = claims.ClientID
			}
		}
	}

	if userID == "" {
		if clientID != "" {
			userID = clientID
		} else {
			userID = "indmoney_user"
		}
	}

	if err := h.db.UpdateBrokerToken(ctx, "indmoney", body.AccessToken, userID, expiresAt); err != nil {
		log.Printf("Failed to store IndMoney token: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to store token"})
		return
	}

	log.Printf("✅ IndMoney access token saved for user %s (expires %s)", userID, expiresAt.Format(time.RFC3339))

	c.JSON(http.StatusOK, gin.H{
		"status":           "success",
		"user_id":          userID,
		"user_name":        userID,
		"token_expires_at": expiresAt.Format(time.RFC3339),
		"authenticated":    true,
	})
}

// GetIndMoneyAuthStatus returns the current IndMoney authentication status
func (h *Handler) GetIndMoneyAuthStatus(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	config, err := h.db.GetBrokerConfig(ctx, "indmoney")
	if err != nil {
		log.Printf("Failed to get IndMoney broker config: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to check auth status"})
		return
	}

	if config == nil || config.AccessToken == "" {
		c.JSON(http.StatusOK, gin.H{
			"status":        "not_authenticated",
			"authenticated": false,
		})
		return
	}

	isExpired := false
	if config.TokenExpiresAt != nil {
		isExpired = time.Now().After(*config.TokenExpiresAt)
	}

	status := "authenticated"
	if isExpired {
		status = "expired"
	}

	result := gin.H{
		"status":        status,
		"authenticated": config.Enabled && !isExpired,
		"user_id":       config.UserID,
		"enabled":       config.Enabled,
		"is_expired":    isExpired,
	}

	if config.TokenExpiresAt != nil {
		result["token_expires_at"] = config.TokenExpiresAt.Format(time.RFC3339)
	}
	if config.LastAuthenticatedAt != nil {
		result["authenticated_at"] = config.LastAuthenticatedAt.Format(time.RFC3339)
	}

	c.JSON(http.StatusOK, result)
}

// LogoutIndMoney logs out the user and invalidates the IndMoney token
func (h *Handler) LogoutIndMoney(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := h.db.ClearBrokerToken(ctx, "indmoney"); err != nil {
		log.Printf("Failed to clear IndMoney token: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"detail": "Failed to logout"})
		return
	}

	log.Println("✅ IndMoney token cleared")

	c.JSON(http.StatusOK, gin.H{
		"status":  "logged_out",
		"message": "IndMoney token cleared successfully",
	})
}
