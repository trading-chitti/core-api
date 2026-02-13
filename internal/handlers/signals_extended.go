package handlers

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// GetDashboardData handles GET /api/signals/dashboard
func (h *Handler) GetDashboardData(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
	includeClosed := c.DefaultQuery("include_closed", "false") == "true"

	data, err := h.db.GetDashboardData(ctx, limit, includeClosed)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get dashboard data"})
		return
	}

	c.JSON(http.StatusOK, data)
}

// GetInvestmentSignals handles GET /api/signals/investment-signals
func (h *Handler) GetInvestmentSignals(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	minConfidence, _ := strconv.ParseFloat(c.DefaultQuery("min_confidence", "0.5"), 64)
	minSuccessRate, _ := strconv.ParseFloat(c.DefaultQuery("min_success_rate", "0"), 64)
	requireSentiment := c.DefaultQuery("require_news_sentiment", "false") == "true"

	data, err := h.db.GetInvestmentSignals(ctx, minConfidence, minSuccessRate, requireSentiment)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get investment signals"})
		return
	}

	c.JSON(http.StatusOK, data)
}

// GetSignalAlerts handles GET /api/signals/alerts
func (h *Handler) GetSignalAlerts(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	strategy := c.Query("strategy")
	minConfidence, _ := strconv.ParseFloat(c.DefaultQuery("minConfidence", "0.3"), 64)

	alerts, err := h.db.GetSignalAlerts(ctx, strategy, minConfidence)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get signal alerts"})
		return
	}

	c.JSON(http.StatusOK, alerts)
}

// GetPredictedGainers handles GET /api/predictions/top-gainers
func (h *Handler) GetPredictedGainers(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
	if limit <= 0 || limit > 50 {
		limit = 10
	}

	gainers, err := h.db.GetPredictedGainers(ctx, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get predicted gainers"})
		return
	}

	c.JSON(http.StatusOK, gainers)
}

// GetPredictedLosers handles GET /api/predictions/top-losers
func (h *Handler) GetPredictedLosers(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))
	if limit <= 0 || limit > 50 {
		limit = 10
	}

	losers, err := h.db.GetPredictedLosers(ctx, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get predicted losers"})
		return
	}

	c.JSON(http.StatusOK, losers)
}
