package handlers

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// GetTopGainers handles GET /api/stocks/top-gainers
func (h *Handler) GetTopGainers(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	gainers, err := h.db.GetTopGainers(ctx, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get top gainers"})
		return
	}

	c.JSON(http.StatusOK, gainers)
}

// GetTopLosers handles GET /api/stocks/top-losers
func (h *Handler) GetTopLosers(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	losers, err := h.db.GetTopLosers(ctx, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get top losers"})
		return
	}

	c.JSON(http.StatusOK, losers)
}

// GetRealtimePrices handles GET /api/stocks/realtime/all
func (h *Handler) GetRealtimePrices(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	prices, err := h.db.GetRealtimePrices(ctx, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get realtime prices"})
		return
	}

	c.JSON(http.StatusOK, prices)
}

// GetRealtimePrice handles GET /api/stocks/:symbol/realtime
func (h *Handler) GetRealtimePrice(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	symbol := c.Param("symbol")
	if symbol == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Symbol is required"})
		return
	}

	price, err := h.db.GetRealtimePrice(ctx, symbol)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Price not found for symbol"})
		return
	}

	c.JSON(http.StatusOK, price)
}

// GetStockData handles GET /api/stocks/:symbol
func (h *Handler) GetStockData(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	symbol := c.Param("symbol")
	if symbol == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Symbol is required"})
		return
	}

	stock, err := h.db.GetStockData(ctx, symbol)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Stock not found"})
		return
	}

	c.JSON(http.StatusOK, stock)
}

// SearchStocks handles GET /api/stocks/search
func (h *Handler) SearchStocks(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := c.Query("q")
	if query == "" {
		c.JSON(http.StatusOK, []interface{}{})
		return
	}

	results, err := h.db.SearchStocks(ctx, query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Search failed"})
		return
	}

	c.JSON(http.StatusOK, results)
}
