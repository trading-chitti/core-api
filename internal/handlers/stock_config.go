package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/trading-chitti/core-api-go/internal/database"
)

// GetStockConfigs handles GET /api/stock-config/stocks
func (h *Handler) GetStockConfigs(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	f := database.StockConfigFilters{}
	f.Limit, _ = strconv.Atoi(c.DefaultQuery("limit", "50"))
	f.Offset, _ = strconv.Atoi(c.DefaultQuery("offset", "0"))
	f.Symbol = c.Query("symbol")
	f.Name = c.Query("name")
	f.Sector = c.Query("sector")
	f.Exchange = c.Query("exchange")
	f.MarketCapCategory = c.Query("market_cap_category")
	f.Fetcher = c.Query("fetcher")
	f.SelectionType = c.Query("selection_type")

	if v := c.Query("intraday_enabled"); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			f.IntradayEnabled = &b
		}
	}
	if v := c.Query("investment_enabled"); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			f.InvestmentEnabled = &b
		}
	}
	if v := c.Query("active"); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			f.Active = &b
		}
	}

	result, err := h.db.GetStockConfigs(ctx, f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get stock configs"})
		return
	}

	c.JSON(http.StatusOK, result)
}

// UpdateStockConfig handles PUT /api/stock-config/stocks/:symbol/:exchange
func (h *Handler) UpdateStockConfig(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	symbol := c.Param("symbol")
	exchange := c.Param("exchange")

	if symbol == "" || exchange == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Symbol and exchange are required"})
		return
	}

	var body map[string]interface{}
	if err := json.NewDecoder(c.Request.Body).Decode(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}

	// Map allowed fields
	updates := make(map[string]interface{})
	if v, ok := body["intraday_enabled"]; ok {
		updates["intraday_enabled"] = v
	}
	if v, ok := body["investment_enabled"]; ok {
		updates["investment_enabled"] = v
	}
	if v, ok := body["fetcher"]; ok {
		updates["fetcher"] = v
	}
	if v, ok := body["active"]; ok {
		updates["active"] = v
	}

	if len(updates) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No valid fields to update"})
		return
	}

	if err := h.db.UpdateStockConfig(ctx, symbol, exchange, updates); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Stock config updated", "symbol": symbol, "exchange": exchange})
}

// GetStockConfigStats handles GET /api/stock-config/stats
func (h *Handler) GetStockConfigStats(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stats, err := h.db.GetStockConfigStats(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get stats"})
		return
	}

	c.JSON(http.StatusOK, stats)
}

// ExportStockConfigsCSV handles GET /api/stock-config/export-csv
func (h *Handler) ExportStockConfigsCSV(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	csv, err := h.db.ExportStockConfigsCSV(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to export CSV"})
		return
	}

	c.Header("Content-Type", "text/csv")
	c.Header("Content-Disposition", "attachment; filename=stock_config.csv")
	c.String(http.StatusOK, csv)
}

// ImportStockConfigsCSV handles POST /api/stock-config/import-csv
func (h *Handler) ImportStockConfigsCSV(c *gin.Context) {
	// For now, return a stub response since CSV import requires file handling
	c.JSON(http.StatusOK, gin.H{
		"job_id":     "pending",
		"message":    "CSV import is not yet implemented in Go API",
		"total_rows": 0,
	})
}

// GetImportJobStatus handles GET /api/stock-config/import-jobs/:jobId
func (h *Handler) GetImportJobStatus(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID := c.Param("jobId")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Job ID is required"})
		return
	}

	status, err := h.db.GetImportJobStatus(ctx, jobID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Import job not found"})
		return
	}

	c.JSON(http.StatusOK, status)
}
