package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os/exec"
	"time"

	"github.com/gin-gonic/gin"
)

// GetSmartSelection handles GET /api/config/smart-selection
func (h *Handler) GetSmartSelection(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var configValue sql.NullString
	err := h.db.GetConn().QueryRowContext(ctx,
		"SELECT config_value FROM md.system_config WHERE config_key = 'smart_stock_selection_enabled'",
	).Scan(&configValue)

	enabled := false
	if err == nil && configValue.Valid {
		enabled = configValue.String == "true"
	}

	// Get stock count setting
	var stockCount sql.NullString
	err = h.db.GetConn().QueryRowContext(ctx,
		"SELECT config_value FROM md.system_config WHERE config_key = 'smart_selection_stock_count'",
	).Scan(&stockCount)

	count := 200
	if err == nil && stockCount.Valid {
		json.Unmarshal([]byte(stockCount.String), &count)
	}

	c.JSON(http.StatusOK, gin.H{
		"enabled":     enabled,
		"stock_count": count,
		"timestamp":   time.Now().Format(time.RFC3339),
	})
}

// UpdateSmartSelection handles PUT /api/config/smart-selection
func (h *Handler) UpdateSmartSelection(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var body struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(c.Request.Body).Decode(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}

	value := "false"
	if body.Enabled {
		value = "true"
	}

	_, err := h.db.GetConn().ExecContext(ctx,
		`INSERT INTO md.system_config (config_key, config_value, description, updated_by)
		VALUES ('smart_stock_selection_enabled', $1, 'Enable ML-based stock selection', 'api')
		ON CONFLICT (config_key) DO UPDATE SET config_value = $1, updated_at = NOW()`,
		value,
	)
	if err != nil {
		log.Printf("Failed to update smart selection: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update config"})
		return
	}

	// Trigger ML stock selection if enabling Smart Mode
	if body.Enabled {
		log.Println("✓ Smart selection enabled - triggering ML stock selection...")
		go triggerMLStockSelection()
	} else {
		log.Println("✓ Smart selection disabled - clearing AI selections...")
		go clearMLSelections(h.db)
	}

	c.JSON(http.StatusOK, gin.H{"enabled": body.Enabled, "message": "Smart selection updated"})
}

// GetStockCounts handles GET /api/config/stock-counts
func (h *Handler) GetStockCounts(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var totalEnabled, zerodhaCount, indmoneyCount, mlSelected, wildcardCount, manualCount int

	h.db.GetConn().QueryRowContext(ctx, `
		SELECT
			COUNT(*) as total_enabled,
			COUNT(*) FILTER (WHERE fetcher = 'ZERODHA') as zerodha_count,
			COUNT(*) FILTER (WHERE fetcher = 'INDMONEY') as indmoney_count,
			COUNT(*) FILTER (WHERE selection_type = 'MORNING_ML') as ml_selected,
			COUNT(*) FILTER (WHERE selection_type = 'WILDCARD_NEWS') as wildcard_count,
			COUNT(*) FILTER (WHERE selection_type IS NULL OR selection_type = '') as manual_count
		FROM md.stock_config
		WHERE active = true
	`).Scan(&totalEnabled, &zerodhaCount, &indmoneyCount, &mlSelected, &wildcardCount, &manualCount)

	c.JSON(http.StatusOK, gin.H{
		"total_enabled":  totalEnabled,
		"zerodha_count":  zerodhaCount,
		"indmoney_count": indmoneyCount,
		"ml_selected":    mlSelected,
		"wildcard_count": wildcardCount,
		"manual_count":   manualCount,
	})
}

// UpdateSmartSelectionStockCount handles PUT /api/config/smart-selection/stock-count
func (h *Handler) UpdateSmartSelectionStockCount(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var body struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(c.Request.Body).Decode(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}

	if body.Count < 10 || body.Count > 2000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Stock count must be between 10 and 2000"})
		return
	}

	countStr, _ := json.Marshal(body.Count)
	_, err := h.db.GetConn().ExecContext(ctx,
		`INSERT INTO md.system_config (config_key, config_value, description, updated_by)
		VALUES ('smart_selection_stock_count', $1, 'Number of stocks to select in Smart Mode (split equally between fetchers)', 'api')
		ON CONFLICT (config_key) DO UPDATE SET config_value = $1, updated_at = NOW()`,
		string(countStr),
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update stock count"})
		return
	}

	// Trigger ML stock selection with new count
	log.Printf("✓ Stock count updated to %d - triggering ML stock selection...", body.Count)
	go triggerMLStockSelection()

	c.JSON(http.StatusOK, gin.H{"count": body.Count, "message": "Stock count updated"})
}

// triggerMLStockSelection runs the ML stock selection Python script
func triggerMLStockSelection() {
	cmd := exec.Command("/opt/homebrew/bin/python3", "/Users/hariprasath/trading-chitti/scripts/select_daily_stocks.py")
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("❌ Failed to run ML stock selection: %v\nOutput: %s", err, string(output))
	} else {
		log.Printf("✅ ML stock selection completed successfully\nOutput: %s", string(output))
	}
}

// clearMLSelections clears all AI selections when Smart Mode is disabled
func clearMLSelections(db interface{ GetConn() *sql.DB }) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := db.GetConn().ExecContext(ctx, `
		UPDATE md.stock_config
		SET intraday_ai_picked = FALSE,
			selection_type = NULL
		WHERE intraday_ai_picked = TRUE
	`)
	if err != nil {
		log.Printf("❌ Failed to clear ML selections: %v", err)
	} else {
		log.Println("✅ ML selections cleared")
	}
}
