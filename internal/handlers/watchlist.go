package handlers

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

// Thread-safe in-memory watchlist
var (
	watchlistStore = map[string]bool{}
	watchlistMu    sync.RWMutex
)

// GetWatchlist handles GET /api/watchlist
func (h *Handler) GetWatchlist(c *gin.Context) {
	watchlistMu.RLock()
	watchlist := []map[string]interface{}{}
	for symbol := range watchlistStore {
		watchlist = append(watchlist, map[string]interface{}{
			"symbol":        symbol,
			"name":          symbol,
			"price":         0,
			"change":        0,
			"changePercent": 0,
		})
	}
	watchlistMu.RUnlock()
	c.JSON(http.StatusOK, watchlist)
}

// AddToWatchlist handles POST /api/watchlist
func (h *Handler) AddToWatchlist(c *gin.Context) {
	var body struct {
		Symbol string `json:"symbol"`
	}
	if err := json.NewDecoder(c.Request.Body).Decode(&body); err != nil || body.Symbol == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Symbol is required"})
		return
	}

	watchlistMu.Lock()
	watchlistStore[body.Symbol] = true
	watchlistMu.Unlock()
	c.JSON(http.StatusOK, gin.H{"message": "Added to watchlist", "symbol": body.Symbol})
}

// RemoveFromWatchlist handles DELETE /api/watchlist/:symbol
func (h *Handler) RemoveFromWatchlist(c *gin.Context) {
	symbol := c.Param("symbol")
	if symbol == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Symbol is required"})
		return
	}

	watchlistMu.Lock()
	delete(watchlistStore, symbol)
	watchlistMu.Unlock()
	c.JSON(http.StatusOK, gin.H{"message": "Removed from watchlist", "symbol": symbol})
}
