package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// GetMarketIndices handles GET /api/market/indices
func (h *Handler) GetMarketIndices(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	indices, err := h.db.GetMarketIndices(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get market indices"})
		return
	}

	c.JSON(http.StatusOK, indices)
}
