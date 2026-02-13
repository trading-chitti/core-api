package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// GetPortfolioStats handles GET /api/portfolio/stats
func (h *Handler) GetPortfolioStats(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stats, err := h.db.GetPortfolioStats(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get portfolio stats"})
		return
	}

	c.JSON(http.StatusOK, stats)
}
