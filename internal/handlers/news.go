package handlers

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// GetNews handles GET /api/news
func (h *Handler) GetNews(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	sentiment := c.Query("sentiment")
	search := c.Query("search")
	symbol := c.Query("symbol")

	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	news, err := h.db.GetNews(ctx, limit, offset, sentiment, search, symbol)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get news"})
		return
	}

	c.JSON(http.StatusOK, news)
}
