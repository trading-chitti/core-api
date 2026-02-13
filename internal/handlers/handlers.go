package handlers

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/trading-chitti/core-api-go/internal/database"
	ws "github.com/trading-chitti/core-api-go/internal/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Allow all origins (in production, restrict this)
		return true
	},
}

// Handler contains all HTTP handlers
type Handler struct {
	db  *database.DB
	hub *ws.Hub
}

// NewHandler creates a new handler
func NewHandler(db *database.DB, hub *ws.Hub) *Handler {
	return &Handler{db: db, hub: hub}
}

// GetSignals handles GET /api/signals
func (h *Handler) GetSignals(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Parse query parameters
	limitStr := c.DefaultQuery("limit", "100")
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		limit = 100
	}

	status := c.Query("status") // Optional: "ACTIVE", "HIT_TARGET", etc.

	// Query database
	signals, err := h.db.GetAllSignals(ctx, limit, status)
	if err != nil {
		log.Printf("❌ Failed to get signals: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to retrieve signals",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"signals": signals,
		"count":   len(signals),
	})
}

// GetActiveSignals handles GET /api/signals/active
func (h *Handler) GetActiveSignals(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	signals, err := h.db.GetActiveSignals(ctx)
	if err != nil {
		log.Printf("❌ Failed to get active signals: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to retrieve active signals",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"signals": signals,
		"count":   len(signals),
	})
}

// GetSignalByID handles GET /api/signals/:id
func (h *Handler) GetSignalByID(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	signalID := c.Param("id")
	if signalID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid signal ID",
		})
		return
	}

	signal, err := h.db.GetSignalByID(ctx, signalID)
	if err != nil {
		log.Printf("❌ Failed to get signal %s: %v", signalID, err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to retrieve signal",
		})
		return
	}

	if signal == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "Signal not found",
		})
		return
	}

	c.JSON(http.StatusOK, signal)
}

// Health handles GET /health
func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":            "healthy",
		"service":           "core-api-go",
		"timestamp":         time.Now().UTC().Format(time.RFC3339),
		"websocket_clients": h.hub.ClientCount(),
	})
}

// ServeWebSocket handles WebSocket connections
func (h *Handler) ServeWebSocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("❌ WebSocket upgrade failed: %v", err)
		return
	}

	client := ws.NewClient(h.hub, conn)
	h.hub.Register(client)

	// Start client goroutines
	go client.WritePump()
	go client.ReadPump()
}

// CORS middleware for development
func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")
		if origin != "" {
			c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
			c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		} else {
			c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		}
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}
