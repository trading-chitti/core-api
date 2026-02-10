package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/trading-chitti/core-api-go/internal/database"
	"github.com/trading-chitti/core-api-go/internal/events"
	"github.com/trading-chitti/core-api-go/internal/handlers"
	"github.com/trading-chitti/core-api-go/internal/websocket"
)

func main() {
	log.Println("🚀 Starting Core API Go service...")

	// Get database DSN from environment
	dsn := os.Getenv("TRADING_CHITTI_PG_DSN")
	if dsn == "" {
		dsn = "postgresql://hariprasath@localhost:6432/trading_chitti?sslmode=disable"
	}

	// Connect to database
	db, err := database.NewDB(dsn)
	if err != nil {
		log.Fatalf("❌ Database connection failed: %v", err)
	}
	defer db.Close()

	// Create WebSocket hub
	hub := websocket.NewHub()
	go hub.Run()
	log.Println("✅ WebSocket hub started")

	// Connect to NATS and subscribe to events
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	subscriber, err := events.NewSubscriber(natsURL, hub)
	if err != nil {
		log.Printf("⚠️  NATS connection failed, events disabled: %v", err)
	} else {
		defer subscriber.Close()
		if err := subscriber.Subscribe(); err != nil {
			log.Fatalf("❌ Failed to subscribe to NATS: %v", err)
		}
	}

	// Create HTTP handlers
	handler := handlers.NewHandler(db, hub)
	monitoringHandler := handlers.NewMonitoringHandler(db.GetConn())

	// Setup Gin router
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Logger())
	router.Use(gin.Recovery())
	router.Use(handlers.CORSMiddleware())

	// API routes
	api := router.Group("/api")
	{
		// Signals endpoints
		api.GET("/signals", handler.GetSignals)
		api.GET("/signals/active", handler.GetActiveSignals)
		api.GET("/signals/:id", handler.GetSignalByID)

		// Monitoring endpoints
		monitoring := api.Group("/monitoring")
		{
			monitoring.GET("/services/health", monitoringHandler.GetServicesHealth)
			monitoring.GET("/metrics", monitoringHandler.GetSystemMetrics)
		}
	}

	// WebSocket endpoint
	router.GET("/ws", handler.ServeWebSocket)

	// Health endpoint
	router.GET("/health", handler.Health)

	// Root endpoint
	router.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"name":        "Trading-Chitti Core API (Go)",
			"version":     "1.0.0",
			"description": "High-performance API with real-time WebSocket streaming",
			"docs":        "/docs",
			"health":      "/health",
			"websocket":   "/ws",
		})
	})

	// Get port from environment (default to 6001 - same as Python core-api)
	port := os.Getenv("PORT")
	if port == "" {
		port = "6001"
	}

	log.Printf("✅ Core API Go listening on port %s", port)
	log.Println("📡 WebSocket endpoint: ws://localhost:" + port + "/ws")
	log.Println("🔌 REST API: http://localhost:" + port + "/api/signals")

	// Start server in goroutine
	go func() {
		if err := router.Run(":" + port); err != nil {
			log.Fatalf("❌ Server failed: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("👋 Shutting down Core API Go...")
}
