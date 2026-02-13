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
			log.Printf("⚠️  NATS subscription failed, continuing without events: %v", err)
		}
	}

	// Create HTTP handlers
	handler := handlers.NewHandler(db, hub)
	monitoringHandler := handlers.NewMonitoringHandler(db.GetConn())
	quantHandler := handlers.NewQuantAnalyticsHandler(db.GetConn())
	systemHandler := handlers.NewSystemHandler(db.GetConn())

	// Setup Gin router
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Logger())
	router.Use(gin.Recovery())
	router.Use(handlers.CORSMiddleware())

	// API routes
	api := router.Group("/api")
	{
		// Portfolio endpoints
		api.GET("/portfolio/stats", handler.GetPortfolioStats)

		// Stock endpoints
		stocksGroup := api.Group("/stocks")
		{
			stocksGroup.GET("/top-gainers", handler.GetTopGainers)
			stocksGroup.GET("/top-losers", handler.GetTopLosers)
			stocksGroup.GET("/realtime/all", handler.GetRealtimePrices)
			stocksGroup.GET("/search", handler.SearchStocks)
			stocksGroup.GET("/:symbol/realtime", handler.GetRealtimePrice)
			stocksGroup.GET("/:symbol", handler.GetStockData)
		}

		// News endpoints
		api.GET("/news", handler.GetNews)

		// Signals endpoints
		signalsGroup := api.Group("/signals")
		{
			signalsGroup.GET("", handler.GetSignals)
			signalsGroup.GET("/active", handler.GetActiveSignals)
			signalsGroup.GET("/alerts", handler.GetSignalAlerts)
			signalsGroup.GET("/investment-signals", handler.GetInvestmentSignals)
			signalsGroup.GET("/dashboard", handler.GetDashboardData)
			signalsGroup.GET("/:id", handler.GetSignalByID)
		}

		// Predictions endpoints
		predictionsGroup := api.Group("/predictions")
		{
			predictionsGroup.GET("/top-gainers", handler.GetPredictedGainers)
			predictionsGroup.GET("/top-losers", handler.GetPredictedLosers)
		}

		// Market data endpoints
		marketGroup := api.Group("/market")
		{
			marketGroup.GET("/indices", handler.GetMarketIndices)
		}

		// Watchlist endpoints
		watchlistGroup := api.Group("/watchlist")
		{
			watchlistGroup.GET("", handler.GetWatchlist)
			watchlistGroup.POST("", handler.AddToWatchlist)
			watchlistGroup.DELETE("/:symbol", handler.RemoveFromWatchlist)
		}

		// Stock configuration endpoints
		stockConfigGroup := api.Group("/stock-config")
		{
			stockConfigGroup.GET("/stocks", handler.GetStockConfigs)
			stockConfigGroup.PUT("/stocks/:symbol/:exchange", handler.UpdateStockConfig)
			stockConfigGroup.GET("/stats", handler.GetStockConfigStats)
			stockConfigGroup.GET("/export-csv", handler.ExportStockConfigsCSV)
			stockConfigGroup.POST("/import-csv", handler.ImportStockConfigsCSV)
			stockConfigGroup.GET("/import-jobs/:jobId", handler.GetImportJobStatus)
		}

		// System configuration endpoints
		configGroup := api.Group("/config")
		{
			configGroup.GET("/smart-selection", handler.GetSmartSelection)
			configGroup.PUT("/smart-selection", handler.UpdateSmartSelection)
			configGroup.GET("/stock-counts", handler.GetStockCounts)
			configGroup.PUT("/smart-selection/stock-count", handler.UpdateSmartSelectionStockCount)
		}

		// Monitor endpoints (dashboard compatibility)
		monitorGroup := api.Group("/monitor")
		{
			monitorGroup.GET("/services", handler.GetMonitorServices)
			monitorGroup.GET("/services/:service", handler.GetMonitorService)
		}

		// Monitoring endpoints (detailed)
		monitoringGroup := api.Group("/monitoring")
		{
			monitoringGroup.GET("/services/health", monitoringHandler.GetServicesHealth)
			monitoringGroup.GET("/metrics", monitoringHandler.GetSystemMetrics)
			monitoringGroup.GET("/metrics/request-rate", monitoringHandler.GetRequestRate)
			monitoringGroup.GET("/metrics/response-time", monitoringHandler.GetResponseTime)
			monitoringGroup.GET("/metrics/error-rate", monitoringHandler.GetErrorRate)
			monitoringGroup.GET("/system/resources", monitoringHandler.GetSystemResources)
			monitoringGroup.GET("/logs/recent", monitoringHandler.GetRecentLogs)
			monitoringGroup.GET("/logs/errors", monitoringHandler.GetErrorLogs)
			monitoringGroup.GET("/broker-status", monitoringHandler.GetBrokerStatus)
		}

		// Quantitative Analytics endpoints
		quantGroup := api.Group("/quant")
		{
			quantGroup.GET("/analytics", quantHandler.GetQuantAnalytics)
		}

		// System monitoring endpoints
		systemGroup := api.Group("/system")
		{
			systemGroup.GET("/services", systemHandler.GetServices)
			systemGroup.GET("/jobs", systemHandler.GetJobs)
			systemGroup.POST("/jobs/:jobName/run", systemHandler.RunJobManually)
			systemGroup.GET("/ml-models", systemHandler.GetMLModels)
		}

		// Authentication endpoints
		authGroup := api.Group("/auth")
		{
			zerodhaGroup := authGroup.Group("/zerodha")
			{
				zerodhaGroup.GET("/login-url", handler.GetZerodhaLoginUrl)
				zerodhaGroup.POST("/request-token", handler.ExchangeRequestToken)
				zerodhaGroup.POST("/token", handler.SaveAccessToken)
				zerodhaGroup.GET("/status", handler.GetZerodhaAuthStatus)
				zerodhaGroup.DELETE("/logout/:user_id", handler.LogoutZerodha)
				zerodhaGroup.POST("/logout/:user_id", handler.LogoutZerodha)
			}

			indmoneyGroup := authGroup.Group("/indmoney")
			{
				indmoneyGroup.POST("/token", handler.SaveIndMoneyToken)
				indmoneyGroup.GET("/status", handler.GetIndMoneyAuthStatus)
				indmoneyGroup.DELETE("/logout", handler.LogoutIndMoney)
				indmoneyGroup.POST("/logout", handler.LogoutIndMoney)
			}
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
			"version":     "2.0.0",
			"description": "Full-featured API with real-time WebSocket streaming",
			"endpoints":   59,
			"health":      "/health",
			"websocket":   "/ws",
		})
	})

	// Get port from environment
	port := os.Getenv("PORT")
	if port == "" {
		port = "6001"
	}

	log.Printf("✅ Core API Go listening on port %s (59 endpoints)", port)

	// Start server in goroutine
	go func() {
		if err := router.Run(":" + port); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Core API Go...")
}
