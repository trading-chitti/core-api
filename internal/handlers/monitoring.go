package handlers

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// MonitoringHandler handles monitoring endpoints
type MonitoringHandler struct {
	db *sql.DB
}

// NewMonitoringHandler creates a new monitoring handler
func NewMonitoringHandler(db *sql.DB) *MonitoringHandler {
	return &MonitoringHandler{db: db}
}

// ServiceHealth represents health status of a service
type ServiceHealth struct {
	Status          string  `json:"status"`
	ResponseTimeMs  float64 `json:"response_time_ms,omitempty"`
	Port            int     `json:"port,omitempty"`
	LastCheck       string  `json:"last_check"`
	Error           string  `json:"error,omitempty"`
}

// GetServicesHealth returns health status of all services
func (h *MonitoringHandler) GetServicesHealth(c *gin.Context) {
	services := map[string]ServiceHealth{}
	now := time.Now().Format(time.RFC3339)

	// Check database
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	if err := h.db.PingContext(ctx); err != nil {
		services["postgres"] = ServiceHealth{
			Status:    "unhealthy",
			Port:      6432,
			LastCheck: now,
			Error:     err.Error(),
		}
	} else {
		services["postgres"] = ServiceHealth{
			Status:         "healthy",
			Port:           6432,
			LastCheck:      now,
			ResponseTimeMs: float64(time.Since(start).Milliseconds()),
		}
	}

	// Check other services via HTTP
	checkHTTP := func(name, url string, port int) ServiceHealth {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		resp, err := http.DefaultClient.Do(req)

		if err != nil {
			return ServiceHealth{
				Status:    "unhealthy",
				Port:      port,
				LastCheck: now,
				Error:     err.Error(),
			}
		}
		defer resp.Body.Close()

		status := "unhealthy"
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			status = "healthy"
		} else if resp.StatusCode == 503 {
			status = "degraded"
		}

		return ServiceHealth{
			Status:         status,
			Port:           port,
			LastCheck:      now,
			ResponseTimeMs: float64(time.Since(start).Milliseconds()),
		}
	}

	// Check critical services
	services["core-api-go"] = ServiceHealth{
		Status:    "healthy",
		Port:      6001,
		LastCheck: now,
	}

	services["intraday-engine"] = checkHTTP("intraday-engine", "http://localhost:6007/health", 6007)
	services["market-bridge"] = checkHTTP("market-bridge", "http://localhost:6005/health", 6005)
	services["dashboard"] = checkHTTP("dashboard", "http://localhost:6003", 6003)

	// Check NATS
	natsHealth := checkHTTP("nats", "http://localhost:8222/varz", 4222)
	services["nats"] = natsHealth

	c.JSON(http.StatusOK, services)
}

// GetSystemMetrics returns basic system metrics
func (h *MonitoringHandler) GetSystemMetrics(c *gin.Context) {
	// Query database for signal stats
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var stats struct {
		TotalSignals int     `json:"total_signals"`
		ActiveSignals int    `json:"active_signals"`
		SuccessRate  *float64 `json:"success_rate"`
	}

	err := h.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE status = 'ACTIVE') as active,
			ROUND(
				COUNT(*) FILTER (WHERE status = 'HIT_TARGET')::numeric /
				NULLIF(COUNT(*) FILTER (WHERE status IN ('HIT_TARGET', 'HIT_STOPLOSS', 'TRAILING_STOP')), 0) * 100,
				2
			) as success_rate
		FROM intraday.signals
		WHERE generated_at >= CURRENT_DATE
	`).Scan(&stats.TotalSignals, &stats.ActiveSignals, &stats.SuccessRate)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to get metrics",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"signals": stats,
		"timestamp": time.Now().Format(time.RFC3339),
	})
}
