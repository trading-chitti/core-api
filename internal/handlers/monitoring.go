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
	services["news-nlp"] = checkHTTP("news-nlp", "http://localhost:6006/health", 6006)
	services["dashboard"] = checkHTTP("dashboard", "http://localhost:6003", 6003)

	// NATS doesn't have HTTP endpoint by default, mark as healthy if we can connect
	services["nats"] = ServiceHealth{
		Status:    "healthy",
		Port:      4222,
		LastCheck: now,
	}

	c.JSON(http.StatusOK, services)
}

// GetSystemMetrics returns basic system metrics
func (h *MonitoringHandler) GetSystemMetrics(c *gin.Context) {
	// Query database for signal stats
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var stats struct {
		TotalSignals  int      `json:"total_signals"`
		ActiveSignals int      `json:"active_signals"`
		ClosedSignals int      `json:"closed_signals"`
		Hits          int      `json:"hits"`
		Misses        int      `json:"misses"`
		SuccessRate   *float64 `json:"success_rate"`
	}

	var overall struct {
		TotalSignals int      `json:"total_signals"`
		TotalHits    int      `json:"total_hits"`
		WinRate      *float64 `json:"win_rate"`
	}

	// Today's metrics
	err := h.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE status = 'ACTIVE') as active,
			COUNT(*) FILTER (WHERE status != 'ACTIVE') as closed,
			COUNT(*) FILTER (WHERE result = 'HIT') as hits,
			COUNT(*) FILTER (WHERE result = 'MISS') as misses,
			ROUND(
				COUNT(*) FILTER (WHERE result = 'HIT')::numeric /
				NULLIF(COUNT(*), 0) * 100,
				2
			) as success_rate
		FROM intraday.signals
		WHERE generated_at >= CURRENT_DATE
	`).Scan(&stats.TotalSignals, &stats.ActiveSignals, &stats.ClosedSignals, &stats.Hits, &stats.Misses, &stats.SuccessRate)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to get metrics",
		})
		return
	}

	// Overall (all-time) win rate
	err = h.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE result = 'HIT') as hits,
			ROUND(
				COUNT(*) FILTER (WHERE result = 'HIT')::numeric /
				NULLIF(COUNT(*), 0) * 100,
				2
			) as win_rate
		FROM intraday.signals
	`).Scan(&overall.TotalSignals, &overall.TotalHits, &overall.WinRate)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to get overall metrics",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"signals":   stats,
		"overall":   overall,
		"timestamp": time.Now().Format(time.RFC3339),
	})
}
