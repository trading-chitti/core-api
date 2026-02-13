package handlers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// ServiceInfo represents a service's status info
type ServiceInfo struct {
	Name            string  `json:"name"`
	Status          string  `json:"status"`
	Uptime          float64 `json:"uptime"`
	RequestCount    int     `json:"requestCount"`
	ErrorRate       float64 `json:"errorRate"`
	AvgResponseTime float64 `json:"avgResponseTime"`
	LastCheck       string  `json:"lastCheck"`
}

// serviceEndpoint defines how to health-check a service
type serviceEndpoint struct {
	Name string
	URL  string
}

var serviceEndpoints = []serviceEndpoint{
	{Name: "intraday-engine", URL: "http://localhost:6007/health"},
	{Name: "market-bridge", URL: "http://localhost:6005/health"},
	{Name: "news-nlp", URL: "http://localhost:6006/health"},
	// NATS doesn't have HTTP endpoint - marked as healthy in GetMonitorServices
	{Name: "dashboard", URL: "http://localhost:6003"},
}

// checkServiceHTTP performs an HTTP health check for a service
func checkServiceHTTP(ctx context.Context, ep serviceEndpoint, now string) ServiceInfo {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, "GET", ep.URL, nil)
	if err != nil {
		return ServiceInfo{Name: ep.Name, Status: "unhealthy", LastCheck: now}
	}

	resp, err := http.DefaultClient.Do(req)
	responseTimeMs := float64(time.Since(start).Milliseconds())

	if err != nil {
		return ServiceInfo{Name: ep.Name, Status: "unhealthy", AvgResponseTime: responseTimeMs, LastCheck: now}
	}
	defer resp.Body.Close()

	status := "unhealthy"
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		status = "healthy"
	} else if resp.StatusCode == 503 {
		status = "degraded"
	}

	return ServiceInfo{Name: ep.Name, Status: status, Uptime: 99.9, AvgResponseTime: responseTimeMs, LastCheck: now}
}

// GetMonitorServices handles GET /api/monitor/services
func (h *Handler) GetMonitorServices(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	now := time.Now().Format(time.RFC3339)

	services := []ServiceInfo{
		{Name: "core-api-go", Status: "healthy", Uptime: 99.9, AvgResponseTime: 1, LastCheck: now},
	}

	// Check database
	dbStatus := "healthy"
	dbStart := time.Now()
	if err := h.db.GetConn().PingContext(ctx); err != nil {
		dbStatus = "unhealthy"
	}
	services = append(services, ServiceInfo{
		Name: "postgres", Status: dbStatus, Uptime: 99.9,
		AvgResponseTime: float64(time.Since(dbStart).Milliseconds()), LastCheck: now,
	})

	// Check all external services via HTTP
	for _, ep := range serviceEndpoints {
		services = append(services, checkServiceHTTP(ctx, ep, now))
	}

	// NATS doesn't have HTTP endpoint, mark as healthy manually
	services = append(services, ServiceInfo{
		Name: "nats", Status: "healthy", Uptime: 99.9, AvgResponseTime: 0, LastCheck: now,
	})

	c.JSON(http.StatusOK, gin.H{"services": services})
}

// GetMonitorService handles GET /api/monitor/services/:service
func (h *Handler) GetMonitorService(c *gin.Context) {
	service := c.Param("service")
	now := time.Now().Format(time.RFC3339)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if service == "core-api-go" || service == "core-api" {
		c.JSON(http.StatusOK, gin.H{"services": []ServiceInfo{
			{Name: "core-api-go", Status: "healthy", Uptime: 99.9, AvgResponseTime: 1, LastCheck: now},
		}})
		return
	}

	if service == "postgres" || service == "database" {
		dbStatus := "healthy"
		dbStart := time.Now()
		if err := h.db.GetConn().PingContext(ctx); err != nil {
			dbStatus = "unhealthy"
		}
		c.JSON(http.StatusOK, gin.H{"services": []ServiceInfo{
			{Name: "postgres", Status: dbStatus, AvgResponseTime: float64(time.Since(dbStart).Milliseconds()), LastCheck: now},
		}})
		return
	}

	for _, ep := range serviceEndpoints {
		if ep.Name == service {
			c.JSON(http.StatusOK, gin.H{"services": []ServiceInfo{checkServiceHTTP(ctx, ep, now)}})
			return
		}
	}

	c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Unknown service: %s", service)})
}
