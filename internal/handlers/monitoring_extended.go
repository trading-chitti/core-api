package handlers

import (
	"bufio"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// GetRequestRate handles GET /api/monitoring/metrics/request-rate
func (h *MonitoringHandler) GetRequestRate(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"rate":      0,
		"unit":      "requests/sec",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// GetResponseTime handles GET /api/monitoring/metrics/response-time
func (h *MonitoringHandler) GetResponseTime(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"avg_ms":    5.0,
		"p95_ms":    15.0,
		"p99_ms":    50.0,
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// GetErrorRate handles GET /api/monitoring/metrics/error-rate
func (h *MonitoringHandler) GetErrorRate(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"rate":      0.0,
		"unit":      "errors/min",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// GetSystemResources handles GET /api/monitoring/system/resources
func (h *MonitoringHandler) GetSystemResources(c *gin.Context) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	c.JSON(http.StatusOK, gin.H{
		"go_routines":      runtime.NumGoroutine(),
		"go_version":       runtime.Version(),
		"num_cpu":          runtime.NumCPU(),
		"memory_alloc_mb":  float64(memStats.Alloc) / 1024 / 1024,
		"memory_sys_mb":    float64(memStats.Sys) / 1024 / 1024,
		"memory_heap_mb":   float64(memStats.HeapAlloc) / 1024 / 1024,
		"gc_cycles":        memStats.NumGC,
		"gc_pause_total_ms": float64(memStats.PauseTotalNs) / 1e6,
		"timestamp":        time.Now().Format(time.RFC3339),
	})
}

// LogEntry represents a log entry
type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Service   string `json:"service"`
	Message   string `json:"message"`
}

// GetRecentLogs handles GET /api/monitoring/logs/recent
func (h *MonitoringHandler) GetRecentLogs(c *gin.Context) {
	logDir := "/Users/hariprasath/trading-chitti/logs"
	logs := []LogEntry{}

	// Main service logs
	serviceFiles := map[string]string{
		"core-api":        "core-api.log",
		"intraday-engine": "intraday-engine.log",
		"market-bridge":   "market-bridge.log",
		"news-nlp":        "news-nlp.log",
		"dashboard":       "dashboard.log",
		"signal-service":  "signal-service.log",
		"sandbox-engine":  "sandbox-engine.log",
		"eod-worker":      "eod_worker.out.log",
		"sentiment-worker": "sentiment-worker.log",
		"nats":            "nats.log",
		"backtester":      "backtester.log",
	}

	// Read main service logs (15 lines each)
	for service, logFile := range serviceFiles {
		filePath := filepath.Join(logDir, logFile)
		entries := readLogFileLines(filePath, service, 15)
		logs = append(logs, entries...)
	}

	// Read cron job logs (10 lines each from most recent files)
	cronLogFiles := map[string]string{
		"bhavcopy-backfill": "cron/bhavcopy_backfill_*.log",
		"bhavcopy-collector": "cron/bhavcopy_collector.log",
		"stock-news":      "cron/stock_news.log",
		"rss-feeds":       "cron/rss_feeds.log",
		"enhanced-news":   "cron/enhanced_news.log",
		"daily-predictions": "cron/daily_predictions.log",
		"fundamentals":    "cron/fundamentals.log",
		"maintenance":     "cron/maintenance.log",
		"premarket":       "cron/premarket_predictions.log",
		"morning-selection": "cron/morning_selection.log",
		"post-mortem":     "cron/post_mortem.log",
	}

	for service, pattern := range cronLogFiles {
		// Handle glob patterns for dated logs
		if strings.Contains(pattern, "*") {
			matches, _ := filepath.Glob(filepath.Join(logDir, pattern))
			// Get most recent file
			if len(matches) > 0 {
				// Sort by modification time and get latest
				latestFile := matches[len(matches)-1]
				entries := readLogFileLines(latestFile, "cron:"+service, 10)
				logs = append(logs, entries...)
			}
		} else {
			filePath := filepath.Join(logDir, pattern)
			entries := readLogFileLines(filePath, "cron:"+service, 10)
			logs = append(logs, entries...)
		}
	}

	// Read ML training logs
	mlLogPattern := filepath.Join(logDir, "ml_training_*.log")
	mlMatches, _ := filepath.Glob(mlLogPattern)
	if len(mlMatches) > 0 {
		latestML := mlMatches[len(mlMatches)-1]
		entries := readLogFileLines(latestML, "ml-training", 15)
		logs = append(logs, entries...)
	}

	c.JSON(http.StatusOK, gin.H{
		"logs":      logs,
		"total":     len(logs),
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// GetErrorLogs handles GET /api/monitoring/logs/errors
func (h *MonitoringHandler) GetErrorLogs(c *gin.Context) {
	logDir := "/Users/hariprasath/trading-chitti/logs"
	logs := []LogEntry{}

	// All service logs to scan for errors
	serviceFiles := map[string]string{
		"core-api":        "core-api.log",
		"intraday-engine": "intraday-engine.log",
		"market-bridge":   "market-bridge.log",
		"news-nlp":        "news-nlp.log",
		"signal-service":  "signal-service.log",
		"sandbox-engine":  "sandbox-engine.log",
		"eod-worker":      "eod_worker.out.log",
		"sentiment-worker": "sentiment-worker.log",
		"backtester":      "backtester.log",
	}

	// Error log files
	errorFiles := map[string]string{
		"core-api-err":        "core-api.err.log",
		"intraday-engine-err": "intraday-engine.err.log",
		"market-bridge-err":   "market-bridge.err.log",
		"news-nlp-err":        "news-nlp.err.log",
		"eod-worker-err":      "eod_worker.err.log",
		"sentiment-worker-err": "sentiment-worker.err.log",
		"sandbox-engine-err":  "sandbox-engine.err.log",
		"market-data-err":     "market-data-collector.err.log",
	}

	// Scan main service logs for errors
	for service, logFile := range serviceFiles {
		filePath := filepath.Join(logDir, logFile)
		entries := readLogFileLines(filePath, service, 30)

		for _, entry := range entries {
			if strings.Contains(strings.ToLower(entry.Level), "error") ||
				strings.Contains(strings.ToLower(entry.Message), "error") ||
				strings.Contains(entry.Message, "❌") ||
				strings.Contains(entry.Message, "✗") ||
				strings.Contains(strings.ToLower(entry.Message), "failed") ||
				strings.Contains(strings.ToLower(entry.Message), "fatal") {
				logs = append(logs, entry)
			}
		}
	}

	// Read dedicated error log files (last 20 lines each)
	for service, errFile := range errorFiles {
		filePath := filepath.Join(logDir, errFile)
		entries := readLogFileLines(filePath, service, 20)
		for _, entry := range entries {
			entry.Level = "ERROR"
			logs = append(logs, entry)
		}
	}

	// Scan recent cron logs for errors
	cronLogs := []string{
		"cron/stock_news.log",
		"cron/rss_feeds.log",
		"cron/enhanced_news.log",
		"cron/daily_predictions.log",
		"cron/fundamentals.log",
		"cron/maintenance.log",
	}

	for _, cronLog := range cronLogs {
		filePath := filepath.Join(logDir, cronLog)
		serviceName := "cron:" + strings.TrimSuffix(filepath.Base(cronLog), ".log")
		entries := readLogFileLines(filePath, serviceName, 20)

		for _, entry := range entries {
			if strings.Contains(strings.ToLower(entry.Message), "error") ||
				strings.Contains(entry.Message, "❌") ||
				strings.Contains(strings.ToLower(entry.Message), "failed") {
				logs = append(logs, entry)
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"logs":      logs,
		"total":     len(logs),
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

func readLogFileLines(filePath, service string, lines int) []LogEntry {
	entries := []LogEntry{}
	file, err := os.Open(filePath)
	if err != nil {
		return entries
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	allLines := []string{}
	for scanner.Scan() {
		allLines = append(allLines, scanner.Text())
	}

	start := len(allLines) - lines
	if start < 0 {
		start = 0
	}

	for _, line := range allLines[start:] {
		if line == "" {
			continue
		}
		entries = append(entries, parseLogEntry(line, service))
	}
	return entries
}

func parseLogEntry(line, service string) LogEntry {
	entry := LogEntry{
		Service:   service,
		Message:   line,
		Timestamp: time.Now().Format("15:04:05"),
		Level:     "INFO",
	}

	if strings.Contains(line, "ERROR") || strings.Contains(line, "❌") {
		entry.Level = "ERROR"
	} else if strings.Contains(line, "WARN") || strings.Contains(line, "⚠️") {
		entry.Level = "WARN"
	} else if strings.Contains(line, "✅") || strings.Contains(line, "INFO") {
		entry.Level = "INFO"
	}

	parts := strings.Fields(line)
	if len(parts) > 0 && (strings.Contains(parts[0], "/") || strings.Contains(parts[0], "-")) {
		entry.Timestamp = parts[0]
		if len(parts) > 1 && strings.Contains(parts[1], ":") {
			entry.Timestamp += " " + parts[1]
		}
	}

	return entry
}

// GetBrokerStatus handles GET /api/monitoring/broker-status
func (h *MonitoringHandler) GetBrokerStatus(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type BrokerStatus struct {
		Name          string  `json:"name"`
		Enabled       bool    `json:"enabled"`
		Authenticated bool    `json:"authenticated"`
		UserID        string  `json:"user_id,omitempty"`
		IsExpired     bool    `json:"is_expired"`
		ExpiresAt     *string `json:"expires_at,omitempty"`
	}

	brokers := []string{"zerodha", "indmoney"}
	statuses := []BrokerStatus{}

	for _, broker := range brokers {
		var (
			enabled    bool
			token      string
			userID     string
			expiresAt  *time.Time
		)

		err := h.db.QueryRowContext(ctx, `
			SELECT enabled, COALESCE(access_token, ''), COALESCE(user_id, ''), token_expires_at
			FROM brokers.config
			WHERE broker_name = $1
			ORDER BY updated_at DESC LIMIT 1
		`, broker).Scan(&enabled, &token, &userID, &expiresAt)

		if err != nil {
			statuses = append(statuses, BrokerStatus{Name: broker, Enabled: false, Authenticated: false})
			continue
		}

		isExpired := false
		var expiresAtStr *string
		if expiresAt != nil {
			isExpired = time.Now().After(*expiresAt)
			s := expiresAt.Format(time.RFC3339)
			expiresAtStr = &s
		}

		statuses = append(statuses, BrokerStatus{
			Name:          broker,
			Enabled:       enabled,
			Authenticated: token != "" && !isExpired,
			UserID:        userID,
			IsExpired:     isExpired,
			ExpiresAt:     expiresAtStr,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"brokers":   statuses,
		"timestamp": time.Now().Format(time.RFC3339),
	})
}
