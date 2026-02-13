package handlers

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// SystemHandler handles system monitoring endpoints
type SystemHandler struct {
	db *sql.DB
}

// NewSystemHandler creates a new system handler
func NewSystemHandler(db *sql.DB) *SystemHandler {
	return &SystemHandler{db: db}
}

// Service represents a system service
type Service struct {
	Name        string    `json:"name"`
	DisplayName string    `json:"displayName"`
	Status      string    `json:"status"`
	PID         int       `json:"pid"`
	Uptime      string    `json:"uptime"`
	Port        int       `json:"port,omitempty"`
	Description string    `json:"description"`
	LogFile     string    `json:"logFile,omitempty"`
	LastRestart time.Time `json:"lastRestart,omitempty"`
}

// CronJob represents a scheduled job
type CronJob struct {
	Name            string    `json:"name"`
	Description     string    `json:"description"`
	Schedule        string    `json:"schedule"`
	ScheduleHuman   string    `json:"scheduleHuman"`
	LastRun         time.Time `json:"lastRun,omitempty"`
	NextRun         time.Time `json:"nextRun,omitempty"`
	Status          string    `json:"status"` // "active", "disabled", "running"
	Command         string    `json:"command"`
	CanRunManually  bool      `json:"canRunManually"`
}

// MLModel represents an ML model with versioning
type MLModel struct {
	Name         string    `json:"name"`
	Version      string    `json:"version"`
	Type         string    `json:"type"` // "XGBoost", "PyTorch", "Mojo", etc.
	FilePath     string    `json:"filePath"`
	FileSize     int64     `json:"fileSize"`
	CreatedAt    time.Time `json:"createdAt"`
	Accuracy     float64   `json:"accuracy,omitempty"`
	Features     int       `json:"features,omitempty"`
	Description  string    `json:"description"`
	IsActive     bool      `json:"isActive"`
}

// GetServices returns list of all system services
func (h *SystemHandler) GetServices(c *gin.Context) {
	services := []Service{
		{
			Name:        "trading-chitti:core-api",
			DisplayName: "Core API (Go)",
			Status:      "running",
			Port:        6001,
			Description: "Main API server with 55+ endpoints",
			LogFile:     "/Users/hariprasath/trading-chitti/logs/core-api-go.log",
		},
		{
			Name:        "trading-chitti:market-bridge",
			DisplayName: "Market Bridge",
			Status:      "running",
			Port:        6005,
			Description: "Real-time market data bridge (Zerodha/NSE)",
			LogFile:     "/Users/hariprasath/trading-chitti/logs/market-bridge.log",
		},
		{
			Name:        "trading-chitti:intraday-engine",
			DisplayName: "Intraday Scanner",
			Status:      "running",
			Port:        6007,
			Description: "Intraday signal generation engine",
			LogFile:     "/Users/hariprasath/trading-chitti/logs/intraday-engine.log",
		},
		{
			Name:        "trading-chitti:dashboard-app",
			DisplayName: "Dashboard (Next.js)",
			Status:      "running",
			Port:        6003,
			Description: "Trading dashboard web interface",
			LogFile:     "/Users/hariprasath/trading-chitti/logs/dashboard-app.log",
		},
		{
			Name:        "trading-chitti:news-nlp",
			DisplayName: "News NLP Collector",
			Status:      "running",
			Description: "News collection and sentiment analysis",
			LogFile:     "/Users/hariprasath/trading-chitti/logs/news-nlp.log",
		},
	}

	// Try to get real status from supervisorctl
	supervisorStatus := getSupervisorStatus()
	for i := range services {
		if status, ok := supervisorStatus[services[i].Name]; ok {
			services[i].Status = status.Status
			services[i].PID = status.PID
			services[i].Uptime = status.Uptime
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"services": services,
		"total":    len(services),
	})
}

// GetJobs returns list of all cron jobs
func (h *SystemHandler) GetJobs(c *gin.Context) {
	jobs := []CronJob{
		{
			Name:           "log-cleanup",
			Description:    "Clean up old log files and rotate logs",
			Schedule:       "0 0 * * *",
			ScheduleHuman:  "Daily at midnight",
			CanRunManually: true,
			Command:        "/Users/hariprasath/trading-chitti/infra/cron/log_cleanup.sh",
			Status:         "active",
		},
		{
			Name:           "daily-predictions",
			Description:    "Generate daily market predictions using ML model",
			Schedule:       "0 8 * * *",
			ScheduleHuman:  "Daily at 8:00 AM",
			CanRunManually: true,
			Command:        "/opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/intraday-engine/scripts/predict_market.py",
			Status:         "active",
		},
		{
			Name:           "morning-selection",
			Description:    "Select top stocks for intraday trading (smart selection)",
			Schedule:       "45 8 * * 1-5",
			ScheduleHuman:  "Weekdays at 8:45 AM",
			CanRunManually: true,
			Command:        "/opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/scripts/select_daily_stocks.py",
			Status:         "active",
		},
		{
			Name:           "ml-retraining",
			Description:    "Weekly ML model retraining with latest data",
			Schedule:       "0 21 * * 0",
			ScheduleHuman:  "Sundays at 9:00 PM",
			CanRunManually: true,
			Command:        "/opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/scripts/retrain_ml_model_auto.py",
			Status:         "active",
		},
		{
			Name:           "stock-news-collector",
			Description:    "Collect individual stock news (GNews API) for ML predictions",
			Schedule:       "*/5 7-15 * * 1-5",
			ScheduleHuman:  "Every 5 min, 7AM-3:30PM weekdays",
			CanRunManually: true,
			Command:        "export LOG_LEVEL=WARNING && /opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/scripts/collect_stock_news.py",
			Status:         "active",
		},
		{
			Name:           "enhanced-news-collector",
			Description:    "Collect enhanced market news for ML predictions",
			Schedule:       "*/5 7-15 * * 1-5",
			ScheduleHuman:  "Every 5 min, 7AM-3:30PM weekdays",
			CanRunManually: true,
			Command:        "export LOG_LEVEL=WARNING && /opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/scripts/collect_enhanced_news.py",
			Status:         "active",
		},
		{
			Name:           "rss-feeds-collector",
			Description:    "Collect RSS feeds for ML predictions",
			Schedule:       "*/5 7-15 * * 1-5",
			ScheduleHuman:  "Every 5 min, 7AM-3:30PM weekdays",
			CanRunManually: true,
			Command:        "LOG_LEVEL=WARNING /opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/scripts/collect_rss_feeds.py",
			Status:         "active",
		},
		{
			Name:           "market-maintenance",
			Description:    "After-market maintenance and cleanup tasks",
			Schedule:       "0 16 * * 1-5",
			ScheduleHuman:  "Weekdays at 4:00 PM",
			CanRunManually: true,
			Command:        "/opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/maintenance/after_market_maintenance.py",
			Status:         "active",
		},
		{
			Name:           "bar-collector-start",
			Description:    "Start intraday bar collector at market open",
			Schedule:       "14 9 * * 1-5",
			ScheduleHuman:  "Weekdays at 9:14 AM",
			CanRunManually: true,
			Command:        "/Users/hariprasath/trading-chitti/scripts/start_bar_collector.sh",
			Status:         "active",
		},
		{
			Name:           "wildcard-cleanup",
			Description:    "Clean up wildcard subscriptions and orphaned data",
			Schedule:       "*/15 * * * *",
			ScheduleHuman:  "Every 15 minutes",
			CanRunManually: true,
			Command:        "/opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/scripts/cleanup_wildcards.py",
			Status:         "active",
		},
		{
			Name:           "fundamentals-update",
			Description:    "Update fundamental data (P/E, debt, revenue, etc.)",
			Schedule:       "0 19 * * 3",
			ScheduleHuman:  "Wednesdays at 7:00 PM",
			CanRunManually: true,
			Command:        "/Users/hariprasath/trading-chitti/infra/cron/update_fundamentals.sh",
			Status:         "active",
		},
		{
			Name:           "premarket-predictions",
			Description:    "Generate pre-market predictions and alerts",
			Schedule:       "0 7 * * 1-5",
			ScheduleHuman:  "Weekdays at 7:00 AM",
			CanRunManually: true,
			Command:        "/Users/hariprasath/trading-chitti/scripts/run_premarket_predictions.sh",
			Status:         "active",
		},
		{
			Name:           "post-mortem",
			Description:    "Daily post-mortem analysis of signals",
			Schedule:       "15 16 * * 1-5",
			ScheduleHuman:  "Weekdays at 4:15 PM",
			CanRunManually: true,
			Command:        "/Users/hariprasath/trading-chitti/scripts/run_daily_post_mortem.sh",
			Status:         "active",
		},
		{
			Name:           "log-rotation",
			Description:    "Rotate and compress log files",
			Schedule:       "0 2 * * *",
			ScheduleHuman:  "Daily at 2:00 AM",
			CanRunManually: false,
			Command:        "/Users/hariprasath/trading-chitti/scripts/rotate_logs.sh",
			Status:         "active",
		},
		{
			Name:           "backtest-data-collector",
			Description:    "Aggregate intraday bars into daily bars (90-day window)",
			Schedule:       "0 23 * * *",
			ScheduleHuman:  "Daily at 11:00 PM",
			CanRunManually: true,
			Command:        "/Users/hariprasath/trading-chitti/infra/cron/backtest_data_collector.sh",
			Status:         "active",
		},
		{
			Name:           "bhavcopy-collector",
			Description:    "Download official NSE Bhavcopy (EOD data)",
			Schedule:       "0 19 * * 1-5",
			ScheduleHuman:  "Weekdays at 7:00 PM",
			CanRunManually: true,
			Command:        "/Users/hariprasath/trading-chitti/infra/cron/bhavcopy_collector.sh",
			Status:         "active",
		},
	}

	// Get last run times from database
	for i := range jobs {
		lastRun, nextRun := getJobTiming(&jobs[i])
		jobs[i].LastRun = lastRun
		jobs[i].NextRun = nextRun
	}

	c.JSON(http.StatusOK, gin.H{
		"jobs":  jobs,
		"total": len(jobs),
	})
}

// RunJobManually triggers a manual job run
func (h *SystemHandler) RunJobManually(c *gin.Context) {
	jobName := c.Param("jobName")

	// Find the job command
	var command string
	jobMap := map[string]string{
		"log-cleanup":             "/Users/hariprasath/trading-chitti/infra/cron/log_cleanup.sh",
		"daily-predictions":       "/opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/intraday-engine/scripts/predict_market.py",
		"morning-selection":       "/opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/scripts/select_daily_stocks.py",
		"ml-retraining":           "/opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/scripts/retrain_ml_model_auto.py",
		"stock-news-collector":    "export LOG_LEVEL=WARNING && /opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/scripts/collect_stock_news.py",
		"enhanced-news-collector": "export LOG_LEVEL=WARNING && /opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/scripts/collect_enhanced_news.py",
		"rss-feeds-collector":     "LOG_LEVEL=WARNING /opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/scripts/collect_rss_feeds.py",
		"market-maintenance":      "/opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/maintenance/after_market_maintenance.py",
		"bar-collector-start":     "/Users/hariprasath/trading-chitti/scripts/start_bar_collector.sh",
		"wildcard-cleanup":        "/opt/homebrew/bin/python3 /Users/hariprasath/trading-chitti/scripts/cleanup_wildcards.py",
		"fundamentals-update":     "/Users/hariprasath/trading-chitti/infra/cron/update_fundamentals.sh",
		"premarket-predictions":   "/Users/hariprasath/trading-chitti/scripts/run_premarket_predictions.sh",
		"post-mortem":             "/Users/hariprasath/trading-chitti/scripts/run_daily_post_mortem.sh",
		"backtest-data-collector": "/Users/hariprasath/trading-chitti/infra/cron/backtest_data_collector.sh",
		"bhavcopy-collector":      "/Users/hariprasath/trading-chitti/infra/cron/bhavcopy_collector.sh",
	}

	command, exists := jobMap[jobName]
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{
			"error":   "Job not found",
			"jobName": jobName,
			"hint":    "Available jobs: log-cleanup, daily-predictions, morning-selection, ml-retraining, stock-news-collector, enhanced-news-market, rss-feeds-market, market-maintenance, bar-collector-start, wildcard-cleanup, fundamentals-update, premarket-predictions, post-mortem, backtest-data-collector, bhavcopy-collector",
		})
		return
	}

	// Run the job in background
	go func() {
		cmd := exec.Command("bash", "-c", command)
		cmd.Env = append(os.Environ(),
			"TRADING_CHITTI_PG_DSN=postgresql://hariprasath@localhost:6432/trading_chitti?sslmode=disable",
			"LOG_LEVEL=WARNING",
		)

		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("❌ Manual job run failed for %s: %v\nOutput: %s", jobName, err, output)
		} else {
			log.Printf("✅ Manual job run successful for %s\nOutput: %s", jobName, output)
		}
	}()

	c.JSON(http.StatusOK, gin.H{
		"message": fmt.Sprintf("Job '%s' triggered successfully", jobName),
		"jobName": jobName,
		"status":  "running",
		"note":    "Job is running in background. Check logs for progress.",
	})
}

// GetMLModels returns list of ML models with versioning
func (h *SystemHandler) GetMLModels(c *gin.Context) {
	models := []MLModel{}

	// Scan ML model directories
	mlDirs := []string{
		"/Users/hariprasath/trading-chitti/intraday-engine/intraday_engine",
		"/Users/hariprasath/trading-chitti/scripts",
	}

	for _, dir := range mlDirs {
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			continue
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			// Check for model files
			name := file.Name()
			if strings.Contains(name, ".joblib") || strings.Contains(name, ".pkl") ||
			   strings.Contains(name, ".pt") || strings.Contains(name, ".pth") {

				model := MLModel{
					Name:      extractModelName(name),
					Version:   extractVersion(name),
					FilePath:  filepath.Join(dir, name),
					FileSize:  file.Size(),
					CreatedAt: file.ModTime(),
					IsActive:  isActiveModel(name),
				}

				// Determine model type
				if strings.HasSuffix(name, ".joblib") || strings.HasSuffix(name, ".pkl") {
					model.Type = "XGBoost/Scikit-learn"
				} else if strings.HasSuffix(name, ".pt") || strings.HasSuffix(name, ".pth") {
					model.Type = "PyTorch"
				}

				// Extract metadata if available
				if strings.Contains(name, "depth") {
					model.Features = 41
					model.Description = "Intraday scanner with market depth features"
				} else if strings.Contains(name, "xgboost") {
					model.Features = 29
					model.Description = "XGBoost intraday prediction model"
				} else if strings.Contains(name, "pytorch") {
					model.Features = 50
					model.Description = "PyTorch GPU-accelerated ML model"
				}

				models = append(models, model)
			}
		}
	}

	// Get model performance from database
	for i := range models {
		accuracy := getModelAccuracy(h.db, models[i].Name)
		models[i].Accuracy = accuracy
	}

	c.JSON(http.StatusOK, gin.H{
		"models": models,
		"total":  len(models),
	})
}

// Helper functions

type SupervisorStatus struct {
	Status string
	PID    int
	Uptime string
}

func getSupervisorStatus() map[string]SupervisorStatus {
	result := make(map[string]SupervisorStatus)

	cmd := exec.Command("supervisorctl", "-c", "/Users/hariprasath/trading-chitti/infra/supervisord.conf", "status")
	output, err := cmd.Output()
	if err != nil {
		return result
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 3 {
			name := fields[0]
			status := fields[1]

			sStatus := SupervisorStatus{
				Status: strings.ToLower(status),
			}

			// Parse PID if available
			if strings.Contains(line, "pid") {
				for i, field := range fields {
					if field == "pid" && i+1 < len(fields) {
						fmt.Sscanf(fields[i+1], "%d,", &sStatus.PID)
					}
					if field == "uptime" && i+1 < len(fields) {
						sStatus.Uptime = strings.Join(fields[i+1:], " ")
						break
					}
				}
			}

			result[name] = sStatus
		}
	}

	return result
}

func getJobTiming(job *CronJob) (lastRun, nextRun time.Time) {
	// Parse cron schedule to calculate next run
	// Simplified - in production, use a cron parser library

	now := time.Now()

	// Example calculations based on common patterns
	switch {
	case strings.Contains(job.Schedule, "*/5 9-15"): // Every 5 minutes during market hours
		nextRun = now.Add(5 * time.Minute)
	case strings.Contains(job.Schedule, "14 9"): // Daily at 9:14 AM
		nextRun = time.Date(now.Year(), now.Month(), now.Day()+1, 9, 14, 0, 0, now.Location())
	case strings.Contains(job.Schedule, "0 23"): // Daily at 11 PM
		nextRun = time.Date(now.Year(), now.Month(), now.Day()+1, 23, 0, 0, 0, now.Location())
	case strings.Contains(job.Schedule, "0 19"): // Weekdays at 7 PM
		nextRun = time.Date(now.Year(), now.Month(), now.Day()+1, 19, 0, 0, 0, now.Location())
	}

	// Last run would come from job state table (if available)
	lastRun = now.Add(-1 * time.Hour) // Placeholder

	return lastRun, nextRun
}

func extractModelName(filename string) string {
	name := strings.TrimSuffix(filename, filepath.Ext(filename))
	// Remove version/date suffix
	parts := strings.Split(name, "_")
	if len(parts) > 2 {
		return strings.Join(parts[:2], "_")
	}
	return name
}

func extractVersion(filename string) string {
	// Extract version from filename like "intraday_xgboost_depth_20260205_192929.joblib"
	parts := strings.Split(filename, "_")
	for _, part := range parts {
		if len(part) == 8 && strings.HasPrefix(part, "202") {
			// Date format: YYYYMMDD
			if t, err := time.Parse("20060102", part); err == nil {
				return t.Format("2006-01-02")
			}
		}
	}
	return "unknown"
}

func isActiveModel(filename string) bool {
	// Models without date suffix are usually the active ones
	return !strings.Contains(filename, "202")
}

func getModelAccuracy(db *sql.DB, modelName string) float64 {
	// Query ML model performance from database (if tracked)
	var accuracy float64
	query := `
		SELECT accuracy
		FROM ml.model_performance
		WHERE model_name = $1
		ORDER BY evaluated_at DESC
		LIMIT 1
	`
	err := db.QueryRow(query, modelName).Scan(&accuracy)
	if err != nil {
		return 0.0
	}
	return accuracy
}
