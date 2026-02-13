package database

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// DashboardSignal represents a signal for the dashboard view
type DashboardSignal struct {
	SignalID        string          `json:"signal_id"`
	SignalNumber    int             `json:"signal_number"`
	Symbol          string          `json:"symbol"`
	StockName       string          `json:"stock_name"`
	Sector          string          `json:"sector"`
	SignalType      string          `json:"signal_type"`
	EntryPrice      float64         `json:"entry_price"`
	CurrentPrice    float64         `json:"current_price"`
	ExitPrice       *float64        `json:"exit_price,omitempty"`
	TargetPrice     float64         `json:"target_price"`
	StopLoss        float64         `json:"stop_loss"`
	ExpectedProfitPct float64       `json:"expected_profit_pct"`
	ActualProfitPct *float64        `json:"actual_profit_pct,omitempty"`
	ConfidenceScore float64         `json:"confidence_score"`
	SuccessRatePct  *float64        `json:"success_rate_pct"`
	Status          string          `json:"status"`
	ValidationStatus string         `json:"validation_status"`
	GeneratedAt     string          `json:"generated_at"`
	UpdatedAt       string          `json:"updated_at"`
	ClosedAt        *string         `json:"closed_at,omitempty"`
	ExpiresAt       string          `json:"expires_at"`
	Metadata        json.RawMessage `json:"metadata"`
}

// DashboardStats represents signal statistics
type DashboardStats struct {
	TotalSignals  int      `json:"total_signals"`
	ActiveCount   int      `json:"active_count"`
	Hits          int      `json:"hits"`
	Misses        int      `json:"misses"`
	Expired       int      `json:"expired"`
	AvgConfidence float64  `json:"avg_confidence"`
	AvgProfitHit  float64  `json:"avg_profit_on_hit"`
	AvgLossMiss   float64  `json:"avg_loss_on_miss"`
	SuccessRate   *float64 `json:"success_rate"`
}

// TopPerformer represents a top-performing stock
type TopPerformer struct {
	Symbol      string  `json:"symbol"`
	StockName   string  `json:"stock_name"`
	SignalCount int     `json:"signal_count"`
	Wins        int     `json:"wins"`
	AvgProfit   float64 `json:"avg_profit"`
}

// SignalDistribution represents signal type distribution
type SignalDistribution struct {
	SignalType    string  `json:"signal_type"`
	Count         int     `json:"count"`
	AvgConfidence float64 `json:"avg_confidence"`
	Hits          int     `json:"hits"`
}

// DashboardData represents the full dashboard response
type DashboardData struct {
	ActiveSignals      []DashboardSignal  `json:"active_signals"`
	ClosedSignals      []DashboardSignal  `json:"closed_signals"`
	Statistics         DashboardStats     `json:"statistics"`
	TopPerformers      []TopPerformer     `json:"top_performers"`
	SignalDistribution []SignalDistribution `json:"signal_distribution"`
	Metadata           map[string]interface{} `json:"metadata"`
}

// GetDashboardData retrieves aggregated dashboard data
func (db *DB) GetDashboardData(ctx context.Context, limit int, includeClosed bool) (*DashboardData, error) {
	data := &DashboardData{
		ActiveSignals:      []DashboardSignal{},
		ClosedSignals:      []DashboardSignal{},
		TopPerformers:      []TopPerformer{},
		SignalDistribution: []SignalDistribution{},
	}

	if limit <= 0 {
		limit = 100
	}

	// Active signals
	activeQuery := `
		SELECT
			signal_id, ROW_NUMBER() OVER (ORDER BY generated_at) as signal_number,
			symbol, COALESCE(stock_name, symbol), COALESCE(sector, ''),
			signal_type, entry_price, current_price, target_price, stop_loss,
			CASE WHEN entry_price > 0 THEN ((target_price - entry_price) / entry_price * 100) ELSE 0 END as expected_profit_pct,
			confidence_score, status,
			COALESCE(generated_at::text, ''), COALESCE(generated_at::text, ''),
			COALESCE((generated_at + INTERVAL '6 hours')::text, ''),
			COALESCE(metadata::text, '{}')
		FROM intraday.signals
		WHERE status = 'ACTIVE' AND generated_at >= CURRENT_DATE
		ORDER BY generated_at DESC
		LIMIT $1
	`
	activeRows, err := db.conn.QueryContext(ctx, activeQuery, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query active signals: %w", err)
	}
	defer activeRows.Close()

	for activeRows.Next() {
		var s DashboardSignal
		var metadataStr string
		if err := activeRows.Scan(
			&s.SignalID, &s.SignalNumber, &s.Symbol, &s.StockName, &s.Sector,
			&s.SignalType, &s.EntryPrice, &s.CurrentPrice, &s.TargetPrice, &s.StopLoss,
			&s.ExpectedProfitPct, &s.ConfidenceScore, &s.Status,
			&s.GeneratedAt, &s.UpdatedAt, &s.ExpiresAt, &metadataStr,
		); err != nil {
			return nil, fmt.Errorf("failed to scan active signal: %w", err)
		}
		s.ValidationStatus = "VALID"
		s.Metadata = json.RawMessage(metadataStr)
		data.ActiveSignals = append(data.ActiveSignals, s)
	}
	if err := activeRows.Err(); err != nil {
		return nil, fmt.Errorf("active rows iteration error: %w", err)
	}

	// Closed signals
	if includeClosed {
		closedQuery := `
			SELECT
				signal_id, ROW_NUMBER() OVER (ORDER BY generated_at) as signal_number,
				symbol, COALESCE(stock_name, symbol), COALESCE(sector, ''),
				signal_type, entry_price, current_price, exit_price, target_price, stop_loss,
				CASE WHEN entry_price > 0 THEN ((target_price - entry_price) / entry_price * 100) ELSE 0 END,
				actual_profit_pct, confidence_score, status,
				COALESCE(generated_at::text, ''), COALESCE(generated_at::text, ''),
				COALESCE(closed_at::text, ''),
				COALESCE(metadata::text, '{}')
			FROM intraday.signals
			WHERE status IN ('HIT_TARGET', 'HIT_STOPLOSS', 'TRAILING_STOP', 'TIME_EXIT', 'EXPIRED')
				AND generated_at >= CURRENT_DATE
			ORDER BY closed_at DESC
			LIMIT $1
		`
		closedRows, err := db.conn.QueryContext(ctx, closedQuery, limit)
		if err != nil {
			return nil, fmt.Errorf("failed to query closed signals: %w", err)
		}
		defer closedRows.Close()

		for closedRows.Next() {
			var s DashboardSignal
			var metadataStr string
			var closedAt *string
			if err := closedRows.Scan(
				&s.SignalID, &s.SignalNumber, &s.Symbol, &s.StockName, &s.Sector,
				&s.SignalType, &s.EntryPrice, &s.CurrentPrice, &s.ExitPrice, &s.TargetPrice, &s.StopLoss,
				&s.ExpectedProfitPct, &s.ActualProfitPct, &s.ConfidenceScore, &s.Status,
				&s.GeneratedAt, &s.UpdatedAt, &closedAt, &metadataStr,
			); err != nil {
				return nil, fmt.Errorf("failed to scan closed signal: %w", err)
			}
			s.ValidationStatus = "CLOSED"
			s.ClosedAt = closedAt
			s.Metadata = json.RawMessage(metadataStr)
			data.ClosedSignals = append(data.ClosedSignals, s)
		}
		if err := closedRows.Err(); err != nil {
			return nil, fmt.Errorf("closed rows iteration error: %w", err)
		}
	}

	// Statistics - using result column to count hits/misses
	// HIT includes: HIT_TARGET + profitable TIME_EXIT/TRAILING_STOP
	// MISS includes: HIT_STOPLOSS + unprofitable TIME_EXIT/TRAILING_STOP
	err = db.conn.QueryRowContext(ctx, `
		SELECT
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE status = 'ACTIVE') as active,
			COUNT(*) FILTER (WHERE result = 'HIT') as hits,
			COUNT(*) FILTER (WHERE result = 'MISS') as misses,
			COUNT(*) FILTER (WHERE status IN ('EXPIRED', 'TIME_EXIT')) as expired,
			COALESCE(AVG(confidence_score), 0) as avg_confidence,
			COALESCE(AVG(actual_profit_pct) FILTER (WHERE result = 'HIT'), 0) as avg_profit_hit,
			COALESCE(AVG(actual_profit_pct) FILTER (WHERE result = 'MISS'), 0) as avg_loss_miss,
			ROUND(
				COUNT(*) FILTER (WHERE result = 'HIT')::numeric /
				NULLIF(COUNT(*) FILTER (WHERE result IS NOT NULL), 0) * 100,
				2
			) as success_rate
		FROM intraday.signals
		WHERE generated_at >= CURRENT_DATE
	`).Scan(
		&data.Statistics.TotalSignals, &data.Statistics.ActiveCount,
		&data.Statistics.Hits, &data.Statistics.Misses, &data.Statistics.Expired,
		&data.Statistics.AvgConfidence, &data.Statistics.AvgProfitHit,
		&data.Statistics.AvgLossMiss, &data.Statistics.SuccessRate,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get signal stats: %w", err)
	}

	// Top performers
	topQuery := `
		SELECT
			symbol, COALESCE(stock_name, symbol),
			COUNT(*) as signal_count,
			COUNT(*) FILTER (WHERE status = 'HIT_TARGET') as wins,
			COALESCE(AVG(actual_profit_pct) FILTER (WHERE actual_profit_pct IS NOT NULL), 0) as avg_profit
		FROM intraday.signals
		WHERE generated_at >= CURRENT_DATE - INTERVAL '7 days'
		GROUP BY symbol, stock_name
		HAVING COUNT(*) >= 2
		ORDER BY wins DESC, avg_profit DESC
		LIMIT 10
	`
	topRows, err := db.conn.QueryContext(ctx, topQuery)
	if err == nil {
		defer topRows.Close()
		for topRows.Next() {
			var t TopPerformer
			if err := topRows.Scan(&t.Symbol, &t.StockName, &t.SignalCount, &t.Wins, &t.AvgProfit); err == nil {
				data.TopPerformers = append(data.TopPerformers, t)
			}
		}
	}

	// Signal distribution
	distQuery := `
		SELECT
			signal_type,
			COUNT(*) as count,
			COALESCE(AVG(confidence_score), 0) as avg_confidence,
			COUNT(*) FILTER (WHERE status = 'HIT_TARGET') as hits
		FROM intraday.signals
		WHERE generated_at >= CURRENT_DATE
		GROUP BY signal_type
		ORDER BY count DESC
	`
	distRows, err := db.conn.QueryContext(ctx, distQuery)
	if err == nil {
		defer distRows.Close()
		for distRows.Next() {
			var d SignalDistribution
			if err := distRows.Scan(&d.SignalType, &d.Count, &d.AvgConfidence, &d.Hits); err == nil {
				data.SignalDistribution = append(data.SignalDistribution, d)
			}
		}
	}

	data.Metadata = map[string]interface{}{
		"timestamp":    time.Now().Format(time.RFC3339),
		"active_count": len(data.ActiveSignals),
		"closed_count": len(data.ClosedSignals),
	}

	return data, nil
}

// InvestmentSignal represents a stock investment signal
type InvestmentStockSignal struct {
	ID               string   `json:"id"`
	Type             string   `json:"type"`
	Symbol           string   `json:"symbol"`
	Name             string   `json:"name"`
	Action           string   `json:"action"`
	CurrentPrice     float64  `json:"current_price"`
	TargetPrice      float64  `json:"target_price"`
	StopLoss         float64  `json:"stop_loss"`
	ExpectedReturn   float64  `json:"expected_return"`
	ExpectedDuration string   `json:"expected_duration"`
	Confidence       float64  `json:"confidence"`
	SuccessRate      float64  `json:"success_rate"`
	NewsSentiment    float64  `json:"news_sentiment"`
	NewsArticleCount int      `json:"news_article_count"`
	Rationale        string   `json:"rationale"`
	Sectors          []string `json:"sectors"`
	Timestamp        string   `json:"timestamp"`
}

// InvestmentSectorSignal represents a sector investment signal
type InvestmentSectorSignal struct {
	ID            string                   `json:"id"`
	Type          string                   `json:"type"`
	Sector        string                   `json:"sector"`
	Action        string                   `json:"action"`
	AvgSentiment  float64                  `json:"avg_sentiment"`
	ArticleCount  int                      `json:"article_count"`
	StocksCount   int                      `json:"stocks_count"`
	AvgConfidence float64                  `json:"avg_confidence"`
	Stocks        []map[string]interface{} `json:"stocks"`
	Timestamp     string                   `json:"timestamp"`
}

// InvestmentSignalsResponse represents the full investment signals response
type InvestmentSignalsResponse struct {
	StockSignals  []InvestmentStockSignal  `json:"stock_signals"`
	SectorSignals []InvestmentSectorSignal `json:"sector_signals"`
	ETFSignals    []interface{}            `json:"etf_signals"`
	Metadata      map[string]interface{}   `json:"metadata"`
}

// GetInvestmentSignals retrieves investment-grade signals
func (db *DB) GetInvestmentSignals(ctx context.Context, minConfidence, minSuccessRate float64, requireSentiment bool) (*InvestmentSignalsResponse, error) {
	resp := &InvestmentSignalsResponse{
		StockSignals:  []InvestmentStockSignal{},
		SectorSignals: []InvestmentSectorSignal{},
		ETFSignals:    []interface{}{},
	}

	// Stock signals from recent intraday signals with good confidence
	query := `
		SELECT
			s.signal_id, s.symbol, COALESCE(s.stock_name, s.symbol), COALESCE(s.sector, ''),
			s.signal_type, s.entry_price, s.target_price, s.stop_loss,
			s.confidence_score, COALESCE(s.recent_news_sentiment, 0),
			s.generated_at
		FROM intraday.signals s
		INNER JOIN md.stock_config sc ON sc.symbol = s.symbol AND sc.active = true AND sc.investment_enabled = true
		WHERE s.confidence_score >= $1
			AND s.generated_at >= CURRENT_DATE - INTERVAL '1 day'
			AND s.status = 'ACTIVE'
		ORDER BY s.confidence_score DESC
		LIMIT 30
	`

	rows, err := db.conn.QueryContext(ctx, query, minConfidence)
	if err != nil {
		return nil, fmt.Errorf("failed to query investment signals: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var signalID, symbol, name, sector, signalType string
		var entryPrice, targetPrice, stopLoss, confidence, sentiment float64
		var generatedAt time.Time

		if err := rows.Scan(&signalID, &symbol, &name, &sector, &signalType,
			&entryPrice, &targetPrice, &stopLoss, &confidence, &sentiment, &generatedAt); err != nil {
			continue
		}

		if requireSentiment && sentiment == 0 {
			continue
		}

		expectedReturn := 0.0
		if entryPrice > 0 {
			expectedReturn = (targetPrice - entryPrice) / entryPrice * 100
		}

		action := "BUY"
		if signalType == "PUT" {
			action = "SELL"
		}

		sig := InvestmentStockSignal{
			ID:               signalID,
			Type:             "stock",
			Symbol:           symbol,
			Name:             name,
			Action:           action,
			CurrentPrice:     entryPrice,
			TargetPrice:      targetPrice,
			StopLoss:         stopLoss,
			ExpectedReturn:   expectedReturn,
			ExpectedDuration: "1-5 days",
			Confidence:       confidence,
			SuccessRate:      0,
			NewsSentiment:    sentiment,
			NewsArticleCount: 0,
			Rationale:        fmt.Sprintf("%s signal for %s with %.0f%% confidence", signalType, symbol, confidence*100),
			Sectors:          []string{},
			Timestamp:        generatedAt.Format(time.RFC3339),
		}
		if sector != "" {
			sig.Sectors = []string{sector}
		}

		resp.StockSignals = append(resp.StockSignals, sig)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	// Sector signals
	sectorQuery := `
		SELECT
			COALESCE(sector, 'OTHER') as sector,
			COUNT(*) as stocks_count,
			AVG(confidence_score) as avg_confidence,
			COALESCE(AVG(recent_news_sentiment), 0) as avg_sentiment
		FROM intraday.signals s
		INNER JOIN md.stock_config sc ON sc.symbol = s.symbol AND sc.active = true AND sc.investment_enabled = true
		WHERE s.generated_at >= CURRENT_DATE - INTERVAL '1 day'
			AND s.status = 'ACTIVE'
			AND s.sector IS NOT NULL AND s.sector != ''
		GROUP BY sector
		HAVING COUNT(*) >= 2
		ORDER BY avg_confidence DESC
		LIMIT 10
	`
	sectorRows, err := db.conn.QueryContext(ctx, sectorQuery)
	if err == nil {
		defer sectorRows.Close()
		for sectorRows.Next() {
			var sector string
			var stocksCount int
			var avgConfidence, avgSentiment float64
			if err := sectorRows.Scan(&sector, &stocksCount, &avgConfidence, &avgSentiment); err == nil {
				action := "BUY"
				if avgSentiment < -0.1 {
					action = "SELL"
				}
				resp.SectorSignals = append(resp.SectorSignals, InvestmentSectorSignal{
					ID:            fmt.Sprintf("sector-%s", sector),
					Type:          "sector",
					Sector:        sector,
					Action:        action,
					AvgSentiment:  avgSentiment,
					ArticleCount:  0,
					StocksCount:   stocksCount,
					AvgConfidence: avgConfidence,
					Stocks:        []map[string]interface{}{},
					Timestamp:     time.Now().Format(time.RFC3339),
				})
			}
		}
	}

	resp.Metadata = map[string]interface{}{
		"filters_applied": map[string]interface{}{
			"min_confidence":          minConfidence,
			"min_success_rate":        minSuccessRate,
			"require_news_sentiment":  requireSentiment,
		},
		"timestamp":    time.Now().Format(time.RFC3339),
		"stock_count":  len(resp.StockSignals),
		"sector_count": len(resp.SectorSignals),
		"etf_count":    0,
	}

	return resp, nil
}

// NewsAlert represents a trading alert derived from news
type NewsAlert struct {
	ID         string   `json:"id"`
	CreatedAt  string   `json:"created_at"`
	Title      string   `json:"title"`
	Link       string   `json:"link"`
	Source     string   `json:"source"`
	Impact     string   `json:"impact"`
	Direction  string   `json:"direction"`
	Action     string   `json:"action"`
	MovePct    float64  `json:"move_pct"`
	MoveRange  string   `json:"move_range"`
	Confidence float64  `json:"confidence"`
	Duration   string   `json:"duration"`
	Sectors    []string `json:"sectors"`
	Symbols    []string `json:"symbols"`
	Rationale  string   `json:"rationale"`
	Meta       interface{} `json:"meta"`
}

// GetSignalAlerts retrieves news-based trading alerts
func (db *DB) GetSignalAlerts(ctx context.Context, strategy string, minConfidence float64) ([]NewsAlert, error) {
	query := `
		SELECT
			a.id,
			COALESCE(a.published_at::text, ''),
			COALESCE(a.title, ''),
			COALESCE(a.url, ''),
			COALESCE(a.source, 'Unknown'),
			COALESCE(a.llm_sentiment, 'neutral'),
			COALESCE(a.llm_confidence, 0.5)
		FROM news.articles a
		WHERE a.published_at >= CURRENT_DATE - INTERVAL '2 days'
			AND a.llm_sentiment IS NOT NULL
			AND COALESCE(a.llm_confidence, 0) >= $1
		ORDER BY a.published_at DESC
		LIMIT 50
	`

	rows, err := db.conn.QueryContext(ctx, query, minConfidence)
	if err != nil {
		return []NewsAlert{}, nil
	}
	defer rows.Close()

	var alerts []NewsAlert
	for rows.Next() {
		var id, createdAt, title, link, source, sentiment string
		var confidence float64
		if err := rows.Scan(&id, &createdAt, &title, &link, &source, &sentiment, &confidence); err != nil {
			continue
		}

		action := "HOLD"
		direction := "neutral"
		movePct := 0.0
		impact := "low"

		switch sentiment {
		case "positive":
			action = "BUY"
			direction = "up"
			movePct = confidence * 3
			impact = "high"
		case "negative":
			action = "SELL"
			direction = "down"
			movePct = -confidence * 3
			impact = "high"
		}

		alert := NewsAlert{
			ID:         id,
			CreatedAt:  createdAt,
			Title:      title,
			Link:       link,
			Source:      source,
			Impact:     impact,
			Direction:  direction,
			Action:     action,
			MovePct:    movePct,
			MoveRange:  fmt.Sprintf("%.1f%% to %.1f%%", movePct*0.5, movePct*1.5),
			Confidence: confidence,
			Duration:   "1-3 days",
			Sectors:    []string{},
			Symbols:    []string{},
			Rationale:  fmt.Sprintf("News sentiment: %s (%.0f%% confidence)", sentiment, confidence*100),
			Meta:       nil,
		}

		alerts = append(alerts, alert)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	// Fetch entities for all alerts
	if len(alerts) > 0 {
		alertIDs := make([]string, len(alerts))
		alertMap := make(map[string]int)
		for i, a := range alerts {
			alertIDs[i] = a.ID
			alertMap[a.ID] = i
		}

		entityQuery := `
			SELECT article_id, symbol
			FROM news.article_entities
			WHERE article_id = ANY($1)
		`
		entityRows, err := db.conn.QueryContext(ctx, entityQuery, alertIDs)
		if err == nil {
			defer entityRows.Close()
			for entityRows.Next() {
				var articleID, sym string
				if err := entityRows.Scan(&articleID, &sym); err == nil {
					if idx, ok := alertMap[articleID]; ok {
						alerts[idx].Symbols = append(alerts[idx].Symbols, sym)
					}
				}
			}
		}
	}

	if alerts == nil {
		alerts = []NewsAlert{}
	}

	return alerts, nil
}

// PredictedMover represents a ML-predicted stock movement
type PredictedMover struct {
	Symbol             string  `json:"symbol"`
	CurrentPrice       float64 `json:"current_price"`
	PredictedPrice     float64 `json:"predicted_price"`
	PredictedChangePct float64 `json:"predicted_change_pct"`
	StopLoss           float64 `json:"stop_loss"`
	Target             float64 `json:"target"`
	Confidence         float64 `json:"confidence"`
	Trend              string  `json:"trend"`
	Reasoning          string  `json:"reasoning"`
	TechnicalSummary   string  `json:"technical_summary"`
}

// GetPredictedGainers returns ML-predicted top gainers
func (db *DB) GetPredictedGainers(ctx context.Context, limit int) ([]PredictedMover, error) {
	query := `
		SELECT
			symbol, current_price, predicted_price, predicted_change_pct,
			stop_loss, target, confidence, trend,
			COALESCE(reasoning, ''), COALESCE(technical_summary, '')
		FROM predictions.daily_predictions
		WHERE prediction_date = CURRENT_DATE
			AND trend = 'bullish'
		ORDER BY predicted_change_pct DESC
		LIMIT $1
	`
	return db.queryPredictions(ctx, query, limit)
}

// GetPredictedLosers returns ML-predicted top losers
func (db *DB) GetPredictedLosers(ctx context.Context, limit int) ([]PredictedMover, error) {
	query := `
		SELECT
			symbol, current_price, predicted_price, predicted_change_pct,
			stop_loss, target, confidence, trend,
			COALESCE(reasoning, ''), COALESCE(technical_summary, '')
		FROM predictions.daily_predictions
		WHERE prediction_date = CURRENT_DATE
			AND trend = 'bearish'
		ORDER BY predicted_change_pct ASC
		LIMIT $1
	`
	return db.queryPredictions(ctx, query, limit)
}

func (db *DB) queryPredictions(ctx context.Context, query string, limit int) ([]PredictedMover, error) {
	rows, err := db.conn.QueryContext(ctx, query, limit)
	if err != nil {
		// Table might not exist yet
		return []PredictedMover{}, nil
	}
	defer rows.Close()

	var results []PredictedMover
	for rows.Next() {
		var p PredictedMover
		if err := rows.Scan(
			&p.Symbol, &p.CurrentPrice, &p.PredictedPrice, &p.PredictedChangePct,
			&p.StopLoss, &p.Target, &p.Confidence, &p.Trend,
			&p.Reasoning, &p.TechnicalSummary,
		); err != nil {
			continue
		}
		results = append(results, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	if results == nil {
		results = []PredictedMover{}
	}
	return results, nil
}
