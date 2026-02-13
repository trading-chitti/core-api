package database

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// StockConfig represents a stock configuration entry
type StockConfig struct {
	Symbol           string  `json:"symbol"`
	Exchange         string  `json:"exchange"`
	Name             *string `json:"name"`
	Sector           *string `json:"sector"`
	MarketCapCat     *string `json:"market_cap_category"`
	IntradayEnabled  bool    `json:"intraday_enabled"`
	InvestmentEnabled bool   `json:"investment_enabled"`
	Fetcher          *string `json:"fetcher"`
	Active           bool    `json:"active"`
	CreatedAt        string  `json:"created_at"`
	UpdatedAt        string  `json:"updated_at"`
	IntradayAIPicked *bool   `json:"intraday_ai_picked,omitempty"`
	SelectionType    *string `json:"selection_type,omitempty"`
}

// StockConfigResponse represents a paginated stock config response
type StockConfigResponse struct {
	Stocks []StockConfig `json:"stocks"`
	Total  int           `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

// StockConfigStats represents aggregate stock config statistics
type StockConfigStats struct {
	TotalStocks          int            `json:"total_stocks"`
	IntradayEnabledCount int            `json:"intraday_enabled_count"`
	InvestmentEnabledCount int          `json:"investment_enabled_count"`
	FetcherDistribution  map[string]int `json:"fetcher_distribution"`
	MarketDistribution   map[string]int `json:"market_distribution"`
}

// StockConfigFilters represents the query filters for stock configs
type StockConfigFilters struct {
	Limit             int
	Offset            int
	Symbol            string
	Name              string
	Sector            string
	Exchange          string
	IntradayEnabled   *bool
	InvestmentEnabled *bool
	MarketCapCategory string
	Fetcher           string
	Active            *bool
	SelectionType     string
}

// GetStockConfigs retrieves paginated stock configurations with filters
func (db *DB) GetStockConfigs(ctx context.Context, f StockConfigFilters) (*StockConfigResponse, error) {
	conditions := []string{}
	args := []interface{}{}
	argIdx := 1

	if f.Symbol != "" {
		conditions = append(conditions, fmt.Sprintf("symbol ILIKE $%d", argIdx))
		args = append(args, "%"+f.Symbol+"%")
		argIdx++
	}
	if f.Name != "" {
		conditions = append(conditions, fmt.Sprintf("name ILIKE $%d", argIdx))
		args = append(args, "%"+f.Name+"%")
		argIdx++
	}
	if f.Sector != "" {
		conditions = append(conditions, fmt.Sprintf("sector = $%d", argIdx))
		args = append(args, f.Sector)
		argIdx++
	}
	if f.Exchange != "" {
		conditions = append(conditions, fmt.Sprintf("exchange = $%d", argIdx))
		args = append(args, f.Exchange)
		argIdx++
	}
	if f.IntradayEnabled != nil {
		conditions = append(conditions, fmt.Sprintf("intraday_enabled = $%d", argIdx))
		args = append(args, *f.IntradayEnabled)
		argIdx++
	}
	if f.InvestmentEnabled != nil {
		conditions = append(conditions, fmt.Sprintf("investment_enabled = $%d", argIdx))
		args = append(args, *f.InvestmentEnabled)
		argIdx++
	}
	if f.MarketCapCategory != "" {
		conditions = append(conditions, fmt.Sprintf("market_cap_category = $%d", argIdx))
		args = append(args, f.MarketCapCategory)
		argIdx++
	}
	if f.Fetcher != "" {
		conditions = append(conditions, fmt.Sprintf("fetcher = $%d", argIdx))
		args = append(args, f.Fetcher)
		argIdx++
	}
	if f.Active != nil {
		conditions = append(conditions, fmt.Sprintf("active = $%d", argIdx))
		args = append(args, *f.Active)
		argIdx++
	}
	if f.SelectionType != "" {
		conditions = append(conditions, fmt.Sprintf("selection_type = $%d", argIdx))
		args = append(args, f.SelectionType)
		argIdx++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Count total
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM md.stock_config %s", whereClause)
	var total int
	if err := db.conn.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("failed to count stock configs: %w", err)
	}

	// Fetch records
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT
			symbol, exchange, name, sector, market_cap_category,
			intraday_enabled, investment_enabled, fetcher, active,
			created_at, updated_at, intraday_ai_picked, selection_type
		FROM md.stock_config
		%s
		ORDER BY symbol ASC
		LIMIT $%d OFFSET $%d
	`, whereClause, argIdx, argIdx+1)

	args = append(args, limit, f.Offset)
	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query stock configs: %w", err)
	}
	defer rows.Close()

	var stocks []StockConfig
	for rows.Next() {
		var s StockConfig
		var createdAt, updatedAt time.Time
		if err := rows.Scan(
			&s.Symbol, &s.Exchange, &s.Name, &s.Sector, &s.MarketCapCat,
			&s.IntradayEnabled, &s.InvestmentEnabled, &s.Fetcher, &s.Active,
			&createdAt, &updatedAt, &s.IntradayAIPicked, &s.SelectionType,
		); err != nil {
			return nil, fmt.Errorf("failed to scan stock config: %w", err)
		}
		s.CreatedAt = createdAt.Format(time.RFC3339)
		s.UpdatedAt = updatedAt.Format(time.RFC3339)
		stocks = append(stocks, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	if stocks == nil {
		stocks = []StockConfig{}
	}

	return &StockConfigResponse{
		Stocks: stocks,
		Total:  total,
		Limit:  limit,
		Offset: f.Offset,
	}, nil
}

// UpdateStockConfig updates a stock's configuration
func (db *DB) UpdateStockConfig(ctx context.Context, symbol, exchange string, updates map[string]interface{}) error {
	// Whitelist of allowed column names to prevent SQL injection
	allowedColumns := map[string]bool{
		"active":             true,
		"intraday_enabled":   true,
		"investment_enabled": true,
		"fetcher":            true,
		"market_cap_category": true,
		"sector":             true,
		"name":               true,
		"intraday_ai_picked": true,
		"selection_type":     true,
	}

	setClauses := []string{}
	args := []interface{}{}
	argIdx := 1

	for key, value := range updates {
		if !allowedColumns[key] {
			return fmt.Errorf("invalid column name: %s", key)
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", key, argIdx))
		args = append(args, value)
		argIdx++
	}

	if len(setClauses) == 0 {
		return nil
	}

	query := fmt.Sprintf(
		"UPDATE md.stock_config SET %s WHERE symbol = $%d AND exchange = $%d",
		strings.Join(setClauses, ", "), argIdx, argIdx+1,
	)
	args = append(args, symbol, exchange)

	result, err := db.conn.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to update stock config: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("stock config not found: %s/%s", symbol, exchange)
	}

	return nil
}

// GetStockConfigStats returns aggregate statistics
func (db *DB) GetStockConfigStats(ctx context.Context) (*StockConfigStats, error) {
	stats := &StockConfigStats{
		FetcherDistribution: make(map[string]int),
		MarketDistribution:  make(map[string]int),
	}

	// Basic counts
	err := db.conn.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE intraday_enabled = true),
			COUNT(*) FILTER (WHERE investment_enabled = true)
		FROM md.stock_config
		WHERE active = true
	`).Scan(&stats.TotalStocks, &stats.IntradayEnabledCount, &stats.InvestmentEnabledCount)
	if err != nil {
		return nil, fmt.Errorf("failed to get stock config stats: %w", err)
	}

	// Fetcher distribution
	fetcherRows, err := db.conn.QueryContext(ctx, `
		SELECT COALESCE(fetcher, 'null'), COUNT(*)
		FROM md.stock_config
		WHERE active = true
		GROUP BY fetcher
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get fetcher distribution: %w", err)
	}
	defer fetcherRows.Close()
	for fetcherRows.Next() {
		var fetcher string
		var count int
		if err := fetcherRows.Scan(&fetcher, &count); err == nil {
			stats.FetcherDistribution[fetcher] = count
		}
	}

	// Market distribution
	marketRows, err := db.conn.QueryContext(ctx, `
		SELECT exchange, COUNT(*)
		FROM md.stock_config
		WHERE active = true
		GROUP BY exchange
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to get market distribution: %w", err)
	}
	defer marketRows.Close()
	for marketRows.Next() {
		var market string
		var count int
		if err := marketRows.Scan(&market, &count); err == nil {
			stats.MarketDistribution[market] = count
		}
	}

	return stats, nil
}

// GetImportJobStatus retrieves the status of a CSV import job
func (db *DB) GetImportJobStatus(ctx context.Context, jobID string) (map[string]interface{}, error) {
	query := `
		SELECT
			job_id, filename, total_rows, processed_rows, successful_rows,
			failed_rows, status, progress_percentage, error_message,
			started_at, completed_at, estimated_completion_at
		FROM md.csv_import_jobs
		WHERE job_id = $1
	`
	var jobIDOut, filename, status string
	var totalRows, processedRows, successfulRows, failedRows int
	var progressPct float64
	var errorMsg *string
	var startedAt time.Time
	var completedAt, estimatedAt *time.Time

	err := db.conn.QueryRowContext(ctx, query, jobID).Scan(
		&jobIDOut, &filename, &totalRows, &processedRows, &successfulRows,
		&failedRows, &status, &progressPct, &errorMsg,
		&startedAt, &completedAt, &estimatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get import job status: %w", err)
	}

	result := map[string]interface{}{
		"job_id":                jobIDOut,
		"filename":             filename,
		"total_rows":           totalRows,
		"processed_rows":       processedRows,
		"successful_rows":      successfulRows,
		"failed_rows":          failedRows,
		"status":               status,
		"progress_percentage":  progressPct,
		"error_message":        errorMsg,
		"started_at":           startedAt.Format(time.RFC3339),
		"completed_at":         nil,
		"estimated_completion_at": nil,
	}
	if completedAt != nil {
		result["completed_at"] = completedAt.Format(time.RFC3339)
	}
	if estimatedAt != nil {
		result["estimated_completion_at"] = estimatedAt.Format(time.RFC3339)
	}

	return result, nil
}

// ExportStockConfigsCSV returns stock configs as CSV string
func (db *DB) ExportStockConfigsCSV(ctx context.Context) (string, error) {
	query := `
		SELECT symbol, exchange, COALESCE(name, ''), COALESCE(sector, ''),
			COALESCE(market_cap_category, ''), intraday_enabled, investment_enabled,
			COALESCE(fetcher, ''), active
		FROM md.stock_config
		ORDER BY symbol
	`
	rows, err := db.conn.QueryContext(ctx, query)
	if err != nil {
		return "", fmt.Errorf("failed to export stock configs: %w", err)
	}
	defer rows.Close()

	var sb strings.Builder
	sb.WriteString("symbol,exchange,name,sector,market_cap_category,intraday_enabled,investment_enabled,fetcher,active\n")

	for rows.Next() {
		var symbol, exchange, name, sector, marketCap, fetcher string
		var intradayEnabled, investmentEnabled, active bool
		if err := rows.Scan(&symbol, &exchange, &name, &sector, &marketCap, &intradayEnabled, &investmentEnabled, &fetcher, &active); err != nil {
			return "", fmt.Errorf("failed to scan row: %w", err)
		}
		sb.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%v,%v,%s,%v\n",
			symbol, exchange, name, sector, marketCap, intradayEnabled, investmentEnabled, fetcher, active))
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("rows iteration error: %w", err)
	}

	return sb.String(), nil
}
