package database

import (
	"context"
	"fmt"
)

// TopMover represents a top gainer or loser stock
type TopMover struct {
	Symbol     string  `json:"symbol"`
	Name       string  `json:"name"`
	Change     float64 `json:"change"`
	Confidence float64 `json:"confidence"`
	Price      float64 `json:"price"`
}

// RealtimePrice represents a stock's current market price
type RealtimePrice struct {
	Symbol        string   `json:"symbol"`
	LastPrice     float64  `json:"last_price"`
	Volume        *int64   `json:"volume"`
	Open          float64  `json:"open"`
	High          float64  `json:"high"`
	Low           float64  `json:"low"`
	Close         float64  `json:"close"`
	ChangePercent *float64 `json:"change_percent"`
	UpdatedAt     string   `json:"updated_at"`
}

// StockData represents detailed stock information
type StockData struct {
	Symbol        string  `json:"symbol"`
	Name          string  `json:"name"`
	Price         float64 `json:"price"`
	Change        float64 `json:"change"`
	ChangePercent float64 `json:"changePercent"`
	Volume        int64   `json:"volume"`
	MarketCap     float64 `json:"marketCap"`
}

// StockSearchResult represents a search result
type StockSearchResult struct {
	Symbol   string `json:"symbol"`
	Name     string `json:"name"`
	Exchange string `json:"exchange"`
}

// GetTopGainers returns top gaining stocks by change percentage
func (db *DB) GetTopGainers(ctx context.Context, limit int) ([]TopMover, error) {
	query := `
		SELECT
			rp.symbol,
			COALESCE(sc.name, rp.symbol) as name,
			COALESCE(rp.change_percent, 0) as change,
			0.7 as confidence,
			COALESCE(rp.last_price, 0) as price
		FROM md.realtime_prices rp
		LEFT JOIN md.stock_config sc ON sc.symbol = rp.symbol AND sc.exchange = COALESCE(rp.exchange, 'NSE')
		WHERE rp.change_percent IS NOT NULL AND rp.change_percent > 0
			AND rp.updated_at > NOW() - INTERVAL '1 day'
			AND rp.symbol IS NOT NULL
		ORDER BY rp.change_percent DESC
		LIMIT $1
	`
	rows, err := db.conn.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query top gainers: %w", err)
	}
	defer rows.Close()

	var results []TopMover
	for rows.Next() {
		var m TopMover
		if err := rows.Scan(&m.Symbol, &m.Name, &m.Change, &m.Confidence, &m.Price); err != nil {
			return nil, fmt.Errorf("failed to scan top gainer: %w", err)
		}
		results = append(results, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return results, nil
}

// GetTopLosers returns top losing stocks by change percentage
func (db *DB) GetTopLosers(ctx context.Context, limit int) ([]TopMover, error) {
	query := `
		SELECT
			rp.symbol,
			COALESCE(sc.name, rp.symbol) as name,
			COALESCE(rp.change_percent, 0) as change,
			0.7 as confidence,
			COALESCE(rp.last_price, 0) as price
		FROM md.realtime_prices rp
		LEFT JOIN md.stock_config sc ON sc.symbol = rp.symbol AND sc.exchange = COALESCE(rp.exchange, 'NSE')
		WHERE rp.change_percent IS NOT NULL AND rp.change_percent < 0
			AND rp.updated_at > NOW() - INTERVAL '1 day'
			AND rp.symbol IS NOT NULL
		ORDER BY rp.change_percent ASC
		LIMIT $1
	`
	rows, err := db.conn.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query top losers: %w", err)
	}
	defer rows.Close()

	var results []TopMover
	for rows.Next() {
		var m TopMover
		if err := rows.Scan(&m.Symbol, &m.Name, &m.Change, &m.Confidence, &m.Price); err != nil {
			return nil, fmt.Errorf("failed to scan top loser: %w", err)
		}
		results = append(results, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return results, nil
}

// GetRealtimePrices returns latest prices for multiple stocks
func (db *DB) GetRealtimePrices(ctx context.Context, limit int) ([]RealtimePrice, error) {
	query := `
		SELECT
			symbol,
			COALESCE(last_price, 0),
			volume,
			COALESCE(open, 0),
			COALESCE(high, 0),
			COALESCE(low, 0),
			COALESCE(close, 0),
			change_percent,
			COALESCE(updated_at::text, '')
		FROM md.realtime_prices
		WHERE symbol IS NOT NULL
			AND updated_at > NOW() - INTERVAL '1 day'
		ORDER BY updated_at DESC
		LIMIT $1
	`
	rows, err := db.conn.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query realtime prices: %w", err)
	}
	defer rows.Close()

	var results []RealtimePrice
	for rows.Next() {
		var p RealtimePrice
		if err := rows.Scan(&p.Symbol, &p.LastPrice, &p.Volume, &p.Open, &p.High, &p.Low, &p.Close, &p.ChangePercent, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan realtime price: %w", err)
		}
		results = append(results, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return results, nil
}

// GetRealtimePrice returns the latest price for a single stock
func (db *DB) GetRealtimePrice(ctx context.Context, symbol string) (*RealtimePrice, error) {
	query := `
		SELECT
			symbol,
			COALESCE(last_price, 0),
			volume,
			COALESCE(open, 0),
			COALESCE(high, 0),
			COALESCE(low, 0),
			COALESCE(close, 0),
			change_percent,
			COALESCE(updated_at::text, '')
		FROM md.realtime_prices
		WHERE symbol = $1
		LIMIT 1
	`
	var p RealtimePrice
	err := db.conn.QueryRowContext(ctx, query, symbol).Scan(
		&p.Symbol, &p.LastPrice, &p.Volume, &p.Open, &p.High, &p.Low, &p.Close, &p.ChangePercent, &p.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get realtime price for %s: %w", symbol, err)
	}
	return &p, nil
}

// GetStockData returns detailed stock data
func (db *DB) GetStockData(ctx context.Context, symbol string) (*StockData, error) {
	query := `
		SELECT
			sc.symbol,
			COALESCE(sc.name, sc.symbol) as name,
			COALESCE(rp.last_price, 0) as price,
			COALESCE(rp.last_price - rp.close, 0) as change,
			COALESCE(rp.change_percent, 0) as change_percent,
			COALESCE(rp.volume, 0) as volume,
			0 as market_cap
		FROM md.stock_config sc
		LEFT JOIN md.realtime_prices rp ON rp.symbol = sc.symbol
		WHERE sc.symbol = $1
		LIMIT 1
	`
	var s StockData
	err := db.conn.QueryRowContext(ctx, query, symbol).Scan(
		&s.Symbol, &s.Name, &s.Price, &s.Change, &s.ChangePercent, &s.Volume, &s.MarketCap,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get stock data for %s: %w", symbol, err)
	}
	return &s, nil
}

// SearchStocks searches for stocks by symbol or name
func (db *DB) SearchStocks(ctx context.Context, query string) ([]StockSearchResult, error) {
	searchQuery := `
		SELECT symbol, COALESCE(name, symbol), exchange
		FROM md.stock_config
		WHERE active = true
			AND (symbol ILIKE $1 OR name ILIKE $1)
		ORDER BY
			CASE WHEN symbol ILIKE $2 THEN 0 ELSE 1 END,
			symbol
		LIMIT 20
	`
	pattern := "%" + query + "%"
	exactPattern := query + "%"
	rows, err := db.conn.QueryContext(ctx, searchQuery, pattern, exactPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to search stocks: %w", err)
	}
	defer rows.Close()

	var results []StockSearchResult
	for rows.Next() {
		var r StockSearchResult
		if err := rows.Scan(&r.Symbol, &r.Name, &r.Exchange); err != nil {
			return nil, fmt.Errorf("failed to scan search result: %w", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return results, nil
}
