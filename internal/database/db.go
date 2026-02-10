package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

type DB struct {
	conn *sql.DB
}

// GetConn returns the underlying database connection
func (db *DB) GetConn() *sql.DB {
	return db.conn
}

// NullRawMessage is a wrapper for json.RawMessage that handles NULL values
type NullRawMessage struct {
	RawMessage json.RawMessage
	Valid      bool
}

// Scan implements the sql.Scanner interface
func (n *NullRawMessage) Scan(value interface{}) error {
	if value == nil {
		n.Valid = false
		n.RawMessage = nil
		return nil
	}
	n.Valid = true
	n.RawMessage = value.([]byte)
	return nil
}

// MarshalJSON implements json.Marshaler
func (n NullRawMessage) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return n.RawMessage, nil
}

// Signal represents a trading signal from the database
type Signal struct {
	SignalID            string          `json:"signal_id"`
	Symbol              string          `json:"symbol"`
	SignalType          string          `json:"signal_type"`
	ConfidenceScore     float64         `json:"confidence_score"`
	EntryPrice          float64         `json:"entry_price"`
	CurrentPrice        float64         `json:"current_price"`
	StopLoss            float64         `json:"stop_loss"`
	TargetPrice         float64         `json:"target_price"`
	Status              string          `json:"status"`
	GeneratedAt         time.Time       `json:"generated_at"`
	ExitPrice           *float64        `json:"exit_price"`
	ClosedAt            *time.Time      `json:"closed_at"`
	ActualProfitPct     *float64        `json:"actual_profit_pct"`
	PredictionFeatures  NullRawMessage  `json:"prediction_features"`
	RecentNewsSentiment *float64        `json:"recent_news_sentiment"`
	Metadata            NullRawMessage  `json:"metadata"`
	ExitReason          *string         `json:"exit_reason"`
	Sector              string          `json:"sector"`
	StockName           string          `json:"stock_name"`
}

// NewDB creates a new database connection
func NewDB(dsn string) (*DB, error) {
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Set connection pool settings
	conn.SetMaxOpenConns(25)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Println("✅ Database connected")
	return &DB{conn: conn}, nil
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.conn.Close()
}

// GetActiveSignals retrieves active signals from the database
func (db *DB) GetActiveSignals(ctx context.Context) ([]Signal, error) {
	query := `
		SELECT
			signal_id, symbol, stock_name, sector, signal_type, confidence_score, entry_price, current_price,
			stop_loss, target_price, status, generated_at, exit_price, closed_at, actual_profit_pct,
			prediction_features, recent_news_sentiment, metadata, exit_reason
		FROM intraday.signals
		WHERE status = 'ACTIVE'
		ORDER BY generated_at DESC
	`

	rows, err := db.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query active signals: %w", err)
	}
	defer rows.Close()

	var signals []Signal
	for rows.Next() {
		var s Signal
		err := rows.Scan(
			&s.SignalID, &s.Symbol, &s.StockName, &s.Sector, &s.SignalType, &s.ConfidenceScore, &s.EntryPrice,
			&s.CurrentPrice, &s.StopLoss, &s.TargetPrice, &s.Status, &s.GeneratedAt,
			&s.ExitPrice, &s.ClosedAt, &s.ActualProfitPct, &s.PredictionFeatures,
			&s.RecentNewsSentiment, &s.Metadata, &s.ExitReason,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan signal: %w", err)
		}
		signals = append(signals, s)
	}

	return signals, nil
}

// GetAllSignals retrieves all signals with optional filters
func (db *DB) GetAllSignals(ctx context.Context, limit int, status string) ([]Signal, error) {
	query := `
		SELECT
			signal_id, symbol, stock_name, sector, signal_type, confidence_score, entry_price, current_price,
			stop_loss, target_price, status, generated_at, exit_price, closed_at, actual_profit_pct,
			prediction_features, recent_news_sentiment, metadata, exit_reason
		FROM intraday.signals
		WHERE 1=1
	`
	args := []interface{}{}

	if status != "" {
		query += " AND status = $1"
		args = append(args, status)
	}

	query += " ORDER BY generated_at DESC"

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", len(args)+1)
		args = append(args, limit)
	}

	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query signals: %w", err)
	}
	defer rows.Close()

	var signals []Signal
	for rows.Next() {
		var s Signal
		err := rows.Scan(
			&s.SignalID, &s.Symbol, &s.StockName, &s.Sector, &s.SignalType, &s.ConfidenceScore, &s.EntryPrice,
			&s.CurrentPrice, &s.StopLoss, &s.TargetPrice, &s.Status, &s.GeneratedAt,
			&s.ExitPrice, &s.ClosedAt, &s.ActualProfitPct, &s.PredictionFeatures,
			&s.RecentNewsSentiment, &s.Metadata, &s.ExitReason,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan signal: %w", err)
		}
		signals = append(signals, s)
	}

	return signals, nil
}

// GetSignalByID retrieves a single signal by ID
func (db *DB) GetSignalByID(ctx context.Context, signalID string) (*Signal, error) {
	query := `
		SELECT
			signal_id, symbol, stock_name, sector, signal_type, confidence_score, entry_price, current_price,
			stop_loss, target_price, status, generated_at, exit_price, closed_at, actual_profit_pct,
			prediction_features, recent_news_sentiment, metadata, exit_reason
		FROM intraday.signals
		WHERE signal_id = $1
	`

	var s Signal
	err := db.conn.QueryRowContext(ctx, query, signalID).Scan(
		&s.SignalID, &s.Symbol, &s.StockName, &s.Sector, &s.SignalType, &s.ConfidenceScore, &s.EntryPrice,
		&s.CurrentPrice, &s.StopLoss, &s.TargetPrice, &s.Status, &s.GeneratedAt,
		&s.ExitPrice, &s.ClosedAt, &s.ActualProfitPct, &s.PredictionFeatures,
		&s.RecentNewsSentiment, &s.Metadata, &s.ExitReason,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get signal: %w", err)
	}

	return &s, nil
}
