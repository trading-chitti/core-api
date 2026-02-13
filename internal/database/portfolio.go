package database

import (
	"context"
	"fmt"
)

// PortfolioStats represents portfolio performance statistics
type PortfolioStats struct {
	ActiveSignals   int     `json:"activeSignals"`
	AvgConfidence   float64 `json:"avgConfidence"`
	WinRate         float64 `json:"winRate"`
	TotalTrades     int     `json:"totalTrades"`
	WinningTrades   int     `json:"winningTrades"`
	BestSignalToday string  `json:"bestSignalToday"`
}

// GetPortfolioStats derives portfolio statistics from signal history
func (db *DB) GetPortfolioStats(ctx context.Context) (*PortfolioStats, error) {
	stats := &PortfolioStats{}

	// Get overall statistics (30-day win rate + active signals)
	err := db.conn.QueryRowContext(ctx, `
		WITH overall_stats AS (
			SELECT
				ROUND(COALESCE(SUM(successful_signals)::DECIMAL / NULLIF(SUM(successful_signals + failed_signals), 0)::DECIMAL * 100, 0)::NUMERIC, 2) as win_rate,
				COALESCE(SUM(total_signals), 0) as total_trades,
				COALESCE(SUM(successful_signals), 0) as winning_trades
			FROM intraday.daily_signal_performance
			WHERE trade_date >= CURRENT_DATE - INTERVAL '30 days'
		),
		active_stats AS (
			SELECT
				COUNT(*) as active_count,
				ROUND(AVG(confidence_score)::NUMERIC * 100, 2) as avg_confidence
			FROM intraday.signals
			WHERE status IN ('ACTIVE', 'TRAILING_STOP')
			AND generated_at >= CURRENT_DATE
		),
		best_signal AS (
			SELECT symbol
			FROM intraday.signals
			WHERE status IN ('ACTIVE', 'TRAILING_STOP')
			AND generated_at >= CURRENT_DATE
			ORDER BY confidence_score DESC
			LIMIT 1
		)
		SELECT
			COALESCE(a.active_count, 0) as active_signals,
			COALESCE(a.avg_confidence, 0) as avg_confidence,
			COALESCE(o.win_rate, 0) as win_rate,
			COALESCE(o.total_trades, 0) as total_trades,
			COALESCE(o.winning_trades, 0) as winning_trades,
			COALESCE(b.symbol, '') as best_signal
		FROM overall_stats o
		CROSS JOIN active_stats a
		LEFT JOIN best_signal b ON true
	`).Scan(&stats.ActiveSignals, &stats.AvgConfidence, &stats.WinRate, &stats.TotalTrades, &stats.WinningTrades, &stats.BestSignalToday)

	if err != nil {
		return nil, fmt.Errorf("failed to get portfolio stats: %w", err)
	}

	return stats, nil
}
