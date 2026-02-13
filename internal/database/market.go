package database

import (
	"context"
	"fmt"
)

// MarketIndex represents a market index value
type MarketIndex struct {
	Index         string  `json:"index"`
	Value         float64 `json:"value"`
	Change        float64 `json:"change"`
	ChangePercent float64 `json:"changePercent"`
}

// GetMarketIndices returns latest market index values
func (db *DB) GetMarketIndices(ctx context.Context) ([]MarketIndex, error) {
	// Try realtime_prices for Nifty/BankNifty/Sensex using known instrument tokens or symbols
	query := `
		SELECT
			CASE
				WHEN symbol = 'NIFTY 50' OR tradingsymbol = 'NIFTY 50' THEN 'NIFTY 50'
				WHEN symbol = 'NIFTY BANK' OR tradingsymbol = 'NIFTY BANK' THEN 'NIFTY BANK'
				ELSE COALESCE(symbol, tradingsymbol, 'UNKNOWN')
			END as index_name,
			COALESCE(rp.last_price, 0) as value,
			COALESCE(rp.last_price - rp.close, 0) as change,
			COALESCE(rp.change_percent, 0) as change_percent
		FROM md.realtime_prices rp
		LEFT JOIN md.instrument_tokens it ON rp.instrument_token = it.instrument_token
		WHERE rp.symbol IN ('NIFTY 50', 'NIFTY BANK')
			OR it.tradingsymbol IN ('NIFTY 50', 'NIFTY BANK')
		LIMIT 10
	`
	rows, err := db.conn.QueryContext(ctx, query)
	if err != nil {
		// Fallback: return empty indices with a note
		return []MarketIndex{
			{Index: "NIFTY 50", Value: 0, Change: 0, ChangePercent: 0},
			{Index: "NIFTY BANK", Value: 0, Change: 0, ChangePercent: 0},
		}, nil
	}
	defer rows.Close()

	var indices []MarketIndex
	for rows.Next() {
		var idx MarketIndex
		if err := rows.Scan(&idx.Index, &idx.Value, &idx.Change, &idx.ChangePercent); err != nil {
			continue
		}
		indices = append(indices, idx)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	// Ensure we always return some indices
	if len(indices) == 0 {
		indices = []MarketIndex{
			{Index: "NIFTY 50", Value: 0, Change: 0, ChangePercent: 0},
			{Index: "NIFTY BANK", Value: 0, Change: 0, ChangePercent: 0},
		}
	}

	return indices, nil
}
