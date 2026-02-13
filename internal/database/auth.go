package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// BrokerConfig represents a row from brokers.config
type BrokerConfig struct {
	ID                  int        `json:"id"`
	BrokerName          string     `json:"broker_name"`
	Enabled             bool       `json:"enabled"`
	APIKey              string     `json:"api_key"`
	APISecret           string     `json:"api_secret"`
	AccessToken         string     `json:"access_token"`
	UserID              string     `json:"user_id"`
	TokenExpiresAt      *time.Time `json:"token_expires_at"`
	LastAuthenticatedAt *time.Time `json:"last_authenticated_at"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
}

// GetBrokerConfig retrieves the active broker config for a given broker
func (db *DB) GetBrokerConfig(ctx context.Context, brokerName string) (*BrokerConfig, error) {
	query := `
		SELECT id, broker_name, enabled,
		       COALESCE(api_key, ''), COALESCE(api_secret, ''),
		       COALESCE(access_token, ''), COALESCE(user_id, ''),
		       token_expires_at, last_authenticated_at,
		       created_at, updated_at
		FROM brokers.config
		WHERE broker_name = $1
		ORDER BY updated_at DESC
		LIMIT 1
	`

	var bc BrokerConfig
	err := db.conn.QueryRowContext(ctx, query, brokerName).Scan(
		&bc.ID, &bc.BrokerName, &bc.Enabled,
		&bc.APIKey, &bc.APISecret,
		&bc.AccessToken, &bc.UserID,
		&bc.TokenExpiresAt, &bc.LastAuthenticatedAt,
		&bc.CreatedAt, &bc.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get broker config: %w", err)
	}

	return &bc, nil
}

// UpdateBrokerToken updates the access token for a broker
func (db *DB) UpdateBrokerToken(ctx context.Context, brokerName, accessToken, userID string, expiresAt time.Time) error {
	query := `
		UPDATE brokers.config
		SET access_token = $1,
		    user_id = $2,
		    token_expires_at = $3,
		    last_authenticated_at = NOW(),
		    enabled = true
		WHERE broker_name = $4
	`

	result, err := db.conn.ExecContext(ctx, query, accessToken, userID, expiresAt, brokerName)
	if err != nil {
		return fmt.Errorf("failed to update broker token: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("no broker config found for %s", brokerName)
	}

	return nil
}

// ClearBrokerToken clears the access token and disables the broker
func (db *DB) ClearBrokerToken(ctx context.Context, brokerName string) error {
	query := `
		UPDATE brokers.config
		SET access_token = NULL,
		    enabled = false
		WHERE broker_name = $1
	`

	_, err := db.conn.ExecContext(ctx, query, brokerName)
	if err != nil {
		return fmt.Errorf("failed to clear broker token: %w", err)
	}

	return nil
}
