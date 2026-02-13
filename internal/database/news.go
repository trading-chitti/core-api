package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// NewsArticle represents a news article from the database
type NewsArticle struct {
	ID             string   `json:"id"`
	Title          string   `json:"title"`
	Source         string   `json:"source"`
	Time           string   `json:"time"`
	URL            *string  `json:"url"`
	Summary        *string  `json:"summary"`
	Sentiment      float64  `json:"sentiment"`
	SentimentLabel *string  `json:"sentimentLabel"`
	Impact         string   `json:"impact"`
	ImpactScore    *float64 `json:"impactScore"`
	Category       string   `json:"category"`
	AffectedStocks []string `json:"affectedStocks"`
	PriceMovement  float64  `json:"priceMovement"`
	Confidence     float64  `json:"confidence"`
}

// NewsResponse represents the paginated news response
type NewsResponse struct {
	Articles []NewsArticle `json:"articles"`
	Total    int           `json:"total"`
	Page     int           `json:"page"`
	HasMore  bool          `json:"hasMore"`
}

// GetNews retrieves paginated news articles with optional filters
func (db *DB) GetNews(ctx context.Context, limit, offset int, sentiment, search, symbol string) (*NewsResponse, error) {
	// Build WHERE clause
	conditions := []string{}
	args := []interface{}{}
	argIdx := 1

	if sentiment != "" {
		conditions = append(conditions, fmt.Sprintf("a.sentiment_label = $%d", argIdx))
		args = append(args, sentiment)
		argIdx++
	}

	if search != "" {
		conditions = append(conditions, fmt.Sprintf("(a.title ILIKE $%d OR a.summary ILIKE $%d)", argIdx, argIdx))
		args = append(args, "%"+search+"%")
		argIdx++
	}

	if symbol != "" {
		conditions = append(conditions, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM news.article_entities ae WHERE ae.article_id = a.id AND ae.symbol = $%d)", argIdx))
		args = append(args, symbol)
		argIdx++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Count total
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM news.articles a %s", whereClause)
	var total int
	if err := db.conn.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("failed to count articles: %w", err)
	}

	// Fetch articles
	query := fmt.Sprintf(`
		SELECT
			a.id,
			COALESCE(a.title, ''),
			COALESCE(a.source, 'Unknown'),
			COALESCE(a.published_at, NOW()),
			a.url,
			a.summary,
			COALESCE(a.sentiment_score, 0.5),
			a.sentiment_label
		FROM news.articles a
		%s
		ORDER BY a.published_at DESC
		LIMIT $%d OFFSET $%d
	`, whereClause, argIdx, argIdx+1)

	args = append(args, limit, offset)

	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query articles: %w", err)
	}
	defer rows.Close()

	var articles []NewsArticle
	for rows.Next() {
		var a NewsArticle
		var publishedAt time.Time
		var llmSentiment sql.NullString

		if err := rows.Scan(
			&a.ID, &a.Title, &a.Source, &publishedAt, &a.URL, &a.Summary,
			&a.Confidence, &llmSentiment,
		); err != nil {
			return nil, fmt.Errorf("failed to scan article: %w", err)
		}

		a.Time = publishedAt.Format(time.RFC3339)

		// Map sentiment
		if llmSentiment.Valid {
			label := llmSentiment.String
			a.SentimentLabel = &label
			switch strings.ToLower(label) {
			case "positive":
				a.Sentiment = a.Confidence
				a.Impact = "high"
			case "negative":
				a.Sentiment = -a.Confidence
				a.Impact = "high"
			default:
				a.Sentiment = 0
				a.Impact = "low"
			}
		} else {
			a.Sentiment = 0
			a.Impact = "low"
		}
		a.Category = "market"
		a.PriceMovement = 0
		a.AffectedStocks = []string{}

		articles = append(articles, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	// Fetch affected stocks for all articles
	if len(articles) > 0 {
		articleIDs := make([]string, len(articles))
		articleMap := make(map[string]int)
		for i, a := range articles {
			articleIDs[i] = a.ID
			articleMap[a.ID] = i
		}

		entityQuery := `
			SELECT article_id, symbol
			FROM news.article_entities
			WHERE article_id = ANY($1)
		`
		entityRows, err := db.conn.QueryContext(ctx, entityQuery, articleIDs)
		if err == nil {
			defer entityRows.Close()
			for entityRows.Next() {
				var articleID, sym string
				if err := entityRows.Scan(&articleID, &sym); err == nil {
					if idx, ok := articleMap[articleID]; ok {
						articles[idx].AffectedStocks = append(articles[idx].AffectedStocks, sym)
					}
				}
			}
		}
	}

	// Ensure non-nil articles
	if articles == nil {
		articles = []NewsArticle{}
	}

	page := (offset / limit) + 1
	return &NewsResponse{
		Articles: articles,
		Total:    total,
		Page:     page,
		HasMore:  offset+limit < total,
	}, nil
}
