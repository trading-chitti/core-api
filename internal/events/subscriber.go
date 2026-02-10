package events

import (
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/trading-chitti/core-api-go/internal/websocket"
)

// Subscriber subscribes to NATS events and broadcasts to WebSocket clients
type Subscriber struct {
	nc  *nats.Conn
	hub *websocket.Hub
}

// SignalEvent represents a signal event from NATS
type SignalEvent struct {
	EventType   string    `json:"event_type"`
	SignalID    int       `json:"signal_id"`
	Symbol      string    `json:"symbol"`
	SignalType  string    `json:"signal_type"`
	EntryPrice  float64   `json:"entry_price"`
	StopLoss    float64   `json:"stop_loss"`
	TargetPrice float64   `json:"target_price"`
	Confidence  float64   `json:"confidence"`
	Status      string    `json:"status"`
	CurrentPrice float64  `json:"current_price"`
	ExitPrice   float64   `json:"exit_price"`
	PNL         float64   `json:"pnl"`
	GeneratedAt string    `json:"generated_at"`
	Timestamp   string    `json:"timestamp"`
}

// TickEvent represents a market tick event from NATS
type TickEvent struct {
	EventType string    `json:"event_type"`
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Volume    uint32    `json:"volume"`
	ChangePct float64   `json:"change_pct"`
	Timestamp string    `json:"timestamp"`
}

// NewSubscriber creates a new NATS event subscriber
func NewSubscriber(natsURL string, hub *websocket.Hub) (*Subscriber, error) {
	nc, err := nats.Connect(natsURL,
		nats.Name("core-api-go"),
		nats.Timeout(5*time.Second),
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1), // Infinite reconnects
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				log.Printf("⚠️  NATS disconnected: %v", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("✅ NATS reconnected: %s", nc.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, err
	}

	log.Printf("✅ NATS subscriber connected: %s", natsURL)
	return &Subscriber{nc: nc, hub: hub}, nil
}

// Close closes the NATS connection
func (s *Subscriber) Close() {
	if s.nc != nil {
		s.nc.Close()
		log.Println("👋 NATS subscriber disconnected")
	}
}

// Subscribe subscribes to all relevant NATS subjects
func (s *Subscriber) Subscribe() error {
	// Subscribe to new signals
	_, err := s.nc.Subscribe("signal.new", func(m *nats.Msg) {
		var event SignalEvent
		if err := json.Unmarshal(m.Data, &event); err != nil {
			log.Printf("❌ Failed to unmarshal signal.new event: %v", err)
			return
		}

		log.Printf("📥 Received signal.new: %s %s (%.2f confidence)", event.Symbol, event.SignalType, event.Confidence)

		// Broadcast to WebSocket clients
		s.hub.Broadcast(map[string]interface{}{
			"type": "signal_new",
			"data": event,
		})
	})
	if err != nil {
		return err
	}

	// Subscribe to signal updates
	_, err = s.nc.Subscribe("signal.updated", func(m *nats.Msg) {
		var event SignalEvent
		if err := json.Unmarshal(m.Data, &event); err != nil {
			log.Printf("❌ Failed to unmarshal signal.updated event: %v", err)
			return
		}

		log.Printf("📥 Received signal.updated: ID=%d Status=%s Price=%.2f", event.SignalID, event.Status, event.CurrentPrice)

		// Broadcast to WebSocket clients
		s.hub.Broadcast(map[string]interface{}{
			"type": "signal_updated",
			"data": event,
		})
	})
	if err != nil {
		return err
	}

	// Subscribe to signal closed
	_, err = s.nc.Subscribe("signal.closed", func(m *nats.Msg) {
		var event SignalEvent
		if err := json.Unmarshal(m.Data, &event); err != nil {
			log.Printf("❌ Failed to unmarshal signal.closed event: %v", err)
			return
		}

		log.Printf("📥 Received signal.closed: ID=%d Status=%s PNL=%.2f", event.SignalID, event.Status, event.PNL)

		// Broadcast to WebSocket clients
		s.hub.Broadcast(map[string]interface{}{
			"type": "signal_closed",
			"data": event,
		})
	})
	if err != nil {
		return err
	}

	// Subscribe to market ticks
	_, err = s.nc.Subscribe("market.tick", func(m *nats.Msg) {
		var event TickEvent
		if err := json.Unmarshal(m.Data, &event); err != nil {
			log.Printf("❌ Failed to unmarshal market.tick event: %v", err)
			return
		}

		// Only broadcast every 5 seconds to avoid overwhelming clients
		// (ticks are high frequency)
		// In production, you'd add throttling logic here

		// Broadcast to WebSocket clients
		s.hub.Broadcast(map[string]interface{}{
			"type": "market_tick",
			"data": event,
		})
	})
	if err != nil {
		return err
	}

	log.Println("✅ Subscribed to NATS subjects: signal.*, market.tick")
	return nil
}
