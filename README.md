# Core API Gateway

**Thin FastAPI layer routing dashboard requests to Mojo services**

## Architecture

```
React Dashboard (port 5173)
        ↓ HTTP/SSE
Core-API Gateway (port 6001) - FastAPI Python
        ↓ Unix/TCP Sockets
┌───────┴────────┬────────────┬──────────┐
↓                ↓            ↓          ↓
mojo-compute  signal-service  news-nlp
(port 6004)   (port 6003)     (port 6002)
```

## Responsibilities

This service does **NOT** contain business logic. It only:
- ✅ Routes HTTP requests to appropriate Mojo services
- ✅ Streams SSE events from Mojo services to dashboard
- ✅ Validates requests (Pydantic schemas)
- ✅ Serializes responses (JSON)
- ✅ Handles CORS for dashboard
- ✅ Provides authentication/authorization (future)
- ✅ Exposes Prometheus metrics

## Installation

```bash
# Install dependencies
pip install -e .

# Or for development
pip install -e ".[dev]"
```

## Configuration

Environment variables (create `.env` file):

```bash
# Socket paths (Unix sockets - default)
MOJO_COMPUTE_SOCKET=/tmp/mojo-compute.sock
SIGNAL_SERVICE_SOCKET=/tmp/signal-service.sock
NEWS_NLP_SOCKET=/tmp/news-nlp.sock

# OR TCP sockets (for distributed deployment)
USE_UNIX_SOCKETS=false
MOJO_COMPUTE_HOST=localhost
MOJO_COMPUTE_PORT=6004
SIGNAL_SERVICE_HOST=localhost
SIGNAL_SERVICE_PORT=6003
NEWS_NLP_HOST=localhost
NEWS_NLP_PORT=6002

# CORS (allow dashboard origins)
CORS_ORIGINS=http://localhost:5173,http://localhost:3000

# Debug mode
DEBUG=false
```

## Running

### Development

```bash
uvicorn core_api.app:app --reload --port 6001
```

### Production

```bash
uvicorn core_api.app:app --host 0.0.0.0 --port 6001 --workers 4
```

## API Endpoints

### Health Check

```bash
GET /health
```

Returns health status of core-api and all Mojo services.

---

### Compute Endpoints (mojo-compute)

**POST /api/compute/sma**
```json
{
  "symbol": "TCS",
  "prices": [1234.5, 1240.0, 1235.5, ...],
  "period": 20
}
```

**POST /api/compute/rsi**
```json
{
  "symbol": "TCS",
  "prices": [1234.5, 1240.0, ...],
  "period": 14
}
```

**POST /api/compute/macd**
```json
{
  "symbol": "TCS",
  "prices": [1234.5, ...],
  "fast_period": 12,
  "slow_period": 26,
  "signal_period": 9
}
```

**POST /api/compute/bollinger**
```json
{
  "symbol": "TCS",
  "prices": [1234.5, ...],
  "period": 20,
  "std_dev": 2.0
}
```

---

### Signal Endpoints (signal-service)

**GET /api/signals/alerts**

Query parameters:
- `limit` (int, default 100)
- `offset` (int, default 0)
- `symbol` (string, optional)
- `sector` (string, optional)

**GET /api/signals/alerts/stream**

Server-Sent Events stream of real-time alerts.

```javascript
// Dashboard usage
const eventSource = new EventSource('/api/signals/alerts/stream');
eventSource.onmessage = (event) => {
  const alert = JSON.parse(event.data);
  console.log('New alert:', alert);
};
```

**POST /api/signals/generate**
```json
{
  "symbol": "TCS",
  "indicators": { "rsi": 45.2, "macd_line": 12.5 },
  "news_context": { "sentiment": 0.65 }
}
```

**GET /api/signals/patterns/{symbol}**

Query parameters:
- `pattern_types` (string, optional, comma-separated)

---

### News Endpoints (news-nlp)

**GET /api/news/articles**

Query parameters:
- `limit` (int, default 100)
- `offset` (int, default 0)
- `symbol` (string, optional)
- `sector` (string, optional)
- `start_date` (ISO string, optional)
- `end_date` (ISO string, optional)

**POST /api/news/analyze/sentiment**
```json
{
  "text": "TCS wins major contract worth $100M"
}
```

**POST /api/news/analyze/entities**
```json
{
  "text": "Reliance and TCS lead Nifty gains in banking sector"
}
```

**POST /api/news/analyze/direction**
```json
{
  "text": "TCS reports strong quarterly results"
}
```

**POST /api/news/ingest** (admin only)
```json
{
  "url": "https://news.google.com/rss/..."
}
```

---

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=core_api --cov-report=html

# Run specific test file
pytest tests/unit/test_clients.py
```

## Docker

```bash
# Build image
docker build -t trading-chitti/core-api .

# Run container
docker run -p 6001:6001 \
  -v /tmp:/tmp \
  trading-chitti/core-api
```

## Monitoring

Prometheus metrics available at `/metrics`

Key metrics:
- `core_api_requests_total{method, endpoint, status}` - Request count
- `core_api_request_duration_seconds{method, endpoint}` - Request latency

## Development

### Project Structure

```
core-api/
├── core_api/
│   ├── __init__.py
│   ├── app.py              # Main FastAPI app
│   ├── config.py           # Configuration
│   ├── clients/            # Socket clients to Mojo services
│   │   ├── base.py         # Base socket client
│   │   ├── mojo_compute_client.py
│   │   ├── signal_service_client.py
│   │   └── news_nlp_client.py
│   ├── routes/             # API route handlers
│   │   ├── compute.py
│   │   ├── signals.py
│   │   └── news.py
│   └── middleware/         # Custom middleware (future)
├── tests/
│   ├── unit/
│   └── integration/
├── pyproject.toml
└── README.md
```

### Adding New Endpoints

1. Add method to appropriate client (e.g., `clients/mojo_compute_client.py`)
2. Add route handler (e.g., `routes/compute.py`)
3. Add request/response Pydantic models
4. Add tests

### Code Style

```bash
# Lint
ruff check core_api/

# Format
ruff format core_api/

# Type check
mypy core_api/
```

---

## Production Checklist

- [ ] Set `DEBUG=false`
- [ ] Configure proper CORS origins
- [ ] Add authentication/authorization
- [ ] Set up rate limiting
- [ ] Configure logging (structured logs)
- [ ] Set up monitoring (Prometheus + Grafana)
- [ ] Use production ASGI server (uvicorn with workers)
- [ ] Configure reverse proxy (nginx)
- [ ] Set up SSL/TLS certificates
- [ ] Implement health check dependencies

---

## Troubleshooting

**Error: "Could not connect to Mojo service"**
- Check if Mojo service is running
- Verify socket path/port is correct
- Check Unix socket permissions (0666 for /tmp/*.sock)

**Error: "Connection refused"**
- Mojo service not started
- Wrong host/port configuration
- Firewall blocking connection

**SSE stream disconnects**
- Check nginx proxy timeout settings
- Ensure `X-Accel-Buffering: no` header is set
- Verify keep-alive connection settings

---

## Next Steps

Once Mojo services are implemented:
1. Update socket protocols if needed
2. Add authentication middleware
3. Implement rate limiting
4. Add request/response logging
5. Set up distributed tracing (OpenTelemetry)

---

**Status**: ✅ Ready for Mojo service integration

The core-api gateway is fully implemented and ready to route requests to Mojo services as soon as they are built!
