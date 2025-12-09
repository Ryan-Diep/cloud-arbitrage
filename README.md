# Crypto Arbitrage Monitor

Real-time cryptocurrency arbitrage detection system across Coinbase and Binance exchanges.

## Architecture

```
Go Producer → Kafka → Java Consumer → REST API → React Dashboard
```

## Prerequisites

- Go 1.21+
- Java 17+
- Node.js 18+
- Docker & Docker Compose
- Maven

## Quick Start

### 1. Start Kafka

```bash
docker-compose up -d
```

### 2. Setup Coinbase API Keys

Create `.env` file in root:

```env
COINBASE_API_KEY=your_api_key
COINBASE_SECRET_KEY=-----BEGIN EC PRIVATE KEY-----
your_secret_key
-----END EC PRIVATE KEY-----
```

### 3. Start Producer (Go)

```bash
cd producer
go mod download
go run main.go
```

### 4. Start Consumer (Java)

```bash
cd consumer
mvn clean install
mvn spring-boot:run
```

### 5. Start Web Dashboard (React)

```bash
cd webapp
npm install
npm start
```

Visit `http://localhost:3000` to see the dashboard!

## Features

- ✅ Auto-discovery of common trading pairs across exchanges
- ✅ Real-time price streaming via WebSocket
- ✅ Arbitrage detection with >1% spread threshold
- ✅ Beautiful web dashboard with live updates
- ✅ Filters out stale data and sub-penny differences
- ✅ Quote currency matching (USD-USD, USDT-USDT, etc.)

## API Endpoints

- `GET http://localhost:8080/api/opportunities` - Returns current arbitrage opportunities

## Configuration

### Producer (Go)
- `MaxSymbols`: 500 (limit tracked symbols)
- `RefreshInterval`: 6 hours (symbol list refresh)

### Consumer (Java)
- `MIN_SPREAD_THRESHOLD`: 1.0% (minimum profitable spread)
- `STALE_DATA_MS`: 3000ms (data freshness window)

## Troubleshooting

**No opportunities showing?**
- Check if both producer and consumer are running
- Verify Kafka is running: `docker ps`
- Check Java consumer logs for Kafka connection

**Web dashboard not connecting?**
- Ensure Java consumer is running on port 8080
- Check CORS settings in ArbitrageController

**Producer not connecting?**
- Verify Coinbase API credentials in `.env`
- Check network connectivity to exchanges

## License

MIT
