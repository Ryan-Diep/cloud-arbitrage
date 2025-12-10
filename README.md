# Crypto Arbitrage Monitor

Real-time cryptocurrency arbitrage detection system across **Coinbase, Binance, Bybit, OKX**
<img width="1633" height="1190" alt="image" src="https://github.com/user-attachments/assets/6ec9aaeb-38dd-4ee2-a2c8-a62a8b468df6" />
<img width="1635" height="1016" alt="image" src="https://github.com/user-attachments/assets/cb31d5e1-0721-46aa-bade-8f9ec70badd9" />


## Architecture

```mermaid
graph LR
    A[Go Producer] -->|High Throughput| B(Kafka)
    B --> C[Java Consumer]
    C -->|REST API| D[React Dashboard]
    E[Coinbase] --> A
    F[Binance] --> A
    G[Bybit] --> A
    H[OKX] --> A
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

### 2. Setup API Keys

Create a `.env` file in the root directory. Only Coinbase requires API keys for public ticker data.

```
COINBASE_API_KEY=your_api_key
COINBASE_SECRET_KEY=-----BEGIN EC PRIVATE KEY-----
your_secret_key
-----END EC PRIVATE KEY-----
```

### 3. Start Producer (Go)

The producer automatically discovers symbols that exist on at least **2 exchanges** and streams them.

```bash
cd producer
go mod download
go run cmd/main.go
```

### 4. Start Consumer (Java)

The consumer detects N-way arbitrage opportunities between any pair of exchanges.

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

Visit **http://localhost:3000** to see the live dashboard!

## Features

- ✅ **Multi-Exchange Support:** Tracks Coinbase, Binance, Bybit, and OKX simultaneously.
- ✅ **Smart Auto-Discovery:** Finds assets listed on ≥2 exchanges.
- ✅ **High-Frequency Streaming:** Handles 1,000+ messages/sec via Kafka.
- ✅ **N-Way Arbitrage:** Detects best BUY and SELL opportunities.
- ✅ **Live Dashboard:** Real-time price feeds, dynamic search, visual spread indicators.
- ✅ **Resilience:** Automatic reconnection logic for WebSockets and Kafka.

## API Endpoints

- `GET http://localhost:8081/api/opportunities` – Returns top arbitrage opportunities.
- `GET http://localhost:8081/api/prices` – Returns live prices for all tracked symbols.
