package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	pb "github.com/Ryan-Diep/cloud-arbitrage/producer/pkg/model"
)

const (
	KafkaTopic      = "crypto-prices"
	KafkaBroker     = "localhost:9092"
	CoinbaseWS      = "wss://advanced-trade-ws.coinbase.com"
	CoinbaseAPI     = "https://api.exchange.coinbase.com/products"
	BinanceBase     = "wss://stream.binance.com:9443/stream?streams="
	BinanceAPI      = "https://api.binance.com/api/v3/exchangeInfo"
	RefreshInterval = 6 * time.Hour
)

var stablecoins = map[string]bool{
	"USD":  true,
	"USDT": true,
	"USDC": true,
	"DAI":  true,
}

type SymbolManager struct {
	mu                sync.RWMutex
	commonSymbols     []string
	crossQuoteSymbols map[string][]string
	coinbaseSymbols   map[string]bool
	binanceSymbols    map[string]bool
}

func NewSymbolManager() *SymbolManager {
	return &SymbolManager{
		coinbaseSymbols:   make(map[string]bool),
		binanceSymbols:    make(map[string]bool),
		crossQuoteSymbols: make(map[string][]string),
	}
}

func (sm *SymbolManager) GetCommonSymbols() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.commonSymbols
}

func (sm *SymbolManager) GetCrossQuoteSymbols() map[string][]string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	result := make(map[string][]string)
	for k, v := range sm.crossQuoteSymbols {
		result[k] = v
	}
	return result
}

func (sm *SymbolManager) UpdateSymbols() error {
	coinbaseSyms, err := fetchCoinbaseSymbols()
	if err != nil {
		return fmt.Errorf("coinbase symbols: %w", err)
	}

	binanceSyms, err := fetchBinanceSymbols()
	if err != nil {
		return fmt.Errorf("binance symbols: %w", err)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.coinbaseSymbols = make(map[string]bool)
	for _, s := range coinbaseSyms {
		sm.coinbaseSymbols[s] = true
	}

	sm.binanceSymbols = make(map[string]bool)
	for _, s := range binanceSyms {
		sm.binanceSymbols[s] = true
	}

	var common []string
	for sym := range sm.coinbaseSymbols {
		if sm.binanceSymbols[sym] {
			common = append(common, sym)
		}
	}
	sm.commonSymbols = common

	baseAssetPairs := make(map[string][]string)

	allSymbols := make(map[string]bool)
	for sym := range sm.coinbaseSymbols {
		allSymbols[sym] = true
	}
	for sym := range sm.binanceSymbols {
		allSymbols[sym] = true
	}

	for sym := range allSymbols {
		parts := strings.Split(sym, "-")
		if len(parts) != 2 {
			continue
		}
		base := parts[0]
		quote := parts[1]

		if stablecoins[quote] {
			baseAssetPairs[base] = append(baseAssetPairs[base], sym)
		}
	}

	crossQuote := make(map[string][]string)
	for base, pairs := range baseAssetPairs {
		if len(pairs) < 2 {
			continue
		}

		hasOnBoth := false
		for _, pair := range pairs {
			if sm.coinbaseSymbols[pair] && sm.binanceSymbols[pair] {
				continue
			}
			onCoinbase := sm.coinbaseSymbols[pair]
			onBinance := sm.binanceSymbols[pair]

			for _, otherPair := range pairs {
				if pair == otherPair {
					continue
				}
				otherOnCoinbase := sm.coinbaseSymbols[otherPair]
				otherOnBinance := sm.binanceSymbols[otherPair]

				if (onCoinbase && otherOnBinance) || (onBinance && otherOnCoinbase) {
					hasOnBoth = true
					break
				}
			}
		}

		if hasOnBoth {
			crossQuote[base] = pairs
		}
	}

	sm.crossQuoteSymbols = crossQuote

	log.Printf("Symbol Discovery: %d Coinbase, %d Binance, %d Exact Matches, %d Cross-Quote Bases",
		len(sm.coinbaseSymbols), len(sm.binanceSymbols), len(common), len(crossQuote))

	return nil
}

func fetchCoinbaseSymbols() ([]string, error) {
	resp, err := http.Get(CoinbaseAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var products []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&products); err != nil {
		return nil, err
	}

	var symbols []string
	for _, p := range products {
		if p.Status == "online" {
			symbols = append(symbols, p.ID)
		}
	}

	return symbols, nil
}

func fetchBinanceSymbols() ([]string, error) {
	resp, err := http.Get(BinanceAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var exchangeInfo struct {
		Symbols []struct {
			Symbol     string `json:"symbol"`
			Status     string `json:"status"`
			BaseAsset  string `json:"baseAsset"`
			QuoteAsset string `json:"quoteAsset"`
		} `json:"symbols"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&exchangeInfo); err != nil {
		return nil, err
	}

	var symbols []string
	for _, s := range exchangeInfo.Symbols {
		if s.Status == "TRADING" && stablecoins[s.QuoteAsset] {
			standardSymbol := s.BaseAsset + "-" + s.QuoteAsset
			symbols = append(symbols, standardSymbol)
		}
	}

	return symbols, nil
}

func main() {
	if err := godotenv.Load("../.env"); err != nil {
		if err := godotenv.Load(); err != nil {
			log.Println("Note: No .env file found.")
		}
	}

	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP(KafkaBroker),
		Topic:        KafkaTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		RequiredAcks: kafka.RequireOne,
		Compression:  kafka.Snappy,
	}
	defer kafkaWriter.Close()

	symbolMgr := NewSymbolManager()

	log.Println("Discovering common symbols across exchanges...")
	if err := symbolMgr.UpdateSymbols(); err != nil {
		log.Fatalf("Failed to discover symbols: %v", err)
	}

	commonSymbols := symbolMgr.GetCommonSymbols()
	crossQuoteSymbols := symbolMgr.GetCrossQuoteSymbols()

	if len(commonSymbols) == 0 && len(crossQuoteSymbols) == 0 {
		log.Fatal("No common or cross-quote symbols found between exchanges")
	}

	log.Printf("Tracking %d exact match symbols", len(commonSymbols))
	log.Printf("Tracking %d cross-quote base assets (e.g., BTC-USDT vs BTC-USDC)", len(crossQuoteSymbols))

	if len(commonSymbols) > 0 {
		log.Printf("Example exact matches: %v", commonSymbols[:min(3, len(commonSymbols))])
	}
	if len(crossQuoteSymbols) > 0 {
		count := 0
		for base, pairs := range crossQuoteSymbols {
			if count >= 3 {
				break
			}
			log.Printf("Example cross-quote: %s -> %v", base, pairs)
			count++
		}
	}

	go func() {
		ticker := time.NewTicker(RefreshInterval)
		defer ticker.Stop()
		for range ticker.C {
			log.Println("Refreshing symbol list...")
			if err := symbolMgr.UpdateSymbols(); err != nil {
				log.Printf("Symbol refresh error: %v", err)
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		startCoinbase(kafkaWriter, symbolMgr)
	}()

	go func() {
		defer wg.Done()
		startBinance(kafkaWriter, symbolMgr)
	}()

	log.Printf("Engine Started. Tracking common + cross-quote symbols on 2 exchanges...")

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		var lastMessages int64

		for range ticker.C {
			stats := kafkaWriter.Stats()
			currentMessages := stats.Messages
			rate := float64(currentMessages-lastMessages) / 10.0
			lastMessages = currentMessages

			log.Printf("Kafka Throughput: %.1f msg/sec | Total: %d messages | Errors: %d | Batches: %d",
				rate, currentMessages, stats.Errors, stats.Writes)
		}
	}()

	wg.Wait()
}

func startBinance(kw *kafka.Writer, sm *SymbolManager) {
	for {
		allSymbols := make(map[string]bool)

		for _, s := range sm.GetCommonSymbols() {
			allSymbols[s] = true
		}

		for _, pairs := range sm.GetCrossQuoteSymbols() {
			for _, pair := range pairs {
				allSymbols[pair] = true
			}
		}

		if len(allSymbols) == 0 {
			time.Sleep(5 * time.Second)
			continue
		}

		var streams []string
		for s := range allSymbols {
			parts := strings.Split(s, "-")
			if len(parts) != 2 {
				continue
			}
			binanceSymbol := strings.ToLower(parts[0] + parts[1])
			streams = append(streams, binanceSymbol+"@ticker")
		}

		maxStreamsPerConn := 200
		streamChunks := chunkStreams(streams, maxStreamsPerConn)

		var wg sync.WaitGroup
		for i, chunk := range streamChunks {
			wg.Add(1)
			go func(chunkIdx int, streams []string) {
				defer wg.Done()
				connectBinanceStream(kw, streams, chunkIdx)
			}(i, chunk)
		}

		wg.Wait()
		time.Sleep(5 * time.Second)
	}
}

func connectBinanceStream(kw *kafka.Writer, streams []string, connIdx int) {
	url := BinanceBase + strings.Join(streams, "/")

	for {
		log.Printf("Binance[%d]: Connecting with %d streams...", connIdx, len(streams))

		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			log.Printf("Binance[%d] Connect Error: %v. Retrying in 5s...", connIdx, err)
			time.Sleep(5 * time.Second)
			continue
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			return nil
		})

		pingTicker := time.NewTicker(20 * time.Second)
		stopPing := make(chan struct{})

		go func() {
			for {
				select {
				case <-pingTicker.C:
					if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
						return
					}
				case <-stopPing:
					return
				}
			}
		}()

		log.Printf("Binance[%d]: Connected", connIdx)

		func() {
			defer conn.Close()
			defer pingTicker.Stop()
			defer close(stopPing)

			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					log.Printf("Binance[%d]: Connection lost. Reconnecting...", connIdx)
					return
				}

				var streamMsg BinanceStreamMessage
				if err := json.Unmarshal(message, &streamMsg); err != nil {
					continue
				}

				data := streamMsg.Data
				if data.LastPrice == "" {
					continue
				}

				priceStr := string(data.LastPrice)
				price, err := strconv.ParseFloat(priceStr, 64)
				if err != nil {
					continue
				}

				rawSymbol := strings.ToUpper(data.Symbol)

				var baseAsset, quoteAsset string
				if strings.HasSuffix(rawSymbol, "USDT") {
					quoteAsset = "USDT"
					baseAsset = strings.TrimSuffix(rawSymbol, "USDT")
				} else if strings.HasSuffix(rawSymbol, "USDC") {
					quoteAsset = "USDC"
					baseAsset = strings.TrimSuffix(rawSymbol, "USDC")
				} else if strings.HasSuffix(rawSymbol, "USD") {
					quoteAsset = "USD"
					baseAsset = strings.TrimSuffix(rawSymbol, "USD")
				} else if strings.HasSuffix(rawSymbol, "DAI") {
					quoteAsset = "DAI"
					baseAsset = strings.TrimSuffix(rawSymbol, "DAI")
				} else {
					continue
				}

				standardSymbol := baseAsset + "-" + quoteAsset

				protoTicker := &pb.Ticker{
					Source:    "BINANCE",
					Symbol:    standardSymbol,
					Price:     price,
					Timestamp: time.Now().UnixMilli(),
				}

				sendToKafka(kw, protoTicker)
			}
		}()

		time.Sleep(2 * time.Second)
	}
}

func startCoinbase(kw *kafka.Writer, sm *SymbolManager) {
	apiKeyName := os.Getenv("COINBASE_API_KEY")
	apiSecret := os.Getenv("COINBASE_SECRET_KEY")

	for {
		allSymbols := make(map[string]bool)

		for _, s := range sm.GetCommonSymbols() {
			allSymbols[s] = true
		}

		for _, pairs := range sm.GetCrossQuoteSymbols() {
			for _, pair := range pairs {
				allSymbols[pair] = true
			}
		}

		if len(allSymbols) == 0 {
			time.Sleep(5 * time.Second)
			continue
		}

		var symbols []string
		for s := range allSymbols {
			symbols = append(symbols, s)
		}

		log.Printf("Coinbase: Connecting...")

		tokenString, err := buildJWT(apiKeyName, apiSecret)
		if err != nil {
			log.Printf("Coinbase JWT Error: %v. Retrying in 5s...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		conn, _, err := websocket.DefaultDialer.Dial(CoinbaseWS, nil)
		if err != nil {
			log.Printf("Coinbase Connect Error: %v. Retrying in 5s...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		maxSymbolsPerSub := 200
		symbolChunks := chunkSymbols(symbols, maxSymbolsPerSub)

		for _, chunk := range symbolChunks {
			subscribeMsg := map[string]interface{}{
				"type":        "subscribe",
				"channel":     "ticker",
				"product_ids": chunk,
				"jwt":         tokenString,
			}
			if err := conn.WriteJSON(subscribeMsg); err != nil {
				log.Printf("Coinbase Subscribe Error: %v", err)
				conn.Close()
				time.Sleep(5 * time.Second)
				continue
			}
		}

		log.Printf("Coinbase: Subscribed to %d symbols", len(symbols))

		func() {
			defer conn.Close()

			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					log.Printf("Coinbase: Connection lost. Reconnecting...")
					return
				}

				var response CoinbaseResponse
				if err := json.Unmarshal(message, &response); err != nil {
					continue
				}

				for _, event := range response.Events {
					for _, tick := range event.Tickers {
						price, _ := strconv.ParseFloat(tick.Price, 64)

						protoTicker := &pb.Ticker{
							Source:    "COINBASE",
							Symbol:    tick.ProductID,
							Price:     price,
							Timestamp: time.Now().UnixMilli(),
						}

						sendToKafka(kw, protoTicker)
					}
				}
			}
		}()

		time.Sleep(2 * time.Second)
	}
}

func sendToKafka(kw *kafka.Writer, t *pb.Ticker) {
	data, err := proto.Marshal(t)
	if err != nil {
		log.Printf("Failed to marshal ticker: %v", err)
		return
	}

	err = kw.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(t.Symbol),
			Value: data,
		},
	)

	if err != nil {
		log.Printf("Failed to write to Kafka: %v", err)
		return
	}
}

func buildJWT(keyName, secretKey string) (string, error) {
	block, _ := pem.Decode([]byte(secretKey))
	if block == nil {
		return "", fmt.Errorf("bad pem")
	}
	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return "", err
	}

	claims := jwt.MapClaims{
		"iss": "cdp",
		"nbf": time.Now().Unix(),
		"exp": time.Now().Add(2 * time.Minute).Unix(),
		"sub": keyName,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = keyName
	token.Header["nonce"] = fmt.Sprintf("%d", time.Now().UnixNano())
	return token.SignedString(key)
}

func chunkStreams(streams []string, chunkSize int) [][]string {
	var chunks [][]string
	for i := 0; i < len(streams); i += chunkSize {
		end := i + chunkSize
		if end > len(streams) {
			end = len(streams)
		}
		chunks = append(chunks, streams[i:end])
	}
	return chunks
}

func chunkSymbols(symbols []string, chunkSize int) [][]string {
	var chunks [][]string
	for i := 0; i < len(symbols); i += chunkSize {
		end := i + chunkSize
		if end > len(symbols) {
			end = len(symbols)
		}
		chunks = append(chunks, symbols[i:end])
	}
	return chunks
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type BinanceStreamMessage struct {
	Stream string        `json:"stream"`
	Data   BinanceTicker `json:"data"`
}

type BinanceTicker struct {
	EventType          string      `json:"e"`
	EventTime          int64       `json:"E"`
	Symbol             string      `json:"s"`
	PriceChange        json.Number `json:"p"`
	PriceChangePercent json.Number `json:"P"`
	WeightedAvgPrice   json.Number `json:"w"`
	LastPrice          json.Number `json:"c"`
	LastQty            json.Number `json:"Q"`
	OpenPrice          json.Number `json:"o"`
	HighPrice          json.Number `json:"h"`
	LowPrice           json.Number `json:"l"`
	BaseVolume         json.Number `json:"v"`
	QuoteVolume        json.Number `json:"q"`
	OpenTime           int64       `json:"O"`
	CloseTime          int64       `json:"C"`
	FirstTradeID       int64       `json:"F"`
	LastTradeID        int64       `json:"L"`
	TotalTrades        int64       `json:"n"`
}

type CoinbaseResponse struct {
	Channel string          `json:"channel"`
	Events  []CoinbaseEvent `json:"events"`
}

type CoinbaseEvent struct {
	Tickers []CoinbaseTicker `json:"tickers"`
}

type CoinbaseTicker struct {
	ProductID string `json:"product_id"`
	Price     string `json:"price"`
}
