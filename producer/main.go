package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	// TODO: REPLACE THIS LINE below with the module name from your go.mod file + "/pkg/model"
	// Example: "github.com/ryan/cloud-arbitrage/ingester/pkg/model"
	pb "github.com/Ryan-Diep/cloud-arbitrage/producer/pkg/model"
)

// --- JSON STRUCTS (To parse Coinbase data) ---
type CoinbaseResponse struct {
	Channel   string          `json:"channel"`
	Timestamp string          `json:"timestamp"`
	Events    []CoinbaseEvent `json:"events"`
}

type CoinbaseEvent struct {
	Type      string           `json:"type"`       // "update" or "snapshot"
	ProductID string           `json:"product_id"` // "ETH-USD"
	Updates   []CoinbaseUpdate `json:"updates"`
}

type CoinbaseUpdate struct {
	Side        string `json:"side"`        // "bid" or "offer"
	PriceLevel  string `json:"price_level"` // Comes as string "3128.75"
	NewQuantity string `json:"new_quantity"`
	EventTime   string `json:"event_time"`
}

func main() {
	// 1. Load Environment Variables
	if err := godotenv.Load("../.env"); err != nil {
		if err := godotenv.Load(); err != nil {
			log.Println("Note: No .env file found. Relying on System Envs.")
		}
	}

	apiKeyName := os.Getenv("COINBASE_API_KEY")
	apiSecret := os.Getenv("COINBASE_SECRET_KEY")
	if apiKeyName == "" || apiSecret == "" {
		log.Fatal("Error: COINBASE_API_KEY or COINBASE_SECRET_KEY is empty.")
	}

	// 2. Setup Kafka Writer
	// This runs in the background and batches messages for performance
	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "crypto-prices",
		Balancer: &kafka.LeastBytes{}, // Distributes messages across partitions
	}
	defer kafkaWriter.Close()

	// 3. Connect to WebSocket
	tokenString, err := buildJWT(apiKeyName, apiSecret)
	if err != nil {
		log.Fatal("Error generating JWT:", err)
	}

	wsURL := "wss://advanced-trade-ws.coinbase.com"
	log.Printf("Connecting to %s...", wsURL)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Fatal("Dial Error:", err)
	}
	defer conn.Close()

	// 4. Subscribe
	subscribeMsg := map[string]interface{}{
		"type":        "subscribe",
		"channel":     "level2",
		"product_ids": []string{"ETH-USD"},
		"jwt":         tokenString,
	}

	if err := conn.WriteJSON(subscribeMsg); err != nil {
		log.Fatal("Write Error:", err)
	}
	log.Println("Subscribed to ETH-USD. Pushing data to Kafka...")

	// 5. The Ingestion Loop
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("Read Error:", err)
			break
		}

		// A. Parse the raw JSON
		var response CoinbaseResponse
		if err := json.Unmarshal(message, &response); err != nil {
			// Sometimes Coinbase sends control messages that aren't price updates
			// We can ignore parse errors for now to keep the stream alive
			continue
		}

		// B. Extract and Transform
		for _, event := range response.Events {
			for _, update := range event.Updates {
				// Convert string price to float
				price, err := strconv.ParseFloat(update.PriceLevel, 64)
				if err != nil {
					continue
				}

				// C. Create the Protobuf Object
				// This uses the code generated from ticker.proto
				ticker := &pb.Ticker{
					Source:    "COINBASE",
					Symbol:    event.ProductID,
					Price:     price,
					Timestamp: time.Now().UnixMilli(), // Current System Time
				}

				// D. Serialize to Binary (Protobuf)
				data, err := proto.Marshal(ticker)
				if err != nil {
					log.Printf("Protobuf Marshaling Error: %v", err)
					continue
				}

				// E. Write to Kafka
				err = kafkaWriter.WriteMessages(context.Background(),
					kafka.Message{
						Key:   []byte(event.ProductID), // "ETH-USD"
						Value: data,
					},
				)
				if err != nil {
					log.Printf("Failed to write to Kafka: %v", err)
				}
			}
		}
		// Visual feedback that it's working (prints a dot for every batch)
		fmt.Print(".")
	}
}

// --- Helper Function: JWT Generation ---
func buildJWT(keyName, secretKey string) (string, error) {
	block, _ := pem.Decode([]byte(secretKey))
	if block == nil {
		return "", fmt.Errorf("failed to parse PEM block containing the private key")
	}

	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("failed to parse EC private key: %v", err)
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
