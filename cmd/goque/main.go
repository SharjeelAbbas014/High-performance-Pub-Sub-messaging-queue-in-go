package main

import (
	"flag"
	"log"

	"goque/broker"
)

func main() {
	port := flag.Int("port", 9092, "Port to listen on")
	dataDir := flag.String("data-dir", "./data", "Directory to store data")
	flag.Parse()

	broker := broker.NewBroker(*dataDir)
	if err := broker.Start(*port); err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}
}
