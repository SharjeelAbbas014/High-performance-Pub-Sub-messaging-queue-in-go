package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	mode := flag.String("mode", "producer", "Mode: producer or consumer")
	topic := flag.String("topic", "test-topic", "Topic name")
	flag.Parse()

	switch *mode {
	case "producer":
		runProducer(*topic)
	case "consumer":
		runConsumer(*topic)
	default:
		log.Fatalf("Invalid mode: %s", *mode)
	}
}

func runProducer(topic string) {
	fmt.Println("Running as producer. Type messages and press Enter to send. Type 'quit' to exit.")
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		message := scanner.Text()
		if message == "quit" {
			break
		}

		if err := publish(topic, []byte(message)); err != nil {
			log.Printf("Error publishing message: %v", err)
		}
	}
}

func runConsumer(topic string) {
	fmt.Printf("Running as consumer for topic: %s\n", topic)
	messages, err := subscribe(topic)
	if err != nil {
		log.Fatalf("Error subscribing: %v", err)
	}

	for msg := range messages {
		fmt.Printf("Received: %s\n", string(msg))
	}
}

func publish(topic string, message []byte) error {
	conn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = fmt.Fprintf(conn, "PUB %s %s\n", topic, message)
	return err
}

func subscribe(topic string) (<-chan []byte, error) {
	conn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		return nil, err
	}

	_, err = fmt.Fprintf(conn, "SUB %s\n", topic)
	if err != nil {
		conn.Close()
		return nil, err
	}

	messages := make(chan []byte)
	go func() {
		defer conn.Close()
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			messages <- scanner.Bytes()
		}
		close(messages)
	}()

	return messages, nil
}
