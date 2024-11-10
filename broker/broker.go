package broker

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
)

type Broker struct {
	dataDir     string
	topics      map[string]*Topic
	mu          sync.RWMutex
	subscribers map[string][]chan []byte
}

type Topic struct {
	name          string
	segments      []*Segment
	activeSegment *Segment
	mu            sync.RWMutex
}

func NewBroker(dataDir string) *Broker {
	return &Broker{
		dataDir:     dataDir,
		topics:      make(map[string]*Topic),
		subscribers: make(map[string][]chan []byte),
	}
}

func (b *Broker) Start(port int) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to start listener: %v", err)
	}
	defer listener.Close()

	fmt.Printf("Broker listening on port %d\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}

		go b.handleConnection(conn)
	}
}

func (b *Broker) handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		cmd := scanner.Text()
		parts := strings.Split(cmd, " ")

		if len(parts) < 2 {
			fmt.Fprintf(conn, "ERROR invalid command\n")
			continue
		}

		fmt.Printf("Received command: %s\n", cmd)

		switch parts[0] {
		case "PUB":
			if len(parts) < 3 {
				fmt.Fprintf(conn, "ERROR invalid publish command\n")
				continue
			}
			topic := parts[1]
			message := []byte(strings.Join(parts[2:], " "))
			if err := b.Publish(topic, message); err != nil {
				fmt.Fprintf(conn, "ERROR %v\n", err)
			} else {
				fmt.Fprintf(conn, "OK\n")
			}

		case "SUB":
			topic := parts[1]
			ch, err := b.Subscribe(topic)
			if err != nil {
				fmt.Fprintf(conn, "ERROR %v\n", err)
				continue
			}

			for msg := range ch {
				fmt.Fprintf(conn, "%s\n", string(msg))
			}
		}
	}
}

func (b *Broker) CreateTopic(topicName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[topicName]; exists {
		return fmt.Errorf("topic %s already exists", topicName)
	}

	topic := &Topic{
		name:     topicName,
		segments: make([]*Segment, 0),
		mu:       sync.RWMutex{},
	}

	segment, err := newSegment(filepath.Join(b.dataDir, topicName), 0)
	if err != nil {
		return fmt.Errorf("failed to create segment: %v", err)
	}

	topic.activeSegment = segment
	topic.segments = append(topic.segments, segment)
	b.topics[topicName] = topic

	return nil
}

func (b *Broker) Publish(topicName string, message []byte) error {
	fmt.Printf("1 %s\n", topicName)
	b.mu.RLock()
	defer b.mu.RUnlock()
	fmt.Printf("2\n")

	topic, exists := b.topics[topicName]
	fmt.Printf("3\n")

	if !exists {
		b.mu.RUnlock()
		if err := b.CreateTopic(topicName); err != nil {
			return err
		}
		b.mu.RLock()
		topic = b.topics[topicName]
	}
	fmt.Printf("4\n")
	topic.mu.Lock()
	offset, err := topic.activeSegment.append(message)
	topic.mu.Unlock()
	fmt.Printf("5\n")
	if err != nil {
		return fmt.Errorf("failed to append message: %v", err)
	}

	subscribers := b.subscribers[topicName]
	fmt.Printf("6\n")
	fmt.Printf("Notifying %d subscribers\n", len(subscribers))

	for _, ch := range subscribers {
		select {
		case ch <- message:
		default:
		}
	}
	fmt.Printf("7\n")
	fmt.Printf("Published message to topic %s at offset %d\n", topicName, offset)
	return nil
}

func (b *Broker) Subscribe(topicName string) (<-chan []byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[topicName]; !exists {
		if err := b.CreateTopic(topicName); err != nil {
			return nil, err
		}
	}

	ch := make(chan []byte, 100)
	if b.subscribers[topicName] == nil {
		b.subscribers[topicName] = make([]chan []byte, 0)
	}
	b.subscribers[topicName] = append(b.subscribers[topicName], ch)

	return ch, nil
}
