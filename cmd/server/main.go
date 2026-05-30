package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"rc_notify_hertz/internal/config"
	"rc_notify_hertz/internal/httpapi"
	"rc_notify_hertz/internal/notifier"
	"rc_notify_hertz/internal/queue"
	"rc_notify_hertz/internal/store"
)

func main() {
	configPath := getenv("CONFIG_PATH", "config/providers.json")
	addr := getenv("ADDR", ":8080")

	loader := config.NewFileLoader(configPath)
	if err := loader.Load(); err != nil {
		log.Fatalf("load config: %v", err)
	}
	loader.StartAutoReload(30 * time.Second)

	recordStore, err := newRecordStore()
	if err != nil {
		log.Fatalf("init store: %v", err)
	}
	deliveryQueue, err := newDeliveryQueue()
	if err != nil {
		log.Fatalf("init delivery queue: %v", err)
	}
	service := notifier.NewService(loader, recordStore, deliveryQueue, notifier.ServiceOptions{
		WorkerCount:       2,
		ImmediateAttempts: 3,
		MaxAttempts:       8,
		BaseBackoff:       2 * time.Second,
		MaxBackoff:        30 * time.Second,
	})
	service.Start()

	router := httpapi.NewRouter(addr, loader, recordStore, service)
	router.Spin()
}

func newDeliveryQueue() (notifier.DeliveryQueue, error) {
	driver := strings.ToLower(getenv("QUEUE_DRIVER", "memory"))
	switch driver {
	case "memory", "":
		log.Printf("delivery queue: memory")
		return queue.NewMemoryQueue(128), nil
	case "kafka":
		brokers := queue.ParseBrokers(getenv("KAFKA_BROKERS", "localhost:9092"))
		topic := getenv("KAFKA_TOPIC", "notification-delivery")
		groupID := getenv("KAFKA_GROUP_ID", "rc-notify-workers")
		log.Printf("delivery queue: kafka brokers=%s topic=%s group=%s", strings.Join(brokers, ","), topic, groupID)
		return queue.NewKafkaQueue(brokers, topic, groupID), nil
	default:
		return nil, fmt.Errorf("unsupported QUEUE_DRIVER %q", driver)
	}
}

func newRecordStore() (store.RecordStore, error) {
	driver := strings.ToLower(getenv("STORE_DRIVER", "memory"))
	switch driver {
	case "memory", "":
		log.Printf("store driver: memory")
		return store.NewMemoryStore(), nil
	case "mysql":
		dsn := os.Getenv("MYSQL_DSN")
		if dsn == "" {
			return nil, fmt.Errorf("MYSQL_DSN is required when STORE_DRIVER=mysql")
		}
		log.Printf("store driver: mysql")
		return store.NewMySQLStore(dsn)
	default:
		return nil, fmt.Errorf("unsupported STORE_DRIVER %q", driver)
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
