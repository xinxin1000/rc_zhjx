package store

import (
	"sort"
	"sync"
	"time"
)

type DeliveryStatus string

const (
	StatusPending   DeliveryStatus = "pending"
	StatusSucceeded DeliveryStatus = "succeeded"
	StatusRetrying  DeliveryStatus = "retrying"
	StatusDead      DeliveryStatus = "dead"
)

type DeliveryRecord struct {
	ID           string         `json:"id"`
	EventKey     string         `json:"event_key"`
	TargetURL    string         `json:"target_url"`
	RequestBody  string         `json:"request_body"`
	ResponseBody string         `json:"response_body,omitempty"`
	ResponseCode any            `json:"response_code,omitempty"`
	Extracted    map[string]any `json:"extracted,omitempty"`
	HTTPStatus   int            `json:"http_status,omitempty"`
	Attempt      int            `json:"attempt"`
	Status       DeliveryStatus `json:"status"`
	Error        string         `json:"error,omitempty"`
	Degradation  string         `json:"degradation,omitempty"` // 降级状态: "" 正常, "light" 轻度, "heavy" 重度
	NextRunAt    *time.Time     `json:"next_run_at,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

type RecordStore interface {
	Create(record DeliveryRecord) (DeliveryRecord, error)
	Update(record DeliveryRecord) error
	Get(id string) (DeliveryRecord, bool, error)
	List(eventKey string, limit int) ([]DeliveryRecord, error)
	DeadLetters(limit int) ([]DeliveryRecord, error)
	FindForReplay(filter ReplayFilter) ([]DeliveryRecord, error)
}

type ReplayFilter struct {
	EventKey    string
	Statuses    []DeliveryStatus
	Degradation string
	From        time.Time
	To          time.Time
	Limit       int
}

type MemoryStore struct {
	mu      sync.RWMutex
	records map[string]DeliveryRecord
	order   []string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: map[string]DeliveryRecord{}}
}

func (s *MemoryStore) Create(record DeliveryRecord) (DeliveryRecord, error) {
	now := time.Now()
	record.CreatedAt = now
	record.UpdatedAt = now
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[record.ID] = record
	s.order = append(s.order, record.ID)
	return record, nil
}

func (s *MemoryStore) Update(record DeliveryRecord) error {
	record.UpdatedAt = time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[record.ID] = record
	return nil
}

func (s *MemoryStore) Get(id string) (DeliveryRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.records[id]
	return record, ok, nil
}

func (s *MemoryStore) List(eventKey string, limit int) ([]DeliveryRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []DeliveryRecord
	for i := len(s.order) - 1; i >= 0; i-- {
		record := s.records[s.order[i]]
		if eventKey != "" && record.EventKey != eventKey {
			continue
		}
		result = append(result, record)
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (s *MemoryStore) DeadLetters(limit int) ([]DeliveryRecord, error) {
	records, err := s.List("", 0)
	if err != nil {
		return nil, err
	}
	result := make([]DeliveryRecord, 0)
	for _, record := range records {
		if record.Status == StatusDead {
			result = append(result, record)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].UpdatedAt.After(result[j].UpdatedAt)
	})
	if limit > 0 && len(result) > limit {
		return result[:limit], nil
	}
	return result, nil
}

func (s *MemoryStore) FindForReplay(filter ReplayFilter) ([]DeliveryRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	statuses := make(map[DeliveryStatus]struct{}, len(filter.Statuses))
	for _, status := range filter.Statuses {
		statuses[status] = struct{}{}
	}

	result := make([]DeliveryRecord, 0)
	for i := len(s.order) - 1; i >= 0; i-- {
		record := s.records[s.order[i]]
		if filter.EventKey != "" && record.EventKey != filter.EventKey {
			continue
		}
		if len(statuses) > 0 {
			if _, ok := statuses[record.Status]; !ok {
				continue
			}
		}
		if filter.Degradation != "" && record.Degradation != filter.Degradation {
			continue
		}
		if !filter.From.IsZero() && record.UpdatedAt.Before(filter.From) {
			continue
		}
		if !filter.To.IsZero() && record.UpdatedAt.After(filter.To) {
			continue
		}
		result = append(result, record)
		if filter.Limit > 0 && len(result) >= filter.Limit {
			break
		}
	}
	return result, nil
}
