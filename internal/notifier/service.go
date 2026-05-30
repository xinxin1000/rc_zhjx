package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"rc_notify_hertz/internal/config"
	"rc_notify_hertz/internal/store"
	tpl "rc_notify_hertz/internal/template"
)

type ServiceOptions struct {
	WorkerCount       int
	ImmediateAttempts int
	MaxAttempts       int
	BaseBackoff       time.Duration
	MaxBackoff        time.Duration
}

type DeliveryQueue interface {
	Enqueue(ctx context.Context, recordID string) error
	Start(ctx context.Context, handler func(context.Context, string) error)
}

type ReplayRequest struct {
	EventKey     string
	Statuses     []store.DeliveryStatus
	Degradation  string
	From         time.Time
	To           time.Time
	Limit        int
	ResetAttempt bool
	DryRun       bool
}

type ReplayResult struct {
	Matched    int      `json:"matched"`
	Enqueued   int      `json:"enqueued"`
	DryRun     bool     `json:"dry_run"`
	RecordIDs  []string `json:"record_ids"`
	SkippedIDs []string `json:"skipped_ids,omitempty"`
}

type DegradationLevel int

const (
	DegradationNone  DegradationLevel = iota // 正常
	DegradationLight                         // 轻度降级：压力下，缩短重试策略
	DegradationHeavy                         // 重度降级：下游故障，直接快速失败
)

type Service struct {
	configs config.Provider
	store   store.RecordStore
	queue   DeliveryQueue
	client  *http.Client
	opts    ServiceOptions

	// 降级信号
	downstreamHealthy atomic.Bool // 下游是否健康，true=健康，false=故障
	underPressure     atomic.Bool // 自身压力大，true=压力大
}

func NewService(configs config.Provider, records store.RecordStore, deliveryQueue DeliveryQueue, opts ServiceOptions) *Service {
	if opts.WorkerCount <= 0 {
		opts.WorkerCount = 1
	}
	if opts.ImmediateAttempts <= 0 {
		opts.ImmediateAttempts = 3
	}
	if opts.MaxAttempts < opts.ImmediateAttempts {
		opts.MaxAttempts = opts.ImmediateAttempts
	}
	if opts.BaseBackoff <= 0 {
		opts.BaseBackoff = time.Second
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = time.Minute
	}
	svc := &Service{
		configs: configs,
		store:   records,
		queue:   deliveryQueue,
		client:  &http.Client{},
		opts:    opts,
	}
	// 默认下游健康
	svc.downstreamHealthy.Store(true)
	return svc
}

func (s *Service) Start() {
	for i := 0; i < s.opts.WorkerCount; i++ {
		go s.queue.Start(context.Background(), s.handleQueuedRecord)
	}
}

// SetDownstreamHealthy 设置下游服务健康状态
// healthy=false 触发重度降级，直接快速失败
func (s *Service) SetDownstreamHealthy(healthy bool) {
	s.downstreamHealthy.Store(healthy)
}

// SetUnderPressure 设置自身压力状态
// pressure=true 触发轻度降级，缩短重试策略
func (s *Service) SetUnderPressure(pressure bool) {
	s.underPressure.Store(pressure)
}

// GetDegradationLevel 获取当前降级级别
func (s *Service) GetDegradationLevel() DegradationLevel {
	if !s.downstreamHealthy.Load() {
		return DegradationHeavy
	}
	if s.underPressure.Load() {
		return DegradationLight
	}
	return DegradationNone
}

func (s *Service) Submit(eventKey string, payload map[string]any) (store.DeliveryRecord, error) {
	cfg, ok := s.configs.Get(eventKey)
	if !ok {
		return store.DeliveryRecord{}, fmt.Errorf("event config %q not found", eventKey)
	}
	body, err := tpl.RenderJSON(cfg.BodyTemplate, payload)
	if err != nil {
		return store.DeliveryRecord{}, err
	}

	record := store.DeliveryRecord{
		ID:          strconv.FormatInt(time.Now().UnixNano(), 36),
		EventKey:    eventKey,
		TargetURL:   cfg.URL,
		RequestBody: string(body),
		Extracted:   tpl.ExtractFields(payload, cfg.ExtractFields),
		Status:      store.StatusPending,
	}
	record, err = s.store.Create(record)
	if err != nil {
		return store.DeliveryRecord{}, err
	}
	if cfg.DispatchMode == "direct" {
		s.deliverDirect(record)
		latest, ok, err := s.store.Get(record.ID)
		if err != nil {
			return store.DeliveryRecord{}, err
		}
		if ok {
			return latest, nil
		}
		return record, nil
	}
	if err := s.enqueue(context.Background(), record.ID); err != nil {
		return store.DeliveryRecord{}, err
	}
	return record, nil
}

func (s *Service) Replay(req ReplayRequest) (ReplayResult, error) {
	filter := store.ReplayFilter{
		EventKey:    req.EventKey,
		Statuses:    req.Statuses,
		Degradation: req.Degradation,
		From:        req.From,
		To:          req.To,
		Limit:       req.Limit,
	}
	records, err := s.store.FindForReplay(filter)
	if err != nil {
		return ReplayResult{}, err
	}

	result := ReplayResult{
		Matched:   len(records),
		DryRun:    req.DryRun,
		RecordIDs: make([]string, 0, len(records)),
	}
	for _, record := range records {
		result.RecordIDs = append(result.RecordIDs, record.ID)
		if req.DryRun {
			continue
		}

		if req.ResetAttempt {
			record.Attempt = 0
		}
		record.Status = store.StatusPending
		record.Error = ""
		record.NextRunAt = nil
		record.Degradation = ""
		if err := s.store.Update(record); err != nil {
			return ReplayResult{}, err
		}
		if err := s.enqueue(context.Background(), record.ID); err != nil {
			return ReplayResult{}, err
		}
		result.Enqueued++
	}
	return result, nil
}

func (s *Service) enqueue(ctx context.Context, id string) error {
	return s.queue.Enqueue(ctx, id)
}

func (s *Service) handleQueuedRecord(ctx context.Context, id string) error {
	record, ok, err := s.store.Get(id)
	if err != nil || !ok {
		return err
	}
	if record.Status == store.StatusSucceeded || record.Status == store.StatusDead {
		return nil
	}
	if record.NextRunAt != nil {
		wait := time.Until(*record.NextRunAt)
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return s.deliverQueued(ctx, record)
}

func (s *Service) deliverDirect(record store.DeliveryRecord) {
	cfg, ok := s.configs.Get(record.EventKey)
	if !ok {
		s.markDead(record, fmt.Errorf("config %q removed", record.EventKey), true)
		return
	}

	// 重度降级：下游故障，直接快速失败
	if !s.downstreamHealthy.Load() {
		record.Degradation = "heavy"
		s.markDead(record, fmt.Errorf("downstream circuit open"), false)
		return
	}

	immediateAttempts := s.opts.ImmediateAttempts
	if s.underPressure.Load() {
		record.Degradation = "light"
		// Direct mode normally retries a few times before falling back to Kafka.
		// Under pressure, keep the request path short and let Kafka absorb retry load.
		immediateAttempts = 1
	}

	var lastErr error
	for record.Attempt < immediateAttempts {
		err := s.attemptDelivery(cfg, &record)
		if err == nil {
			s.markSucceeded(record)
			return
		}
		lastErr = err
	}
	record.Status = store.StatusRetrying
	record.Error = lastErr.Error()
	nextRunAt := time.Now().Add(s.backoff(record.Attempt))
	record.NextRunAt = &nextRunAt
	if err := s.store.Update(record); err != nil {
		log.Printf("update delivery record failed id=%s error=%v", record.ID, err)
		return
	}
	if err := s.enqueue(context.Background(), record.ID); err != nil {
		log.Printf("enqueue kafka delivery message failed id=%s error=%v", record.ID, err)
	}
}

func (s *Service) deliverQueued(ctx context.Context, record store.DeliveryRecord) error {
	cfg, ok := s.configs.Get(record.EventKey)
	if !ok {
		s.markDead(record, fmt.Errorf("config %q removed", record.EventKey), true)
		return nil
	}
	if !s.downstreamHealthy.Load() {
		record.Degradation = "heavy"
		s.markDead(record, fmt.Errorf("downstream circuit open"), false)
		return nil
	}
	if s.underPressure.Load() {
		record.Degradation = "light"
	}
	if err := s.attemptDelivery(cfg, &record); err == nil {
		s.markSucceeded(record)
		return nil
	} else {
		record.Error = err.Error()
	}
	if record.Attempt >= s.opts.MaxAttempts || (record.Degradation == "light" && record.Attempt >= s.opts.ImmediateAttempts) {
		s.markDead(record, errors.New(record.Error), record.Degradation == "")
		return nil
	}
	record.Status = store.StatusRetrying
	nextRunAt := time.Now().Add(s.backoff(record.Attempt))
	record.NextRunAt = &nextRunAt
	if err := s.store.Update(record); err != nil {
		return err
	}
	return s.enqueue(ctx, record.ID)
}

func (s *Service) attemptDelivery(cfg config.TargetConfig, record *store.DeliveryRecord) error {
	record.Attempt++
	responseBody, httpStatus, responseCode, err := s.callTarget(cfg, record.RequestBody)
	record.ResponseBody = responseBody
	record.HTTPStatus = httpStatus
	record.ResponseCode = responseCode
	return err
}

func (s *Service) markSucceeded(record store.DeliveryRecord) {
	record.Status = store.StatusSucceeded
	record.Error = ""
	record.NextRunAt = nil
	if err := s.store.Update(record); err != nil {
		log.Printf("update delivery record failed id=%s error=%v", record.ID, err)
	}
}

func (s *Service) callTarget(cfg config.TargetConfig, requestBody string) (string, int, any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutMS)*time.Millisecond)
	defer cancel()

	var body io.Reader
	if cfg.Method != http.MethodGet && cfg.Method != http.MethodDelete {
		body = bytes.NewBufferString(requestBody)
	}
	req, err := http.NewRequestWithContext(ctx, cfg.Method, cfg.URL, body)
	if err != nil {
		return "", 0, nil, err
	}
	for key, value := range cfg.Headers {
		req.Header.Set(key, value)
	}
	if req.Header.Get("Content-Type") == "" && body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return "", 0, nil, err
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	responseCode := extractResponseCode(respBytes)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return string(respBytes), resp.StatusCode, responseCode, fmt.Errorf("http status %d", resp.StatusCode)
	}
	if code, ok := responseCode.(float64); ok && code != 0 {
		return string(respBytes), resp.StatusCode, responseCode, fmt.Errorf("business code %v", responseCode)
	}
	return string(respBytes), resp.StatusCode, responseCode, nil
}

func (s *Service) backoff(attempt int) time.Duration {
	exp := math.Pow(2, float64(attempt-s.opts.ImmediateAttempts))
	delay := time.Duration(exp) * s.opts.BaseBackoff
	if delay > s.opts.MaxBackoff {
		return s.opts.MaxBackoff
	}
	return delay
}

func (s *Service) markDead(record store.DeliveryRecord, err error, sendAlert bool) {
	if err == nil {
		err = errors.New("delivery failed")
	}
	record.Status = store.StatusDead
	record.Error = err.Error()
	record.NextRunAt = nil
	if updateErr := s.store.Update(record); updateErr != nil {
		log.Printf("update delivery record failed id=%s error=%v", record.ID, updateErr)
	}
	if sendAlert {
		sendWechatAlert(record)
	}
}

func extractResponseCode(body []byte) any {
	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil {
		return nil
	}
	return object["code"]
}

func sendWechatAlert(record store.DeliveryRecord) {
	log.Printf("[wechat-alert] delivery dead letter id=%s event=%s target=%s error=%s", record.ID, record.EventKey, record.TargetURL, record.Error)
}
