package store

import (
	"encoding/json"
	"errors"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type MySQLStore struct {
	db *gorm.DB
}

type deliveryRecordModel struct {
	ID               string `gorm:"primaryKey;size:64"`
	EventKey         string `gorm:"index;size:128;not null"`
	TargetURL        string `gorm:"type:text;not null"`
	RequestBody      string `gorm:"type:longtext"`
	ResponseBody     string `gorm:"type:longtext"`
	ResponseCodeJSON string `gorm:"type:text"`
	ExtractedJSON    string `gorm:"type:text"`
	HTTPStatus       int
	Attempt          int
	Status           string `gorm:"index;size:32;not null"`
	Error            string `gorm:"type:text"`
	Degradation      string `gorm:"size:16"` // 降级状态: "" 正常, "light" 轻度, "heavy" 重度
	NextRunAt        *time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func NewMySQLStore(dsn string) (*MySQLStore, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	if err := db.AutoMigrate(&deliveryRecordModel{}); err != nil {
		return nil, err
	}
	return &MySQLStore{db: db}, nil
}

func (s *MySQLStore) Create(record DeliveryRecord) (DeliveryRecord, error) {
	now := time.Now()
	record.CreatedAt = now
	record.UpdatedAt = now
	model, err := toModel(record)
	if err != nil {
		return DeliveryRecord{}, err
	}
	if err := s.db.Create(&model).Error; err != nil {
		return DeliveryRecord{}, err
	}
	return record, nil
}

func (s *MySQLStore) Update(record DeliveryRecord) error {
	record.UpdatedAt = time.Now()
	model, err := toModel(record)
	if err != nil {
		return err
	}
	return s.db.Save(&model).Error
}

func (s *MySQLStore) Get(id string) (DeliveryRecord, bool, error) {
	var model deliveryRecordModel
	err := s.db.First(&model, "id = ?", id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return DeliveryRecord{}, false, nil
	}
	if err != nil {
		return DeliveryRecord{}, false, err
	}
	record, err := fromModel(model)
	return record, err == nil, err
}

func (s *MySQLStore) List(eventKey string, limit int) ([]DeliveryRecord, error) {
	var models []deliveryRecordModel
	query := s.db.Order("updated_at DESC")
	if eventKey != "" {
		query = query.Where("event_key = ?", eventKey)
	}
	if limit > 0 {
		query = query.Limit(limit)
	}
	if err := query.Find(&models).Error; err != nil {
		return nil, err
	}
	return modelsToRecords(models)
}

func (s *MySQLStore) DeadLetters(limit int) ([]DeliveryRecord, error) {
	var models []deliveryRecordModel
	query := s.db.Where("status = ?", string(StatusDead)).Order("updated_at DESC")
	if limit > 0 {
		query = query.Limit(limit)
	}
	if err := query.Find(&models).Error; err != nil {
		return nil, err
	}
	return modelsToRecords(models)
}

func (s *MySQLStore) FindForReplay(filter ReplayFilter) ([]DeliveryRecord, error) {
	var models []deliveryRecordModel
	query := s.db.Order("updated_at DESC")
	if filter.EventKey != "" {
		query = query.Where("event_key = ?", filter.EventKey)
	}
	if len(filter.Statuses) > 0 {
		statuses := make([]string, 0, len(filter.Statuses))
		for _, status := range filter.Statuses {
			statuses = append(statuses, string(status))
		}
		query = query.Where("status IN ?", statuses)
	}
	if filter.Degradation != "" {
		query = query.Where("degradation = ?", filter.Degradation)
	}
	if !filter.From.IsZero() {
		query = query.Where("updated_at >= ?", filter.From)
	}
	if !filter.To.IsZero() {
		query = query.Where("updated_at <= ?", filter.To)
	}
	if filter.Limit > 0 {
		query = query.Limit(filter.Limit)
	}
	if err := query.Find(&models).Error; err != nil {
		return nil, err
	}
	return modelsToRecords(models)
}

func toModel(record DeliveryRecord) (deliveryRecordModel, error) {
	responseCodeJSON, err := marshalString(record.ResponseCode)
	if err != nil {
		return deliveryRecordModel{}, err
	}
	extractedJSON, err := marshalString(record.Extracted)
	if err != nil {
		return deliveryRecordModel{}, err
	}
	return deliveryRecordModel{
		ID:               record.ID,
		EventKey:         record.EventKey,
		TargetURL:        record.TargetURL,
		RequestBody:      record.RequestBody,
		ResponseBody:     record.ResponseBody,
		ResponseCodeJSON: responseCodeJSON,
		ExtractedJSON:    extractedJSON,
		HTTPStatus:       record.HTTPStatus,
		Attempt:          record.Attempt,
		Status:           string(record.Status),
		Error:            record.Error,
		Degradation:      record.Degradation,
		NextRunAt:        record.NextRunAt,
		CreatedAt:        record.CreatedAt,
		UpdatedAt:        record.UpdatedAt,
	}, nil
}

func fromModel(model deliveryRecordModel) (DeliveryRecord, error) {
	var responseCode any
	if err := unmarshalString(model.ResponseCodeJSON, &responseCode); err != nil {
		return DeliveryRecord{}, err
	}
	var extracted map[string]any
	if err := unmarshalString(model.ExtractedJSON, &extracted); err != nil {
		return DeliveryRecord{}, err
	}
	return DeliveryRecord{
		ID:           model.ID,
		EventKey:     model.EventKey,
		TargetURL:    model.TargetURL,
		RequestBody:  model.RequestBody,
		ResponseBody: model.ResponseBody,
		ResponseCode: responseCode,
		Extracted:    extracted,
		HTTPStatus:   model.HTTPStatus,
		Attempt:      model.Attempt,
		Status:       DeliveryStatus(model.Status),
		Error:        model.Error,
		Degradation:  model.Degradation,
		NextRunAt:    model.NextRunAt,
		CreatedAt:    model.CreatedAt,
		UpdatedAt:    model.UpdatedAt,
	}, nil
}

func modelsToRecords(models []deliveryRecordModel) ([]DeliveryRecord, error) {
	records := make([]DeliveryRecord, 0, len(models))
	for _, model := range models {
		record, err := fromModel(model)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func marshalString(value any) (string, error) {
	if value == nil {
		return "", nil
	}
	data, err := json.Marshal(value)
	return string(data), err
}

func unmarshalString(raw string, target any) error {
	if raw == "" {
		return nil
	}
	return json.Unmarshal([]byte(raw), target)
}
