package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"rc_notify_hertz/internal/config"
	"rc_notify_hertz/internal/notifier"
	"rc_notify_hertz/internal/store"
)

func NewRouter(addr string, configs config.Provider, records store.RecordStore, svc *notifier.Service) *server.Hertz {
	h := server.Default(server.WithHostPorts(addr))

	h.StaticFile("/admin/configs-page", "web/configs.html")
	h.StaticFile("/admin/records-page", "web/records.html")

	h.GET("/health", func(ctx context.Context, c *app.RequestContext) {
		c.JSON(consts.StatusOK, utils.H{"status": "ok"})
	})

	h.GET("/admin/configs", func(ctx context.Context, c *app.RequestContext) {
		c.JSON(consts.StatusOK, utils.H{"items": configs.List()})
	})

	h.GET("/admin/records", func(ctx context.Context, c *app.RequestContext) {
		limit := parseLimit(string(c.Query("limit")), 50)
		eventKey := string(c.Query("event_key"))
		items, err := records.List(eventKey, limit)
		if err != nil {
			c.JSON(consts.StatusInternalServerError, utils.H{"error": err.Error()})
			return
		}
		c.JSON(consts.StatusOK, utils.H{"items": items})
	})

	h.GET("/admin/dlq", func(ctx context.Context, c *app.RequestContext) {
		limit := parseLimit(string(c.Query("limit")), 50)
		items, err := records.DeadLetters(limit)
		if err != nil {
			c.JSON(consts.StatusInternalServerError, utils.H{"error": err.Error()})
			return
		}
		c.JSON(consts.StatusOK, utils.H{"items": items})
	})

	h.POST("/admin/records/replay", func(ctx context.Context, c *app.RequestContext) {
		var req replayRequest
		if err := json.Unmarshal(c.Request.Body(), &req); err != nil {
			c.JSON(consts.StatusBadRequest, utils.H{"error": "invalid json body"})
			return
		}
		replayReq, err := req.toServiceRequest()
		if err != nil {
			c.JSON(consts.StatusBadRequest, utils.H{"error": err.Error()})
			return
		}
		result, err := svc.Replay(replayReq)
		if err != nil {
			c.JSON(consts.StatusInternalServerError, utils.H{"error": err.Error()})
			return
		}
		c.JSON(consts.StatusOK, result)
	})

	h.POST("/api/events/:event_key/notify", func(ctx context.Context, c *app.RequestContext) {
		eventKey := c.Param("event_key")
		var payload map[string]any
		if err := json.Unmarshal(c.Request.Body(), &payload); err != nil {
			c.JSON(consts.StatusBadRequest, utils.H{"error": "invalid json body"})
			return
		}
		record, err := svc.Submit(eventKey, payload)
		if err != nil {
			c.JSON(consts.StatusBadRequest, utils.H{"error": err.Error()})
			return
		}
		c.JSON(consts.StatusAccepted, utils.H{
			"id":     record.ID,
			"status": record.Status,
		})
	})

	return h
}

type replayRequest struct {
	EventKey     string   `json:"event_key"`
	From         string   `json:"from"`
	To           string   `json:"to"`
	Statuses     []string `json:"statuses"`
	Degradation  string   `json:"degradation"`
	ResetAttempt bool     `json:"reset_attempt"`
	DryRun       bool     `json:"dry_run"`
	Limit        int      `json:"limit"`
}

func (r replayRequest) toServiceRequest() (notifier.ReplayRequest, error) {
	from, err := parseOptionalTime(r.From)
	if err != nil {
		return notifier.ReplayRequest{}, err
	}
	to, err := parseOptionalTime(r.To)
	if err != nil {
		return notifier.ReplayRequest{}, err
	}
	if !from.IsZero() && !to.IsZero() && from.After(to) {
		return notifier.ReplayRequest{}, fmt.Errorf("from must be before to")
	}
	limit := r.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	statuses, err := parseStatuses(r.Statuses)
	if err != nil {
		return notifier.ReplayRequest{}, err
	}
	return notifier.ReplayRequest{
		EventKey:     strings.TrimSpace(r.EventKey),
		From:         from,
		To:           to,
		Statuses:     statuses,
		Degradation:  strings.TrimSpace(r.Degradation),
		ResetAttempt: r.ResetAttempt,
		DryRun:       r.DryRun,
		Limit:        limit,
	}, nil
}

func parseStatuses(raw []string) ([]store.DeliveryStatus, error) {
	if len(raw) == 0 {
		return []store.DeliveryStatus{store.StatusDead}, nil
	}
	statuses := make([]store.DeliveryStatus, 0, len(raw))
	for _, item := range raw {
		switch store.DeliveryStatus(strings.TrimSpace(item)) {
		case store.StatusRetrying, store.StatusDead:
			statuses = append(statuses, store.DeliveryStatus(strings.TrimSpace(item)))
		default:
			return nil, fmt.Errorf("unsupported replay status %q", item)
		}
	}
	return statuses, nil
}

func parseOptionalTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	value, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, err
	}
	return value, nil
}

func parseLimit(raw string, fallback int) int {
	if raw == "" {
		return fallback
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 0 {
		return fallback
	}
	return limit
}
