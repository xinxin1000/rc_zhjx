package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type TargetConfig struct {
	EventKey      string            `json:"event_key"`
	Name          string            `json:"name"`
	Method        string            `json:"method"`
	URL           string            `json:"url"`
	Headers       map[string]string `json:"headers"`
	BodyTemplate  any               `json:"body_template"`
	ExtractFields map[string]string `json:"extract_fields"`
	DispatchMode  string            `json:"dispatch_mode"`
	TimeoutMS     int               `json:"timeout_ms"`
}

type Provider interface {
	Get(eventKey string) (TargetConfig, bool)
	List() []TargetConfig
}

type FileLoader struct {
	path    string
	mu      sync.RWMutex
	configs map[string]TargetConfig
	modTime time.Time
}

func NewFileLoader(path string) *FileLoader {
	return &FileLoader{path: path, configs: map[string]TargetConfig{}}
}

func (l *FileLoader) Load() error {
	data, err := os.ReadFile(l.path)
	if err != nil {
		return err
	}

	var configs []TargetConfig
	if err := json.Unmarshal(data, &configs); err != nil {
		return err
	}

	next := make(map[string]TargetConfig, len(configs))
	for _, cfg := range configs {
		cfg.EventKey = strings.TrimSpace(cfg.EventKey)
		cfg.Method = strings.ToUpper(strings.TrimSpace(cfg.Method))
		if cfg.Method == "" {
			cfg.Method = "POST"
		}
		if cfg.TimeoutMS == 0 {
			cfg.TimeoutMS = 3000
		}
		cfg.DispatchMode = strings.ToLower(strings.TrimSpace(cfg.DispatchMode))
		if cfg.DispatchMode == "" {
			cfg.DispatchMode = "queue"
		}
		if err := validate(cfg); err != nil {
			return err
		}
		next[cfg.EventKey] = cfg
	}

	info, _ := os.Stat(l.path)
	l.mu.Lock()
	l.configs = next
	if info != nil {
		l.modTime = info.ModTime()
	}
	l.mu.Unlock()
	return nil
}

func (l *FileLoader) StartAutoReload(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			info, err := os.Stat(l.path)
			if err != nil {
				continue
			}
			l.mu.RLock()
			unchanged := !info.ModTime().After(l.modTime)
			l.mu.RUnlock()
			if unchanged {
				continue
			}
			_ = l.Load()
		}
	}()
}

func (l *FileLoader) Get(eventKey string) (TargetConfig, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	cfg, ok := l.configs[eventKey]
	return cfg, ok
}

func (l *FileLoader) List() []TargetConfig {
	l.mu.RLock()
	defer l.mu.RUnlock()
	configs := make([]TargetConfig, 0, len(l.configs))
	for _, cfg := range l.configs {
		configs = append(configs, cfg)
	}
	return configs
}

func validate(cfg TargetConfig) error {
	if cfg.EventKey == "" {
		return errors.New("event_key is required")
	}
	if cfg.URL == "" {
		return fmt.Errorf("%s url is required", cfg.EventKey)
	}
	if cfg.Method != "GET" && cfg.Method != "POST" && cfg.Method != "PUT" && cfg.Method != "PATCH" && cfg.Method != "DELETE" {
		return fmt.Errorf("%s unsupported method %q", cfg.EventKey, cfg.Method)
	}
	if cfg.DispatchMode != "queue" && cfg.DispatchMode != "direct" {
		return fmt.Errorf("%s unsupported dispatch_mode %q", cfg.EventKey, cfg.DispatchMode)
	}
	return nil
}
