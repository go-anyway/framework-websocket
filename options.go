// Copyright 2025 zampo.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// @contact  zampo3380@gmail.com

package websocket

import (
	"fmt"
	"time"

	pkgConfig "github.com/go-anyway/framework-config"
)

// Config WebSocket 配置结构体（用于从配置文件创建）
type Config struct {
	Enabled          bool               `yaml:"enabled" env:"WEBSOCKET_ENABLED" default:"true"`
	ReadBufferSize   int                `yaml:"read_buffer_size" env:"WEBSOCKET_READ_BUFFER_SIZE" default:"4096"`
	WriteBufferSize  int                `yaml:"write_buffer_size" env:"WEBSOCKET_WRITE_BUFFER_SIZE" default:"4096"`
	HandshakeTimeout pkgConfig.Duration `yaml:"handshake_timeout" env:"WEBSOCKET_HANDSHAKE_TIMEOUT" default:"10s"`
	PingPeriod       pkgConfig.Duration `yaml:"ping_period" env:"WEBSOCKET_PING_PERIOD" default:"54s"`
	PongWait         pkgConfig.Duration `yaml:"pong_wait" env:"WEBSOCKET_PONG_WAIT" default:"60s"`
	WriteWait        pkgConfig.Duration `yaml:"write_wait" env:"WEBSOCKET_WRITE_WAIT" default:"10s"`
	MaxMessageSize   int64              `yaml:"max_message_size" env:"WEBSOCKET_MAX_MESSAGE_SIZE" default:"524288"` // 512KB
	EnableTrace      bool               `yaml:"enable_trace" env:"WEBSOCKET_ENABLE_TRACE" default:"true"`
}

// Validate 验证 WebSocket 配置
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("websocket config cannot be nil")
	}
	if !c.Enabled {
		return nil // 如果未启用，不需要验证
	}
	if c.ReadBufferSize < 1 {
		return fmt.Errorf("websocket read_buffer_size must be greater than 0, got %d", c.ReadBufferSize)
	}
	if c.WriteBufferSize < 1 {
		return fmt.Errorf("websocket write_buffer_size must be greater than 0, got %d", c.WriteBufferSize)
	}
	if c.MaxMessageSize < 1 {
		return fmt.Errorf("websocket max_message_size must be greater than 0, got %d", c.MaxMessageSize)
	}
	// 验证超时时间
	if c.HandshakeTimeout.Duration() <= 0 {
		return fmt.Errorf("websocket handshake_timeout must be greater than 0")
	}
	if c.PingPeriod.Duration() <= 0 {
		return fmt.Errorf("websocket ping_period must be greater than 0")
	}
	if c.PongWait.Duration() <= 0 {
		return fmt.Errorf("websocket pong_wait must be greater than 0")
	}
	if c.WriteWait.Duration() <= 0 {
		return fmt.Errorf("websocket write_wait must be greater than 0")
	}
	// PongWait 应该大于 PingPeriod
	if c.PongWait.Duration() <= c.PingPeriod.Duration() {
		return fmt.Errorf("websocket pong_wait must be greater than ping_period")
	}
	return nil
}

// ToOptions 转换为 Options
func (c *Config) ToOptions() (*Options, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if !c.Enabled {
		return nil, fmt.Errorf("websocket is not enabled")
	}

	handshakeTimeout := c.HandshakeTimeout.Duration()
	if handshakeTimeout == 0 {
		handshakeTimeout = 10 * time.Second
	}
	pingPeriod := c.PingPeriod.Duration()
	if pingPeriod == 0 {
		pingPeriod = 54 * time.Second
	}
	pongWait := c.PongWait.Duration()
	if pongWait == 0 {
		pongWait = 60 * time.Second
	}
	writeWait := c.WriteWait.Duration()
	if writeWait == 0 {
		writeWait = 10 * time.Second
	}

	return &Options{
		ReadBufferSize:   c.ReadBufferSize,
		WriteBufferSize:  c.WriteBufferSize,
		HandshakeTimeout: handshakeTimeout,
		PingPeriod:       pingPeriod,
		PongWait:         pongWait,
		WriteWait:        writeWait,
		MaxMessageSize:   c.MaxMessageSize,
		EnableTrace:      c.EnableTrace,
	}, nil
}

// HandshakeTimeoutDuration 返回 time.Duration 类型的 HandshakeTimeout
func (c *Config) HandshakeTimeoutDuration() time.Duration {
	return c.HandshakeTimeout.Duration()
}

// PingPeriodDuration 返回 time.Duration 类型的 PingPeriod
func (c *Config) PingPeriodDuration() time.Duration {
	return c.PingPeriod.Duration()
}

// PongWaitDuration 返回 time.Duration 类型的 PongWait
func (c *Config) PongWaitDuration() time.Duration {
	return c.PongWait.Duration()
}

// WriteWaitDuration 返回 time.Duration 类型的 WriteWait
func (c *Config) WriteWaitDuration() time.Duration {
	return c.WriteWait.Duration()
}

// Options WebSocket 服务器配置选项（内部使用）
type Options struct {
	ReadBufferSize   int                      // 读缓冲区大小
	WriteBufferSize  int                      // 写缓冲区大小
	HandshakeTimeout time.Duration            // 握手超时时间
	CheckOrigin      func(r interface{}) bool // 跨域检查函数（实际类型为 *http.Request，使用 interface{} 避免循环依赖）
	PingPeriod       time.Duration            // 心跳检测周期
	PongWait         time.Duration            // Pong 等待时间
	WriteWait        time.Duration            // 写等待时间
	MaxMessageSize   int64                    // 最大消息大小
	EnableTrace      bool                     // 是否启用追踪
}
