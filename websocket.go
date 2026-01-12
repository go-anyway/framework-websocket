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
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-anyway/framework-log"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// DefaultOptions 返回默认配置选项
func DefaultOptions() *Options {
	return &Options{
		ReadBufferSize:   4096,
		WriteBufferSize:  4096,
		HandshakeTimeout: 10 * time.Second,
		CheckOrigin: func(r interface{}) bool {
			return true // 默认允许所有来源
		},
		PingPeriod:     54 * time.Second,
		PongWait:       60 * time.Second,
		WriteWait:      10 * time.Second,
		MaxMessageSize: 512 * 1024, // 512KB
		EnableTrace:    true,
	}
}

// Upgrader WebSocket 升级器
type Upgrader struct {
	upgrader *websocket.Upgrader
	opts     *Options
}

// NewUpgrader 创建新的 WebSocket 升级器
func NewUpgrader(opts *Options) *Upgrader {
	if opts == nil {
		opts = DefaultOptions()
	}

	var checkOrigin func(r *http.Request) bool
	if opts.CheckOrigin != nil {
		checkOrigin = func(r *http.Request) bool {
			return opts.CheckOrigin(r)
		}
	}

	upgrader := &websocket.Upgrader{
		ReadBufferSize:  opts.ReadBufferSize,
		WriteBufferSize: opts.WriteBufferSize,
		CheckOrigin:     checkOrigin,
	}

	return &Upgrader{
		upgrader: upgrader,
		opts:     opts,
	}
}

// Upgrade 将 HTTP 连接升级为 WebSocket 连接
func (u *Upgrader) Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (*Conn, error) {
	ws, err := u.upgrader.Upgrade(w, r, responseHeader)
	if err != nil {
		return nil, fmt.Errorf("failed to upgrade connection: %w", err)
	}

	conn := &Conn{
		conn:      ws,
		opts:      u.opts,
		send:      make(chan []byte, 256),
		rooms:     make(map[string]bool),
		closeOnce: sync.Once{},
	}

	// 设置读取限制
	ws.SetReadLimit(u.opts.MaxMessageSize)
	ws.SetReadDeadline(time.Now().Add(u.opts.PongWait))
	ws.SetPongHandler(func(string) error {
		ws.SetReadDeadline(time.Now().Add(u.opts.PongWait))
		return nil
	})

	return conn, nil
}

// Conn WebSocket 连接封装
type Conn struct {
	conn      *websocket.Conn
	opts      *Options
	send      chan []byte
	rooms     map[string]bool
	roomsMu   sync.RWMutex
	closeOnce sync.Once
	closed    bool
	closedMu  sync.RWMutex
}

// ID 返回连接 ID（用于追踪）
func (c *Conn) ID() string {
	return fmt.Sprintf("%p", c)
}

// RemoteAddr 返回远程地址
func (c *Conn) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

// ReadMessage 读取消息
func (c *Conn) ReadMessage() (messageType int, data []byte, err error) {
	return c.conn.ReadMessage()
}

// WriteMessage 写入消息
func (c *Conn) WriteMessage(messageType int, data []byte) error {
	c.closedMu.RLock()
	if c.closed {
		c.closedMu.RUnlock()
		return fmt.Errorf("connection is closed")
	}
	c.closedMu.RUnlock()

	c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteWait))
	return c.conn.WriteMessage(messageType, data)
}

// WriteJSON 写入 JSON 消息
func (c *Conn) WriteJSON(v interface{}) error {
	c.closedMu.RLock()
	if c.closed {
		c.closedMu.RUnlock()
		return fmt.Errorf("connection is closed")
	}
	c.closedMu.RUnlock()

	c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteWait))
	return c.conn.WriteJSON(v)
}

// Close 关闭连接
func (c *Conn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closedMu.Lock()
		c.closed = true
		c.closedMu.Unlock()
		close(c.send)
		err = c.conn.Close()
	})
	return err
}

// IsClosed 检查连接是否已关闭
func (c *Conn) IsClosed() bool {
	c.closedMu.RLock()
	defer c.closedMu.RUnlock()
	return c.closed
}

// JoinRoom 加入房间
func (c *Conn) JoinRoom(room string) {
	c.roomsMu.Lock()
	defer c.roomsMu.Unlock()
	c.rooms[room] = true
}

// LeaveRoom 离开房间
func (c *Conn) LeaveRoom(room string) {
	c.roomsMu.Lock()
	defer c.roomsMu.Unlock()
	delete(c.rooms, room)
}

// GetRooms 获取所有房间
func (c *Conn) GetRooms() []string {
	c.roomsMu.RLock()
	defer c.roomsMu.RUnlock()
	rooms := make([]string, 0, len(c.rooms))
	for room := range c.rooms {
		rooms = append(rooms, room)
	}
	return rooms
}

// IsInRoom 检查是否在指定房间
func (c *Conn) IsInRoom(room string) bool {
	c.roomsMu.RLock()
	defer c.roomsMu.RUnlock()
	return c.rooms[room]
}

// Hub WebSocket 连接中心
type Hub struct {
	// 注册的连接
	connections map[*Conn]bool

	// 房间映射：room -> connections
	rooms map[string]map[*Conn]bool

	// 广播消息通道
	broadcast chan []byte

	// 房间广播消息通道：room -> message
	roomBroadcast chan RoomMessage

	// 注册连接通道
	register chan *Conn

	// 注销连接通道
	unregister chan *Conn

	// 互斥锁
	mu sync.RWMutex

	// 配置选项
	opts *Options

	// 关闭标志
	closed  bool
	closeMu sync.RWMutex
}

// RoomMessage 房间消息
type RoomMessage struct {
	Room    string
	Message []byte
}

// NewHub 创建新的 Hub
func NewHub(opts *Options) *Hub {
	if opts == nil {
		opts = DefaultOptions()
	}

	return &Hub{
		connections:   make(map[*Conn]bool),
		rooms:         make(map[string]map[*Conn]bool),
		broadcast:     make(chan []byte, 256),
		roomBroadcast: make(chan RoomMessage, 256),
		register:      make(chan *Conn),
		unregister:    make(chan *Conn),
		opts:          opts,
	}
}

// Run 运行 Hub（必须在 goroutine 中运行）
func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			h.closeMu.Lock()
			h.closed = true
			h.closeMu.Unlock()
			h.closeAllConnections()
			return

		case conn := <-h.register:
			h.mu.Lock()
			h.connections[conn] = true
			h.mu.Unlock()

		case conn := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.connections[conn]; ok {
				delete(h.connections, conn)
				// 从所有房间中移除
				rooms := conn.GetRooms()
				for _, room := range rooms {
					h.removeFromRoom(room, conn)
				}
				close(conn.send)
			}
			h.mu.Unlock()

		case message := <-h.broadcast:
			h.mu.RLock()
			for conn := range h.connections {
				select {
				case conn.send <- message:
				default:
					close(conn.send)
					delete(h.connections, conn)
				}
			}
			h.mu.RUnlock()

		case roomMsg := <-h.roomBroadcast:
			h.mu.RLock()
			if room, ok := h.rooms[roomMsg.Room]; ok {
				for conn := range room {
					select {
					case conn.send <- roomMsg.Message:
					default:
						close(conn.send)
						delete(h.connections, conn)
						delete(room, conn)
					}
				}
			}
			h.mu.RUnlock()
		}
	}
}

// Register 注册连接
func (h *Hub) Register(conn *Conn) {
	h.closeMu.RLock()
	if h.closed {
		h.closeMu.RUnlock()
		return
	}
	h.closeMu.RUnlock()

	select {
	case h.register <- conn:
	default:
	}
}

// Unregister 注销连接
func (h *Hub) Unregister(conn *Conn) {
	h.closeMu.RLock()
	if h.closed {
		h.closeMu.RUnlock()
		return
	}
	h.closeMu.RUnlock()

	select {
	case h.unregister <- conn:
	default:
	}
}

// Broadcast 广播消息到所有连接
func (h *Hub) Broadcast(message []byte) {
	h.closeMu.RLock()
	if h.closed {
		h.closeMu.RUnlock()
		return
	}
	h.closeMu.RUnlock()

	select {
	case h.broadcast <- message:
	default:
	}
}

// BroadcastToRoom 广播消息到指定房间
func (h *Hub) BroadcastToRoom(room string, message []byte) {
	h.closeMu.RLock()
	if h.closed {
		h.closeMu.RUnlock()
		return
	}
	h.closeMu.RUnlock()

	select {
	case h.roomBroadcast <- RoomMessage{Room: room, Message: message}:
	default:
	}
}

// JoinRoom 将连接加入房间
func (h *Hub) JoinRoom(conn *Conn, room string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.rooms[room] == nil {
		h.rooms[room] = make(map[*Conn]bool)
	}
	h.rooms[room][conn] = true
	conn.JoinRoom(room)
}

// LeaveRoom 将连接移出房间
func (h *Hub) LeaveRoom(conn *Conn, room string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if roomConnections, ok := h.rooms[room]; ok {
		delete(roomConnections, conn)
		if len(roomConnections) == 0 {
			delete(h.rooms, room)
		}
	}
	conn.LeaveRoom(room)
}

// GetRoomConnections 获取房间内的连接数
func (h *Hub) GetRoomConnections(room string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if roomConnections, ok := h.rooms[room]; ok {
		return len(roomConnections)
	}
	return 0
}

// GetConnectionCount 获取总连接数
func (h *Hub) GetConnectionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.connections)
}

// removeFromRoom 从房间中移除连接（内部方法，需要持有锁）
func (h *Hub) removeFromRoom(room string, conn *Conn) {
	if roomConnections, ok := h.rooms[room]; ok {
		delete(roomConnections, conn)
		if len(roomConnections) == 0 {
			delete(h.rooms, room)
		}
	}
}

// closeAllConnections 关闭所有连接
func (h *Hub) closeAllConnections() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for conn := range h.connections {
		conn.Close()
		delete(h.connections, conn)
	}
}

// Server WebSocket 服务器
type Server struct {
	hub      *Hub
	upgrader *Upgrader
	opts     *Options
}

// NewServer 创建新的 WebSocket 服务器
func NewServer(opts *Options) *Server {
	if opts == nil {
		opts = DefaultOptions()
	}

	hub := NewHub(opts)
	upgrader := NewUpgrader(opts)

	return &Server{
		hub:      hub,
		upgrader: upgrader,
		opts:     opts,
	}
}

// HandleWebSocket 处理 WebSocket 连接
func (s *Server) HandleWebSocket(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}

	s.hub.Register(conn)

	// 启动读写 goroutine
	go s.writePump(conn)
	go s.readPump(conn)

	return conn, nil
}

// Start 启动服务器（启动 Hub）
func (s *Server) Start(ctx context.Context) {
	go s.hub.Run(ctx)
}

// GetHub 获取 Hub
func (s *Server) GetHub() *Hub {
	return s.hub
}

// readPump 读取消息
func (s *Server) readPump(conn *Conn) {
	defer func() {
		s.hub.Unregister(conn)
		conn.Close()
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Error("WebSocket read error", zap.Error(err))
			}
			break
		}

		// 处理消息（可以在这里添加消息处理逻辑）
		_ = message
	}
}

// writePump 写入消息和心跳
func (s *Server) writePump(conn *Conn) {
	ticker := time.NewTicker(s.opts.PingPeriod)
	defer func() {
		ticker.Stop()
		conn.Close()
	}()

	for {
		select {
		case message, ok := <-conn.send:
			conn.conn.SetWriteDeadline(time.Now().Add(s.opts.WriteWait))
			if !ok {
				conn.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := conn.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// 批量发送队列中的消息
			n := len(conn.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-conn.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			conn.conn.SetWriteDeadline(time.Now().Add(s.opts.WriteWait))
			if err := conn.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
