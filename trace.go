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
	"time"

	"github.com/go-anyway/framework-log"
	pkgtrace "github.com/go-anyway/framework-trace"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// handleWebSocketWithTrace 带追踪的 WebSocket 处理包装器
func handleWebSocketWithTrace(
	ctx context.Context,
	operation string,
	handler func(context.Context) error,
	enableTrace bool,
) error {
	startTime := time.Now()

	// 创建追踪 span
	var span trace.Span
	if enableTrace {
		ctx, span = pkgtrace.StartSpan(ctx, "websocket."+operation,
			trace.WithAttributes(
				attribute.String("websocket.operation", operation),
			),
		)
		defer span.End()
	}

	// 执行操作
	err := handler(ctx)
	duration := time.Since(startTime)

	// 记录日志
	if err != nil {
		log.FromContext(ctx).Error("WebSocket operation failed",
			zap.String("operation", operation),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	} else {
		log.FromContext(ctx).Info("WebSocket operation completed",
			zap.String("operation", operation),
			zap.Duration("duration", duration),
		)
	}

	// 更新追踪状态
	if enableTrace && span != nil {
		span.SetAttributes(
			attribute.Float64("websocket.duration_ms", float64(duration.Milliseconds())),
		)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		} else {
			span.SetStatus(codes.Ok, "")
		}
	}

	return err
}
