package middleware

import (
	"github.com/wb-go/wbf/ginext"
	"go.uber.org/zap"
)

func LoggingMiddleware(log *zap.Logger) ginext.HandlerFunc {
	return func(c *ginext.Context) {
		requestLog := log.With(
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.String("remote_addr", c.Request.RemoteAddr),
		)

		requestLog.Info("Request started")

		c.Set("logger", requestLog)
		c.Next()
		requestLog.Info("Request completed")
	}
}
