package router

import (
	"DelayedNotifier/internal/router/handlers"
	"DelayedNotifier/internal/router/middleware"
	"github.com/wb-go/wbf/ginext"
	"go.uber.org/zap"
)

type Router struct {
	rout    *ginext.Engine
	handler *handlers.HandlersNotifier
	log     *zap.Logger
}

func NewRouter(mode string, handler *handlers.HandlersNotifier, log *zap.Logger) *Router {
	router := Router{
		rout:    ginext.New(mode),
		handler: handler,
		log:     log.Named("router"),
	}
	router.setupRouter()
	return &router
}

func (r *Router) setupRouter() {
	r.rout.Use(middleware.LoggingMiddleware(r.log))
	r.rout.POST("/notify", r.handler.CreateNotify)
	r.rout.GET("/notify/:id", r.handler.GetNotify)
	r.rout.DELETE("/notify/:id", r.handler.DeleteNotify)
}

func (r *Router) GetEngine() *ginext.Engine {
	return r.rout
}

func (r *Router) Start(addr string) error {
	return r.rout.Run(addr)
}
