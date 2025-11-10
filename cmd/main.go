package main

import (
	"DelayedNotifier/internal/rabbit/consumer"
	"DelayedNotifier/internal/rabbit/publisher"
	"DelayedNotifier/internal/repository"
	"DelayedNotifier/internal/router"
	"DelayedNotifier/internal/router/handlers"
	"DelayedNotifier/internal/sender"
	"DelayedNotifier/internal/service"
	"DelayedNotifier/pkg/logger"
	"context"
	"github.com/wb-go/wbf/config"
	"go.uber.org/zap"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := config.New()
	_ = cfg.LoadConfigFiles("./config/config.yaml")
	log, err := logger.NewLogger(cfg.GetString("log_level"))
	if err != nil {
		panic(err)
	}
	defer log.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	publish, err := publisher.NewPublisher(cfg.GetString("amqp_server_url"),
		cfg.GetString("work_queue"), cfg.GetString("delayed_exchange"),
		cfg.GetString("work_routing_key"),
		log)
	if err != nil {
		log.Fatal("Failed to create publisher", zap.Error(err))
	}
	defer publish.Close()

	repo, err := repository.NewRepository(cfg.GetString("master_dsn"), []string{}, log)
	if err != nil {
		log.Fatal("Failed to create repository", zap.Error(err))
	}

	sendr := sender.NewTelegramSender(cfg.GetString("bot_token"))
	serviceNotifier := service.NewService(publish, repo, sendr, log)

	cons, err := consumer.NewConsumer(cfg.GetString("amqp_server_url"), serviceNotifier, cfg.GetString("work_queue"), log)
	if err != nil {
		log.Fatal("Failed to create consumer", zap.Error(err))
	}
	defer cons.Close()

	handlerNotifier := handlers.NewHandlersNotifier(serviceNotifier)

	// --- 1. Запуск консьюмера в отдельной горутине ---
	go func() {
		log.Info("Starting consumer...")
		cons.StartListening(ctx, cfg.GetInt("max_attempts"))
		log.Info("Consumer stopped.")
	}()

	// --- 2. Настройка и запуск HTTP-сервера ---
	rout := router.NewRouter(cfg.GetString("log_level"), handlerNotifier, log)
	srv := &http.Server{
		Addr:    cfg.GetString("addr"),
		Handler: rout.GetEngine(),
	}

	go func() {
		log.Info("Starting HTTP server", zap.String("address", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("Failed to start server", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down server...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown:", zap.Error(err))
	}

	cancel()

	log.Info("Server exiting")
}
