package handlers

import (
	"DelayedNotifier/internal/models"
	"DelayedNotifier/internal/service"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/wb-go/wbf/ginext"
	"go.uber.org/zap"
	"net/http"
)

type HandlersNotifier struct {
	serviceNotifier *service.Service
}

func NewHandlersNotifier(serviceNotifier *service.Service) *HandlersNotifier {
	return &HandlersNotifier{
		serviceNotifier: serviceNotifier,
	}
}

func (h *HandlersNotifier) CreateNotify(c *ginext.Context) {
	log := c.MustGet("logger").(*zap.Logger)
	log.Info("CreateNotifier started")

	notification := &models.Notification{}
	if err := json.NewDecoder(c.Request.Body).Decode(notification); err != nil {
		log.Error("Failed to decode body", zap.Error(err))
		c.JSON(http.StatusBadRequest, ginext.H{"error": "Invalid request body"})
		return
	}

	//if err := notification.Validate(); err != nil {
	//	log.Error("Failed to validate notification", zap.Error(err))
	//	c.JSON(http.StatusBadRequest, ginext.H{"error": err.Error()})
	//	return
	//}

	id, err := h.serviceNotifier.Create(c.Request.Context(), *notification)
	if err != nil {
		log.Error("Failed to publish message", zap.Error(err))
		c.JSON(http.StatusInternalServerError, ginext.H{"error": "Failed to publish message"})
		return
	}
	log.Info("CreateNotifier finished", zap.String("id", id))
	c.JSON(http.StatusOK, gin.H{"id": id})
}

func (h *HandlersNotifier) GetNotify(c *ginext.Context) {
	log := c.MustGet("logger").(*zap.Logger)
	log.Info("GetNotifier started")
	id := c.Param("id")
	notification, err := h.serviceNotifier.Get(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, models.ErrNotFound) {
			log.Error("Notifier not found", zap.String("id", id))
			c.JSON(http.StatusNotFound, ginext.H{"error": "Notifier not found"})
			return
		}
		log.Error("Failed to get notifier", zap.String("id", id), zap.Error(err))
		c.JSON(http.StatusInternalServerError, ginext.H{"error": "Failed to get notifier"})
		return
	}
	log.Info("GetNotifier finished", zap.String("id", id))
	c.JSON(http.StatusOK, notification)
}

func (h *HandlersNotifier) DeleteNotify(c *ginext.Context) {
	log := c.MustGet("logger").(*zap.Logger)
	log.Info("DeleteNotifier started")
	id := c.Param("id")
	if err := h.serviceNotifier.Delete(c.Request.Context(), id); err != nil {
		if errors.Is(err, models.ErrNotFound) {
			log.Warn("Notifier not found", zap.String("id", id))
			c.JSON(http.StatusNotFound, ginext.H{"error": "Notifier not found"})
			return
		}
		log.Error("Failed to delete notifier", zap.String("id", id), zap.Error(err))
		c.JSON(http.StatusInternalServerError, ginext.H{"error": "Failed to delete notifier"})
		return
	}
	log.Info("DeleteNotifier finished", zap.String("id", id))
	c.JSON(http.StatusNoContent, gin.H{"id": id})
}
