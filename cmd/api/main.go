package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"

	"github.com/G-Research/k8s-batch/internal/model"
	"github.com/G-Research/k8s-batch/internal/repository"
)

func main() {
	r := gin.Default()
	db := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	r.POST("jobs", func(c *gin.Context) {
		var jobs []model.JobRequest

		err := c.Bind(&jobs)
		if err != nil {
			sendError(c, err)
			return
		}
		err = repository.AddJobs(db, jobs)
		if err != nil {
			sendError(c, err)
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message": "ok",
		})
	})

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	r.Run() // listen and serve on 0.0.0.0:8080
}

func sendError(c *gin.Context, e error) {
	c.JSON(http.StatusBadRequest, gin.H{
		"errorMessage": e.Error(),
	})
}
