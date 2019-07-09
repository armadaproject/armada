package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/k8s-batch/internal/model"
)

type AggregatedQueue interface {
	LeaseJobs(requests LeaseRequest) (JobLease, error)
	RenewLease(ids []string) error
	ReportDone(ids []string) error
}

type LeaseRequest struct {
	ClusterID         string
	AvailableResource v1.ResourceList
}

type JobLease struct {
	jobs []model.Job
}

func SetupQueueApiGroup(g *gin.RouterGroup, queue *AggregatedQueue) {

	g.POST("lease", func(c *gin.Context) {
		var leaseRequest LeaseRequest
		err := c.Bind(&leaseRequest)
		if err != nil {
			sendError(c, err)
			return
		}
		lease, err := (*queue).LeaseJobs(leaseRequest)
		if err != nil {
			sendError(c, err)
			return
		}
		c.JSON(http.StatusOK, lease)
	})

	g.POST("renew-lease", func(c *gin.Context) {
		var ids []string
		err := c.Bind(&ids)
		if err != nil {
			sendError(c, err)
			return
		}
		err = (*queue).RenewLease(ids)
		if err != nil {
			sendError(c, err)
			return
		}
		c.Status(http.StatusOK)
	})

	g.POST("done", func(c *gin.Context) {
		var ids []string
		err := c.Bind(&ids)
		if err != nil {
			sendError(c, err)
			return
		}
		err = (*queue).ReportDone(ids)
		if err != nil {
			sendError(c, err)
			return
		}
		c.Status(http.StatusOK)
	})
}

func sendError(c *gin.Context, e error) {
	c.JSON(http.StatusBadRequest, gin.H{
		"errorMessage": e.Error(),
	})
}
