package db

import (
	"context"

	"github.com/armadaproject/armada/internal/lookout/model"
	"github.com/armadaproject/armada/pkg/api"
)

type ClickHouseDatabase struct {
	config map[string]string
}

func NewClickHouseDatabase(config map[string]string) *ClickHouseDatabase {
	return &ClickHouseDatabase{config: config}
}

func (c *ClickHouseDatabase) InitialiseSchema(ctx context.Context) error {
	panic("not implemented")
}

func (c *ClickHouseDatabase) ExecuteIngestionQueryBatch(ctx context.Context, queries []IngestionQuery) error {
	panic("not implemented")
}

func (c *ClickHouseDatabase) GetJobRunDebugMessage(ctx context.Context, jobRunID string) (string, error) {
	panic("not implemented")
}

func (c *ClickHouseDatabase) GetJobRunError(ctx context.Context, jobRunID string) (string, error) {
	panic("not implemented")
}

func (c *ClickHouseDatabase) GetJobSpec(ctx context.Context, jobID string) (*api.Job, error) {
	panic("not implemented")
}

func (c *ClickHouseDatabase) GetJobs(ctx *context.Context, filters []*model.Filter, activeJobSets bool, order *model.Order, skip int, take int) ([]*model.Job, error) {
	panic("not implemented")
}

func (c *ClickHouseDatabase) GetJobGroups(ctx *context.Context, filters []*model.Filter, order *model.Order, groupedField *model.GroupedField, aggregates []string, skip int, take int) ([]*model.JobGroup, error) {
	panic("not implemented")
}

func (c *ClickHouseDatabase) PopulateHistoricalJobs(_ context.Context, _ HistoricalJobsParams) error {
	panic("not implemented")
}

func (c *ClickHouseDatabase) TearDown(ctx context.Context) error {
	panic("not implemented")
}

func (c *ClickHouseDatabase) Close() {
	panic("not implemented")
}
