package repository

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
)

type GroupByResult struct {
	// Total number of groups
	Count  int
	Groups []*model.JobGroup
}

type GroupJobsRepository interface {
	GroupBy(
		ctx context.Context,
		filters []*model.Filter,
		order *model.Order,
		groupedField string,
		aggregates []string,
		skip int,
		take int,
	) (*GroupByResult, error)
}

type SqlGroupJobsRepository struct {
	db            *pgxpool.Pool
	lookoutTables *LookoutTables
}

func NewSqlGroupJobsRepository(db *pgxpool.Pool) *SqlGroupJobsRepository {
	return &SqlGroupJobsRepository{
		db:            db,
		lookoutTables: NewTables(),
	}
}

func (r *SqlGroupJobsRepository) GroupBy(
	ctx context.Context,
	filters []*model.Filter,
	order *model.Order,
	groupedField string,
	aggregates []string,
	skip int,
	take int,
) (*GroupByResult, error) {
	var groups []*model.JobGroup
	var count int

	err := r.db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadOnly,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		countQuery, err := NewQueryBuilder(r.lookoutTables).CountGroups(filters, groupedField)
		if err != nil {
			return err
		}
		logQuery(countQuery)
		rows, err := tx.Query(ctx, countQuery.Sql, countQuery.Args...)
		if err != nil {
			return err
		}
		count, err = database.ReadInt(rows)
		if err != nil {
			return err
		}
		groupByQuery, err := NewQueryBuilder(r.lookoutTables).GroupBy(filters, order, groupedField, skip, take)
		if err != nil {
			return err
		}
		logQuery(groupByQuery)
		groupRows, err := tx.Query(ctx, groupByQuery.Sql, groupByQuery.Args...)
		if err != nil {
			return err
		}
		groups, err = rowsToGroups(groupRows, groupedField)
		return err
	})
	if err != nil {
		return nil, err
	}

	return &GroupByResult{
		Groups: groups,
		Count:  count,
	}, nil
}

func rowsToGroups(rows pgx.Rows, groupedField string) ([]*model.JobGroup, error) {
	var groups []*model.JobGroup
	for rows.Next() {
		jobGroup, err := scanGroup(rows, groupedField)
		if err != nil {
			return nil, err
		}
		groups = append(groups, jobGroup)
	}
	return groups, nil
}

func scanGroup(rows pgx.Rows, field string) (*model.JobGroup, error) {
	if field == "state" {
		var stateInt int
		var count int64
		err := rows.Scan(&stateInt, &count)
		if err != nil {
			return nil, err
		}
		state, ok := lookout.JobStateMap[stateInt]
		if !ok {
			return nil, errors.Errorf("state not found: %d", stateInt)
		}
		return &model.JobGroup{
			Name:       string(state),
			Count:      count,
			Aggregates: make(map[string]string),
		}, nil
	}
	var group string
	var count int64
	err := rows.Scan(&group, &count)
	if err != nil {
		return nil, err
	}
	return &model.JobGroup{
		Name:       group,
		Count:      count,
		Aggregates: make(map[string]string),
	}, nil
}
