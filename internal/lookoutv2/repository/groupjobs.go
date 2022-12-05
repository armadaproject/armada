package repository

import (
	"context"
	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/common/database/lookout"
	"github.com/G-Research/armada/internal/lookoutv2/model"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/jackc/pgx/v4/pgxpool"
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
	) (GroupByResult, error)
}

type SqlGroupJobsRepository struct {
	db           *pgxpool.Pool
	queryBuilder *QueryBuilder
}

func NewSqlGroupJobsRepository(db *pgxpool.Pool) *SqlGroupJobsRepository {
	return &SqlGroupJobsRepository{
		db:           db,
		queryBuilder: &QueryBuilder{lookoutTables: NewTables()},
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
	var err error
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	})
	if err != nil {
		log.WithError(err).Error("failed to start transaction")
		return nil, err
	}
	defer func() {
		if err != nil {
			log.WithError(err).Error("transaction failed, rolling back")
			txCloseErr := tx.Rollback(ctx)
			if txCloseErr != nil {
				log.WithError(txCloseErr).Error("failed to roll back transaction")
			}
		} else {
			txCloseErr := tx.Commit(ctx)
			if txCloseErr != nil {
				log.WithError(txCloseErr).Error("failed to commit transaction")
			}
			err = txCloseErr
		}
	}()

	countQuery, err := r.queryBuilder.CountGroups(filters, groupedField)
	rows, err := tx.Query(ctx, countQuery.Sql, countQuery.Args...)
	if err != nil {
		return nil, err
	}
	count, err := database.ReadInt(rows)
	if err != nil {
		return nil, err
	}

	groupByQuery, err := r.queryBuilder.GroupBy(filters, order, groupedField, skip, take)
	groupRows, err := tx.Query(ctx, groupByQuery.Sql, groupByQuery.Args...)
	if err != nil {
		return nil, err
	}

	groups, err := rowsToGroups(groupRows, groupedField)
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
