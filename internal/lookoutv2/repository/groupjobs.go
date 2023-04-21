package repository

import (
	"context"
	"math"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/util"
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

type scanVarInit func() interface{}

type parserFn func(interface{}) (string, error)

type scanContext struct {
	field   string
	varInit scanVarInit
	parser  parserFn
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
		groupByQuery, err := NewQueryBuilder(r.lookoutTables).GroupBy(filters, order, groupedField, aggregates, skip, take)
		if err != nil {
			return err
		}
		logQuery(groupByQuery)
		groupRows, err := tx.Query(ctx, groupByQuery.Sql, groupByQuery.Args...)
		if err != nil {
			return err
		}
		groups, err = rowsToGroups(groupRows, groupedField, aggregates)
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

func rowsToGroups(rows pgx.Rows, groupedField string, aggregates []string) ([]*model.JobGroup, error) {
	var groups []*model.JobGroup
	for rows.Next() {
		jobGroup, err := scanGroup(rows, groupedField, aggregates)
		if err != nil {
			return nil, err
		}
		groups = append(groups, jobGroup)
	}
	return groups, nil
}

func scanGroup(rows pgx.Rows, field string, aggregates []string) (*model.JobGroup, error) {
	groupScanContext, err := groupScanContextForField(field)
	if err != nil {
		return nil, err
	}
	group := groupScanContext.varInit()
	var count int64

	scanContexts := make([]*scanContext, len(aggregates))
	aggregateVars := make([]interface{}, len(aggregates))
	for i, aggregate := range aggregates {
		sc, err := aggregateScanContextForField(aggregate)
		if err != nil {
			return nil, err
		}
		aggregateVars[i] = sc.varInit()
		scanContexts[i] = sc
	}
	aggregateRefs := make([]interface{}, len(aggregates))
	for i := 0; i < len(aggregates); i++ {
		aggregateRefs[i] = &aggregateVars[i]
	}
	varAddresses := util.Concat([]interface{}{&group, &count}, aggregateRefs)
	err = rows.Scan(varAddresses...)
	if err != nil {
		return nil, err
	}
	parsedGroup, err := groupScanContext.parser(group)
	if err != nil {
		return nil, err
	}
	aggregatesMap := make(map[string]string)
	for i, sc := range scanContexts {
		val := aggregateVars[i]
		parsedVal, err := sc.parser(val)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse value for field %s", sc.field)
		}
		aggregatesMap[sc.field] = parsedVal
	}
	return &model.JobGroup{
		Name:       parsedGroup,
		Count:      count,
		Aggregates: aggregatesMap,
	}, nil
}

func groupScanContextForField(field string) (*scanContext, error) {
	switch field {
	case stateField:
		return &scanContext{
			field:   field,
			varInit: int16ScanVar,
			parser:  stateParser,
		}, nil
	default:
		return &scanContext{
			field:   field,
			varInit: stringScanVar,
			parser:  stringParser,
		}, nil
	}
}

func aggregateScanContextForField(field string) (*scanContext, error) {
	switch field {
	case lastTransitionTimeField:
		return &scanContext{
			field:   lastTransitionTimeField,
			varInit: numericScanVar,
			parser:  avgLastTransitionTimeParser,
		}, nil
	case submittedField:
		return &scanContext{
			field:   submittedField,
			varInit: timeScanVar,
			parser:  maxSubmittedTimeParser,
		}, nil
	default:
		return nil, errors.Errorf("no aggregate found for field %s", field)
	}
}

func stringScanVar() interface{} {
	return ""
}

func int16ScanVar() interface{} {
	return int16(0)
}

func numericScanVar() interface{} {
	return pgtype.Numeric{}
}

func timeScanVar() interface{} {
	return time.Time{}
}

func avgLastTransitionTimeParser(val interface{}) (string, error) {
	lastTransitionTimeSeconds, ok := val.(pgtype.Numeric)
	if !ok {
		return "", errors.Errorf("could not convert %v: %T to int64", val, val)
	}
	var dst float64
	err := lastTransitionTimeSeconds.AssignTo(&dst)
	if err != nil {
		return "", err
	}
	t := time.Unix(int64(math.Round(dst)), 0)
	return t.Format(time.RFC3339), nil
}

func maxSubmittedTimeParser(val interface{}) (string, error) {
	maxSubmittedTime, ok := val.(time.Time)
	if !ok {
		return "", errors.Errorf("could not convert %v: %T to time", val, val)
	}
	return maxSubmittedTime.Format(time.RFC3339), nil
}

func stateParser(val interface{}) (string, error) {
	stateInt, ok := val.(int16)
	if !ok {
		return "", errors.Errorf("could not convert %v: %T to int for state", val, val)
	}
	state, ok := lookout.JobStateMap[int(stateInt)]
	if !ok {
		return "", errors.Errorf("state not found: %d", stateInt)
	}
	return string(state), nil
}

func stringParser(val interface{}) (string, error) {
	str, ok := val.(string)
	if !ok {
		return "", errors.Errorf("could not convert %v: %T to string", val, val)
	}
	return str, nil
}
