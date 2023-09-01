package repository

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
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
		ctx *armadacontext.ArmadaContext,
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

const stateAggregatePrefix = "state_"

func NewSqlGroupJobsRepository(db *pgxpool.Pool) *SqlGroupJobsRepository {
	return &SqlGroupJobsRepository{
		db:            db,
		lookoutTables: NewTables(),
	}
}

func (r *SqlGroupJobsRepository) GroupBy(
	ctx *armadacontext.ArmadaContext,
	filters []*model.Filter,
	activeJobSets bool,
	order *model.Order,
	groupedField *model.GroupedField,
	aggregates []string,
	skip int,
	take int,
) (*GroupByResult, error) {
	var groups []*model.JobGroup
	var count int

	err := pgx.BeginTxFunc(ctx, r.db, pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadOnly,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		countQuery, err := NewQueryBuilder(r.lookoutTables).CountGroups(filters, activeJobSets, groupedField)
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
		groupByQuery, err := NewQueryBuilder(r.lookoutTables).GroupBy(filters, activeJobSets, order, groupedField, aggregates, skip, take)
		if err != nil {
			return err
		}
		logQuery(groupByQuery)
		groupRows, err := tx.Query(ctx, groupByQuery.Sql, groupByQuery.Args...)
		if err != nil {
			return err
		}
		groups, err = rowsToGroups(groupRows, groupedField, aggregates, filters)
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

func rowsToGroups(rows pgx.Rows, groupedField *model.GroupedField, aggregates []string, filters []*model.Filter) ([]*model.JobGroup, error) {
	var groups []*model.JobGroup
	for rows.Next() {
		jobGroup, err := scanGroup(rows, groupedField.Field, aggregates, filters)
		if err != nil {
			return nil, err
		}
		groups = append(groups, jobGroup)
	}
	return groups, nil
}

func scanGroup(rows pgx.Rows, field string, aggregates []string, filters []*model.Filter) (*model.JobGroup, error) {
	groupParser := ParserForGroup(field)
	var count int64
	var aggregateParsers []FieldParser
	for _, aggregate := range aggregates {
		parsers, err := ParsersForAggregate(aggregate, filters)
		if err != nil {
			return nil, err
		}
		aggregateParsers = append(aggregateParsers, parsers...)
	}
	aggregateRefs := make([]interface{}, len(aggregateParsers))
	for i, parser := range aggregateParsers {
		aggregateRefs[i] = parser.GetVariableRef()
	}
	varAddresses := util.Concat([]interface{}{groupParser.GetVariableRef(), &count}, aggregateRefs)
	err := rows.Scan(varAddresses...)
	if err != nil {
		return nil, err
	}
	parsedGroup, err := groupParser.ParseValue()
	if err != nil {
		return nil, err
	}
	aggregatesMap := make(map[string]interface{})
	for _, parser := range aggregateParsers {
		val, err := parser.ParseValue()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse value for field %s", parser.GetField())
		}
		if strings.HasPrefix(parser.GetField(), stateAggregatePrefix) {
			singleStateCount, ok := val.(int)
			if !ok {
				return nil, errors.Errorf("failed to parse value for state aggregate: cannot convert value to int: %v: %T", singleStateCount, singleStateCount)
			}
			stateCountsVal, ok := aggregatesMap[stateField]
			if !ok {
				stateCountsVal = map[string]int{}
				aggregatesMap[stateField] = stateCountsVal
			}
			stateCounts, ok := stateCountsVal.(map[string]int)
			if !ok {
				return nil, errors.Errorf("failed to parse value for state aggregate: cannot cast state counts to map")
			}
			state := parser.GetField()[len(stateAggregatePrefix):]
			stateCounts[state] = singleStateCount
		} else {
			aggregatesMap[parser.GetField()] = val
		}
	}
	return &model.JobGroup{
		Name:       fmt.Sprintf("%s", parsedGroup),
		Count:      count,
		Aggregates: aggregatesMap,
	}, nil
}
