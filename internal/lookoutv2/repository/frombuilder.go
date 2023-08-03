package repository

import (
	"fmt"
	"strings"
)

type JoinType string

const (
	Left  JoinType = "LEFT"
	Inner JoinType = "INNER"
)

type FromBuilder struct {
	baseTable       string
	baseTableAbbrev string
	joins           []*join
}

type join struct {
	joinType     JoinType
	table        string
	abbreviation string
	on           []string
}

func NewFromBuilder(baseTable, baseTableAbbrev string) *FromBuilder {
	return &FromBuilder{
		baseTable:       baseTable,
		baseTableAbbrev: baseTableAbbrev,
		joins:           nil,
	}
}

// Join specifies JOIN with other table
// Include multiple values in the on list to join by multiple columns
// Note: the columns you join on need to have the same names in both tables
func (b *FromBuilder) Join(joinType JoinType, table string, abbrev string, on []string) *FromBuilder {
	b.joins = append(b.joins, &join{
		joinType:     joinType,
		table:        table,
		abbreviation: abbrev,
		on:           on,
	})
	return b
}

func (b *FromBuilder) Build() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("FROM %s AS %s", b.baseTable, b.baseTableAbbrev))
	for _, join := range b.joins {
		joinConditions := make([]string, len(join.on))
		for i, col := range join.on {
			joinConditions[i] = fmt.Sprintf("%[1]s.%[2]s = %[3]s.%[2]s",
				b.baseTableAbbrev,
				col,
				join.abbreviation)
		}
		fullJoinCondition := strings.Join(joinConditions, " AND ")
		sb.WriteString(fmt.Sprintf(" %[1]s JOIN %[2]s AS %[3]s ON %[4]s",
			join.joinType,
			join.table,
			join.abbreviation,
			fullJoinCondition))
	}
	return sb.String()
}
