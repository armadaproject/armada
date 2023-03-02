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
	on           string
}

func NewFromBuilder(baseTable, baseTableAbbrev string) *FromBuilder {
	return &FromBuilder{
		baseTable:       baseTable,
		baseTableAbbrev: baseTableAbbrev,
		joins:           nil,
	}
}

func (b *FromBuilder) Join(joinType JoinType, table, abbrev, on string) *FromBuilder {
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
		sb.WriteString(fmt.Sprintf(" %[1]s JOIN %[2]s AS %[3]s ON %[4]s.%[5]s = %[3]s.%[5]s",
			join.joinType,
			join.table,
			join.abbreviation,
			b.baseTableAbbrev,
			join.on))
	}
	return sb.String()
}
