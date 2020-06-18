package sql

import (
	"database/sql"
	"encoding/json"

	"github.com/G-Research/armada/pkg/api"
)

type UsageRepository struct {
	db *sql.DB
}

func NewUsageRepository(db *sql.DB) *UsageRepository {
	return &UsageRepository{db: db}
}

func (r UsageRepository) GetClusterPriority(clusterId string) (map[string]float64, error) {
	rows, err := r.db.Query("SELECT queue, priority FROM cluster_priority WHERE cluster = $1", clusterId)
	if err != nil {
		return nil, err
	}

	result := map[string]float64{}
	for rows.Next() {
		var queue string
		var priority float64
		err := rows.Scan(&queue, &priority)
		if err != nil {
			return nil, err
		}
		result[queue] = priority
	}
	return result, nil
}

func (r UsageRepository) GetClusterPriorities(clusterIds []string) (map[string]map[string]float64, error) {
	rows, err := r.db.Query("SELECT cluster, queue, priority FROM cluster_priority")
	if err != nil {
		return nil, err
	}

	result := map[string]map[string]float64{}
	for rows.Next() {
		var cluster string
		var queue string
		var priority float64
		err := rows.Scan(&cluster, &queue, &priority)
		if err != nil {
			return nil, err
		}

		clusterPriorities, ok := result[cluster]
		if !ok {
			clusterPriorities = map[string]float64{}
			result[cluster] = clusterPriorities
		}
		clusterPriorities[queue] = priority
	}
	return result, nil
}

func (r UsageRepository) GetClusterUsageReports() (map[string]*api.ClusterUsageReport, error) {
	rows, err := r.db.Query("SELECT data FROM cluster_usage")
	if err != nil {
		return nil, err
	}
	return readClusterUsageReport(rows)
}

func (r UsageRepository) UpdateCluster(report *api.ClusterUsageReport, priorities map[string]float64) error {
	data, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = upsert(r.db, "cluster_usage", "cluster", []string{"data"}, []interface{}{report.ClusterId, data})

	if err != nil {
		return err
	}

	if len(priorities) > 0 {
		values := []interface{}{}
		for queue, usage := range priorities {
			values = append(values, report.ClusterId, queue, usage)
		}
		insertSql := createInsertQuery("cluster_priority", []string{"cluster", "queue", "priority"}, values)
		insertSql += " ON CONFLICT ON CONSTRAINT cluster_priority_pkey DO UPDATE SET priority = EXCLUDED.priority"
		_, err = r.db.Exec(insertSql, values...)
		return err
	}
	return nil
}

func (r UsageRepository) GetClusterLeasedReports() (map[string]*api.ClusterLeasedReport, error) {
	rows, err := r.db.Query("SELECT data FROM cluster_leased")
	if err != nil {
		return nil, err
	}
	return readClusterLeasedReport(rows)
}

func (r UsageRepository) UpdateClusterLeased(report *api.ClusterLeasedReport) error {
	data, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = upsert(r.db, "cluster_leased", "cluster", []string{"data"}, []interface{}{report.ClusterId, data})
	return err
}

func readClusterLeasedReport(rows *sql.Rows) (map[string]*api.ClusterLeasedReport, error) {
	infos := map[string]*api.ClusterLeasedReport{}
	err := readByteRows(rows, func(b []byte) error {
		var report *api.ClusterLeasedReport
		err := json.Unmarshal(b, &report)
		if err != nil {
			return err
		}
		infos[report.ClusterId] = report
		return err
	})
	return infos, err
}

func readClusterUsageReport(rows *sql.Rows) (map[string]*api.ClusterUsageReport, error) {
	infos := map[string]*api.ClusterUsageReport{}
	err := readByteRows(rows, func(b []byte) error {
		var report *api.ClusterUsageReport
		err := json.Unmarshal(b, &report)
		if err != nil {
			return err
		}
		infos[report.ClusterId] = report
		return err
	})
	return infos, err
}
