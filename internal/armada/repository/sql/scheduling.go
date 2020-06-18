package sql

import (
	"database/sql"
	"encoding/json"

	"github.com/G-Research/armada/pkg/api"
)

type SchedulingInfoRepository struct {
	db *sql.DB
}

func NewSchedulingInfoRepository(db *sql.DB) *SchedulingInfoRepository {
	return &SchedulingInfoRepository{db: db}
}

func (r *SchedulingInfoRepository) GetClusterSchedulingInfo() (map[string]*api.ClusterSchedulingInfoReport, error) {
	rows, err := r.db.Query("SELECT data FROM cluster_scheduling_info")
	if err != nil {
		return nil, err
	}
	return readSchedulingInfo(rows)
}

func (r *SchedulingInfoRepository) UpdateClusterSchedulingInfo(report *api.ClusterSchedulingInfoReport) error {
	data, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = upsert(r.db, "cluster_scheduling_info", "cluster", []string{"data"}, []interface{}{report.ClusterId, data})
	return err
}

func readSchedulingInfo(rows *sql.Rows) (map[string]*api.ClusterSchedulingInfoReport, error) {
	infos := map[string]*api.ClusterSchedulingInfoReport{}
	err := readByteRows(rows, func(b []byte) error {
		var info *api.ClusterSchedulingInfoReport
		err := json.Unmarshal(b, &info)
		if err != nil {
			return err
		}
		infos[info.ClusterId] = info
		return err
	})
	return infos, err
}
