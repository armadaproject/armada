package repository

import (
	"database/sql"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	redis2 "github.com/G-Research/armada/internal/armada/repository/redis"
	sqlRepository "github.com/G-Research/armada/internal/armada/repository/sql"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

func TestGetClusterLeasedReports(t *testing.T) {
	withUsageRepository(func(r UsageRepository) {
		cluster1Report := makeClusterLeasedReport("cluster-1", "queue-1")
		cluster2Report := makeClusterLeasedReport("cluster-2", "queue-1", "queue-2")

		e := r.UpdateClusterLeased(cluster1Report)
		assert.Nil(t, e)

		e = r.UpdateClusterLeased(cluster2Report)
		assert.Nil(t, e)

		retrievedReport, e := r.GetClusterLeasedReports()
		assert.Nil(t, e)
		assert.Len(t, retrievedReport, 2)
		assert.Equal(t, retrievedReport["cluster-1"], cluster1Report)
		assert.Equal(t, retrievedReport["cluster-2"], cluster2Report)
	})
}

func TestUpdateClusterLeased(t *testing.T) {
	withUsageRepository(func(r UsageRepository) {
		report := makeClusterLeasedReport("cluster-1", "queue-1")
		e := r.UpdateClusterLeased(report)
		assert.Nil(t, e)

		updatedReport := makeClusterLeasedReport("cluster-1", "queue-1", "queue-2")
		e = r.UpdateClusterLeased(updatedReport)
		assert.Nil(t, e)

		retrievedReport, e := r.GetClusterLeasedReports()
		assert.Nil(t, e)
		assert.Len(t, retrievedReport, 1)
		assert.Equal(t, retrievedReport["cluster-1"], updatedReport)
	})
}

func makeClusterLeasedReport(clusterId string, queueNames ...string) *api.ClusterLeasedReport {
	cpuAndMemory := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}
	queueReports := make([]*api.QueueLeasedReport, 0, len(queueNames))
	for _, queueName := range queueNames {
		report := &api.QueueLeasedReport{
			Name:            queueName,
			ResourcesLeased: cpuAndMemory,
		}
		queueReports = append(queueReports, report)
	}
	report := &api.ClusterLeasedReport{
		ClusterId:  clusterId,
		ReportTime: time.Now().UTC(),
		Queues:     queueReports,
	}

	return report
}

func withUsageRepository(action func(r UsageRepository)) {
	withSqlUsageRepository(action)
	withRedisUsageRepository(action)

}

func withRedisUsageRepository(action func(r UsageRepository)) {

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB()
	defer client.Close()

	client.FlushDB()

	repo := redis2.NewRedisUsageRepository(client)
	action(repo)
}

func withSqlUsageRepository(action func(r UsageRepository)) {
	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=postgres password=psw dbname=postgres sslmode=disable")

	if err != nil {
		panic(err)
	}

	db.Exec("TRUNCATE TABLE cluster_leased")
	defer db.Exec("TRUNCATE TABLE cluster_leased")
	defer db.Close()

	repo := sqlRepository.NewUsageRepository(db)
	action(repo)
}
