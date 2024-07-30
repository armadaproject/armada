package metrics

//func TestReportSchedulerResult(t *testing.T) {
//	tests := map[string]struct {
//		schedulerResult schedulerresult.SchedulerResult
//	}{
//		"JobsConsidered": {},
//	}
//	for name, tc := range tests {
//		t.Run(name, func(t *testing.T) {
//			metrics, err := New(nil, nil)
//			require.NoError(t, err)
//			metrics.ReportSchedulerResult(nil)
//		})
//	}
//}

//func createSchedulerResult() {
//	schedCtx := context.SchedulingContext{
//		Pool:                 "pool1",
//		FairnessCostProvider: nil,
//		WeightSum:            100.0,
//		QueueSchedulingContexts: map[string]*context.QueueSchedulingContext{
//			{
//				Queue:             "",
//				Allocated:         schedulerobjects.ResourceList{},
//				Demand:            schedulerobjects.ResourceList{},
//				CappedDemand:      schedulerobjects.ResourceList{},
//				FairShare:         0,
//				AdjustedFairShare: 0,
//			},
//		},
//	}
//}
//
//func nCpu(n int) {
//
//}
