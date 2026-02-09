package event

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var activeEventSubscriptions = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "armada_event_api_active_subscriptions",
		Help: "Number of active (long-lived) event API subscriptions (Watch/GetJobSetEvents with watch=true) split by user.",
	},
	[]string{"user"},
)
