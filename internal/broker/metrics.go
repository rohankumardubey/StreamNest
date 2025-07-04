package broker

import "github.com/prometheus/client_golang/prometheus"

var (
	messagesProduced = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "streamnest_messages_produced_total",
		Help: "Total number of messages produced",
	})
	messagesConsumed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "streamnest_messages_consumed_total",
		Help: "Total number of messages consumed",
	})
)

func RegisterMetrics() {
	prometheus.MustRegister(messagesProduced, messagesConsumed)
}

func IncProduced() {
	messagesProduced.Inc()
}

func IncConsumed() {
	messagesConsumed.Inc()
}
