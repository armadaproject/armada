package context

func CalculateAwayQueueName(queueName string) string {
	return queueName + "-away"
}
