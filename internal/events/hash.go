package events

import "crypto/sha256"

// HashFromQueueJobSetName returns a joint hash of queue and jobSetName.
// To ensure that queue=aa, jobSetName=b and queue=a, jobSetName=ab result in different hashes,
// queue and jobSetName are hashed separately and then hashed together.
func HashFromQueueJobSetName(queue, jobSetName string) []byte {
	queueHash := sha256.Sum256([]byte(queue))
	jobSetNameHash := sha256.Sum256([]byte(jobSetName))
	hash := sha256.Sum256(append(queueHash[:], jobSetNameHash[:]...))
	return hash[:]
}
