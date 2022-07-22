-- -- name: GetRecord :one
-- SELECT * FROM records WHERE id = $1 LIMIT 1;

-- name: ListRuns :many
SELECT * FROM runs ORDER BY run_id;

-- -- name: UpsertRecord :exec
-- INSERT INTO records (id, value, payload) VALUES ($1, $2, $3)
-- ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value, payload = EXCLUDED.payload;

-- -- name: UpsertRecords :exec
-- INSERT INTO records (id, value, payload)
-- SELECT unnest(@ids) AS id,
--        unnest(@values) AS names,
--        unnest(@payloads) AS payloads
-- ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value, payload = EXCLUDED.payload;

-- -- name: UpdateRecord :exec
-- UPDATE records SET value = $2, payload = $3 WHERE id = $1;

-- -- name: DeleteRecord :exec
-- DELETE FROM records WHERE id = $1;

-- name: GetTopicMessageIds :many
SELECT * FROM pulsar WHERE topic = $1;

-- name: UpsertMessageId :exec
INSERT INTO pulsar (topic, ledgerId, entryId, batchIdx, partitionIdx) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (topic) DO UPDATE SET ledgerId = EXCLUDED.ledgerId, entryId = EXCLUDED.entryId, batchIdx = EXCLUDED.batchIdx, partitionIdx = EXCLUDED.partitionIdx;
