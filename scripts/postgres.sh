# Create tables required by the scheduler.
docker exec -i postgres psql -U postgres -c "$(cat ./internal/scheduler/sql/schema.sql)"