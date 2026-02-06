# Broadside Example Configurations

This directory contains example YAML configuration files for Broadside load tests. These examples demonstrate different testing scenarios and configuration patterns.

## Available Examples

### test-inmemory.yaml

**Purpose**: Quick smoke testing of the Broadside framework

**Use Cases**:
- Verifying changes to Broadside code
- Testing configuration changes without database setup
- Learning Broadside configuration structure

**Configuration Highlights**:
- 30-second test with 5-second warmup
- In-memory database (no external dependencies)
- Small historical job count (10,000)
- Single queue with single job set
- Includes example actions (reprioritisation and cancellation)

**Running**:
```bash
go run ./cmd/broadside --config ./cmd/broadside/configs/examples/test-inmemory.yaml
```

**Performance Note**: The in-memory adapter is not suitable for performance benchmarking. It's purely for framework development and smoke testing.

---

### test-postgres.yaml

**Purpose**: PostgreSQL load testing with realistic configuration

**Use Cases**:
- Performance benchmarking against PostgreSQL
- Testing with production-like query patterns
- Multi-queue, multi-job-set scenarios

**Configuration Highlights**:
- 60-second test with 10-second warmup
- PostgreSQL connection (localhost:5433)
- Multiple queues (queue-1: 60%, queue-2: 40%)
- Multiple job sets with different characteristics
- Historical jobs distributed across states
- Parallel ingestion workers (4 workers)
- Multiple query types with realistic rates
- Scheduled actions for measuring bulk operation impact

**Database Setup**:

1. **Start PostgreSQL container**:
   ```bash
   docker compose -f ./cmd/broadside/configs/examples/docker-compose-postgres.yaml up postgres --wait -d
   ```

2. **Verify container is healthy**:
   ```bash
   docker ps | grep broadside-postgres
   ```

3. **Run the test**:
   ```bash
   go run ./cmd/broadside --config ./cmd/broadside/configs/examples/test-postgres.yaml
   ```

4. **Clean up**:
   ```bash
   docker compose -f ./cmd/broadside/configs/examples/docker-compose-postgres.yaml down postgres -v
   ```

**Performance Note**: Docker on localhost provides quick setup but is not suitable for rigorous performance benchmarking. For accurate benchmarks, use a dedicated PostgreSQL instance on a VM or cloud provider.

---

### docker-compose-postgres.yaml

**Purpose**: Docker Compose configuration for local PostgreSQL instance

**Configuration**:
- PostgreSQL 15
- Port 5433 (avoids conflicts with existing PostgreSQL instances)
- Named volume for data persistence (`broadside-postgres-data`)
- Health checks for startup verification
- User: `broadside`, Database: `broadside_test`

**Managing the Container**:

Start:
```bash
docker compose -f ./cmd/broadside/configs/examples/docker-compose-postgres.yaml up postgres -d
```

Check status:
```bash
docker compose -f ./cmd/broadside/configs/examples/docker-compose-postgres.yaml ps
```

View logs:
```bash
docker compose -f ./cmd/broadside/configs/examples/docker-compose-postgres.yaml logs postgres
```

Stop:
```bash
docker compose -f ./cmd/broadside/configs/examples/docker-compose-postgres.yaml stop postgres
```

Stop and remove volume:
```bash
docker compose -f ./cmd/broadside/configs/examples/docker-compose-postgres.yaml down postgres -v
```

**Connecting with psql**:
```bash
PGPASSWORD=broadside psql -h localhost -p 5433 -U broadside -d broadside_test
```

---

## Creating Your Own Configuration

1. **Choose a starting point**:
   - Use `test-inmemory.yaml` for quick iterations
   - Use `test-postgres.yaml` for realistic load testing

2. **Copy to configs/ directory**:
   ```bash
   cp cmd/broadside/configs/examples/test-postgres.yaml cmd/broadside/configs/my-test.yaml
   ```

3. **Customise for your needs**:
   - Update database connection parameters
   - Adjust test duration and warmup period
   - Configure queue/job set distribution to match your workload
   - Set submission and query rates based on your production traffic
   - Add or remove scheduled actions

4. **Run your test**:
   ```bash
   go run ./cmd/broadside --config ./cmd/broadside/configs/my-test.yaml
   ```

## Configuration Tips

### For Development
- Keep test durations short (30s-1m)
- Use `inMemory: true` for fastest iteration
- Reduce historical job counts
- Disable actions if not testing them

### For Performance Benchmarking
- Use dedicated database instances (not Docker)
- Set warmup duration to 5-10% of test duration
- Run multiple iterations and report median results
- Configure production-like submission and query rates
- Document database configuration (CPU, memory, tuning parameters)

### For Stress Testing
- Gradually increase submission rates beyond production levels
- Add multiple actions scheduled close together
- Monitor database resource usage throughout the test
- Watch for backlog warnings in test output

## Typo Note

Line 23 in the original README had a typo: "cotainer" → "container" (fixed in the Docker example section above).
