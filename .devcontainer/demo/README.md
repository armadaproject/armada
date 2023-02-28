# Run

Run this when the script completes

```bash
go run ./cmd/armadactl/main.go create queue test --priorityFactor 1
go run ./cmd/armadactl/main.go submit ./example/jobs.yaml
go run ./cmd/armadactl/main.go watch test job-set-1
```