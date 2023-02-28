# Run

Run this when the script completes

```bash
go run .//armadactl/main.go create queue test --priorityFactor 1
go run ./cmd/armadactl/main.go submit ./devcontainer/demo/jobs.yaml
go run ./cmd/armadactl/main.go watch test job-set-1
```

# View Lookout

If you want to view lookout, click the bottom right icon and select "Open in Vscode Desktop".

Forward these ports:

- 8089: Lookout
- 8082: Binoculars
- 10000: Lookoutv2 API
- 8080: Armada Server API

and go to: http://localhost:8089