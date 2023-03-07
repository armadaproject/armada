go run ./cmd/lookout/main.go --migrateDatabase
dlv debug --listen=:4000 --headless=true --log=true --accept-multiclient --api-version=2 --continue --output __debug_lookout ./cmd/lookout/main.go --
