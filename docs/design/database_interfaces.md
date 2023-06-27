# Armada Database Interfaces

## Problem Description

Open source projects should not be hard coded to a particular Database. Armada currently only allows users to use Postgres. This project is to build interfaces around our connections to Postgres so we can allow other databases.

## Solution

1. Introduce base common database interfaces that can be shared reused by all components (Lookout, Scheduler, Scheduler Ingester).
2. Add interfaces that abstracts the hardcoded Postgres configuration.
3. Add interfaces around `pgx` structs.

### Functional Specification (API Description)

Most of the components (Lookout, Scheduler, Scheduler Ingester) rely on [PostgresConfig](github.com/armadaproject/armada/internal/armada/configuration) to connect to external databases, we can avoid hardcoding the configuration of those components to use `PostgresConfig` but defining a generic `DatabaseConfig` interface that's when implemented will provide those components with the necessary details to connect to databases.

    /**
    Components configuration (e.g. ArmadaConfiguration) can now make use of this interface instead of hardcoding PostgresConfig.
    */
    type DatabaseConfig interface {
    	GetMaxOpenConns() int
    	GetMaxIdleConns() int
    	GetConnMaxLifetime() time.Duration
    	GetConnectionString() string
    }

    type DatabaseConnection interface {
    	GetConnection() (*sql.DB, error)
    	GetConfig() DatabaseConfig
    }

The existing configurations can then be tweaked to use the new generic `DatabaseConfig` interface instead of hardcoding `PostgresConfig`

    type ArmadaConfig struct {
        ...
        Postgres 		 PostgresConfig // this can be replace now
        PulsarDedupStore DatabaseConfig // new property
    }
