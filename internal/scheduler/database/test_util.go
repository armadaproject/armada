package database

import (
	"embed"
	_ "embed"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"

	"github.com/G-Research/armada/internal/common/database"
)

//go:embed testdata/migrations/*.sql
var fs embed.FS

func Example() {
	d, err := iofs.New(fs, "testdata/migrations")
	if err != nil {
		log.Fatal(err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", d, "postgres://postgres@localhost/postgres?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	err = m.Up()
	if err != nil {
		// ...
	}
	// ...
}
func WithTestDb(action func(queries *Queries, db *pgxpool.Pool) error) error {
	// TODO: make the scheduler database properly support migrations
	migrations := []database.Migration{
		database.NewMigration(1, "initial", SchemaTemplate()),
	}
	return database.WithTestDb(migrations, func(db *pgxpool.Pool) error {
		return action(New(db), db)
	})
}
