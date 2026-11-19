package postgres

import (
	"os"
	"testing"
)

// Require labels a test that needs PostgreSQL. It skips the test when
// ARMADA_TEST_SKIP_POSTGRES=1, before the caller opens a database connection.
func Require(t testing.TB) {
	t.Helper()
	t.Attr("requires", "postgres")
	if os.Getenv("ARMADA_TEST_SKIP_POSTGRES") == "1" {
		t.Skip("requires PostgreSQL (ARMADA_TEST_SKIP_POSTGRES=1)")
	}
}
