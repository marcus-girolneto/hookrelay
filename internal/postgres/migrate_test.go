package postgres

import (
	"strings"
	"testing"
	"testing/fstest"
)

func TestLoadMigrationsOrdersAndFilters(t *testing.T) {
	t.Parallel()
	fsys := fstest.MapFS{
		"m/0002_b.sql":  {Data: []byte("SELECT 2")},
		"m/0001_a.sql":  {Data: []byte("SELECT 1")},
		"m/0010_c.sql":  {Data: []byte("SELECT 10")},
		"m/README.md":   {Data: []byte("ignored")},
		"m/nested/x.go": {Data: []byte("ignored")},
	}
	ms, err := loadMigrations(fsys, "m")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, m := range ms {
		got = append(got, m.Version)
	}
	if strings.Join(got, ",") != "0001_a,0002_b,0010_c" {
		t.Fatalf("versions = %v", got)
	}
	if ms[0].SQL != "SELECT 1" {
		t.Fatalf("sql = %q", ms[0].SQL)
	}
	if _, err := loadMigrations(fsys, "missing"); err == nil {
		t.Fatal("expected error for missing directory")
	}
}

func TestEmbeddedMigrations(t *testing.T) {
	t.Parallel()
	ms, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) == 0 || ms[0].Version != "0001_init" || !strings.Contains(ms[0].SQL, "CREATE TABLE deliveries") {
		t.Fatalf("unexpected embedded migrations: %+v", ms)
	}
}
