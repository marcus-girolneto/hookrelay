package memstore_test

import (
	"testing"

	"github.com/marcus-girolneto/hookrelay/internal/memstore"
	"github.com/marcus-girolneto/hookrelay/internal/storetest"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(*testing.T) storetest.Store { return memstore.New() })
}
