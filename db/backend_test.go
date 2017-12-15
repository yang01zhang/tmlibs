package db

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cmn "github.com/tendermint/tmlibs/common"
)

func cleanupDBDir(dir, name string) {
	os.RemoveAll(path.Join(dir, name) + ".db")
}

func testBackendGetSetDelete(t *testing.T, backend string) {
	// Default
	dir, dirname := cmn.Tempdir(fmt.Sprintf("test_backend_%s_", backend))
	defer dir.Close()
	db := NewDB("testdb", backend, dirname)

	// A nonexistent key should return nil, even if the key is empty
	require.Nil(t, db.Get([]byte("")))

	// A nonexistent key should return nil, even if the key is nil
	require.Nil(t, db.Get(nil))

	// A nonexistent key should return nil.
	key := []byte("abc")
	require.Nil(t, db.Get(key))

	// Set empty value.
	db.Set(key, []byte(""))
	require.NotNil(t, db.Get(key))
	require.Empty(t, db.Get(key))

	// Set nil value.
	db.Set(key, nil)
	require.NotNil(t, db.Get(key))
	require.Empty(t, db.Get(key))

	// Delete.
	db.Delete(key)
	require.Nil(t, db.Get(key))
}

func TestBackendsGetSetDelete(t *testing.T) {
	for dbType, _ := range backends {
		testBackendGetSetDelete(t, dbType)
	}
}

func assertPanics(t *testing.T, dbType, name string, fn func()) {
	defer func() {
		r := recover()
		assert.NotNil(t, r, cmn.Fmt("expecting %s.%s to panic", dbType, name))
	}()

	fn()
}

func TestBackendsNilKeys(t *testing.T) {

	// Test all backends.
	for dbType, creator := range backends {

		// Setup
		name := cmn.Fmt("test_%x", cmn.RandStr(12))
		db, err := creator(name, "")
		assert.Nil(t, err)

		assertPanics(t, dbType, "get", func() { db.Get(nil) })
		assertPanics(t, dbType, "has", func() { db.Has(nil) })
		assertPanics(t, dbType, "set", func() { db.Set(nil, []byte("abc")) })
		assertPanics(t, dbType, "setsync", func() { db.SetSync(nil, []byte("abc")) })
		assertPanics(t, dbType, "delete", func() { db.Delete(nil) })
		assertPanics(t, dbType, "deletesync", func() { db.DeleteSync(nil) })

		// Cleanup
		db.Close()
		cleanupDBDir("", name)
	}
}

func TestGoLevelDBBackendStr(t *testing.T) {
	name := cmn.Fmt("test_%x", cmn.RandStr(12))
	db := NewDB(name, GoLevelDBBackendStr, "")
	defer cleanupDBDir("", name)
}
