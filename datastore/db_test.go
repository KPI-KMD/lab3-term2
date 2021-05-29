package datastore

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestDb_Put(t *testing.T) {

	dir, err := ioutil.TempDir("", "test-db")
	tempDir = dir
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(currentFile, dir, 200, false)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	outFile, err := os.Open(filepath.Join(dir, currentFile))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}

	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1*2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(currentFile, dir, 200, false)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})
}

func TestDB_Segmentation(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	tempDir = dir
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(currentFile, dir, 200, false)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value3"},
		{"key5", "value3"},
		{"key6", "value3"},
		{"key7", "value3"},
		{"key8", "value3"},
		{"key9", "value3"},
		{"key10", "value3"},
		{"key11", "value3"},
		{"key12", "value3"},
		{"key13", "value3"},
		{"key14", "value3"},
		{"key15", "value3"},
		{"key16", "value3"},
		{"key17", "value3"},
	}

	t.Run("test segmentation", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			for _, pair := range pairs {
				err := db.Put(pair[0], pair[1])
				if err != nil {
					t.Errorf("Cannot put %s: %s", pairs[0], err)
				}
			}
		}
		numOfSegs := len(db.segments)
		if numOfSegs != 7 {
			t.Errorf("Wrong number of segments: %d", numOfSegs)
		}

		value, err := db.Get("key2")
		if err != nil {
			t.Errorf("Cannot get %s: %s", "key2", err)
		}
		if value != "value2" {
			t.Errorf("Bad value returned expected %s, got %s", "value2", value)
		}

	})

}

func TestDB_Merge(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	tempDir = dir
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(currentFile, dir, 200, true)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value3"},
		{"key5", "value3"},
		{"key6", "value3"},
		{"key7", "value3"},
		{"key8", "value3"},
		{"key9", "value3"},
		{"key10", "value3"},
		{"key11", "value3"},
		{"key12", "value3"},
		{"key13", "value3"},
		{"key14", "value3"},
		{"key15", "value3"},
		{"key16", "value3"},
		{"key17", "value3"},
	}

	t.Run("test merge", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			for _, pair := range pairs {
				err := db.Put(pair[0], pair[1])
				if err != nil {
					t.Errorf("Cannot put %s: %s", pairs[0], err)
				}
			}
		}
		numOfSegs := len(db.segments)
		if numOfSegs != 3 {
			t.Errorf("Wrong number of segments: %d", numOfSegs)
		}

	})
}

func TestDb_PutGetInt64(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	tempDir = dir
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(currentFile, dir, 200, false)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	pairsInt64 := [][]string{
		{"key1", "111"},
		{"key2", "222"},
		{"key3", "333"},
	}
	pairsNotInt64 := [][]string{
		{"key5", "dsawqe"},
		{"key6", "dsazx"},
		{"key7", "ewq"},
	}

	t.Run("putInt64/getInt64", func(t *testing.T) {

		for _, pair := range pairsInt64 {
			val, err := strconv.ParseInt(pair[1], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			err = db.PutInt64(pair[0], val)
			if err != nil {
				t.Errorf("Cannot put %s: %s", pair[0], err)
			}
			value, err := db.GetInt64(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pair[0], err)
			}
			if value != val {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], strconv.FormatInt(value, 10))
			}
		}
	})

	t.Run("getInt64wrongtype", func(t *testing.T) {

		notForInt64pair := pairsNotInt64[0]
		res, err := db.GetInt64(notForInt64pair[0])
		if err == nil {
			t.Errorf("Expected error for key %s, but got %s", notForInt64pair[0], strconv.FormatInt(res, 10))
		}
	})
}
