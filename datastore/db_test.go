package datastore

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	tempDir = dir
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(currentFile, dir)
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
		db, err = NewDb(currentFile, dir)
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

	db, err := NewDb(currentFile, dir)
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
		if numOfSegs != 5 {
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

	t.Run("check merge", func(t *testing.T) {

		err := db.mergeSegments()
		if err != nil {
			t.Errorf("Error while merging: %s", err)
		}
		if len(db.segments) != 2 {
			t.Errorf("Wrong number of segments: %d, expected %d", len(db.segments), 2)
		}

		value, err := db.Get("key3")
		if err != nil {
			t.Errorf("Cannot get %s: %s", pairs[0], err)
		}
		if value != "value3" {
			t.Errorf("Bad value returned expected %s, got %s", "value3", value)
		}

	})
}
