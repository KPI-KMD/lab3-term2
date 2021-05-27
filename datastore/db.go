package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

var segments []Db

const currentFile = "current-data"
const outFileName = "segment-"

var tempDir string

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	out       *os.File
	outPath   string
	outOffset int64

	index hashIndex
}

//outFileName+strconv.Itoa(len(segments)+1
func NewDb(filename, dir string) (*Db, error) {
	outputPath := filepath.Join(dir, filename)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	db := &Db{
		outPath:   outputPath,
		out:       f,
		outOffset: 0,
		index:     make(hashIndex),
	}
	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return db, nil
}

func mergeSegments() error {
	var segmentsMerged []Db
	mergedDb, err := NewDb(outFileName+strconv.Itoa(len(segments)+1), tempDir)
	mergedDb.outOffset = 0
	if err != nil {
		return err
	}
	for _, el := range segments {
		for key := range el.index {
			segIndex, position, ok := getLastFromSegments(key)
			if segIndex != nil && ok {
				file, err := os.Open(segments[*segIndex].outPath)
				if err != nil {
					return err
				}
				_, err = file.Seek(position, 0)
				if err != nil {
					return err
				}

				reader := bufio.NewReader(file)
				value, err := readValue(reader)
				if err != nil {
					return err
				}
				e := entry{
					key:   key,
					value: value,
				}
				encoded := e.Encode()
				if int(mergedDb.outOffset)+len(encoded) > bufSize {
					mergedDb.Close()
					segmentsMerged = append(segmentsMerged, *mergedDb)
					mergedDb, err = NewDb(outFileName+strconv.Itoa(len(segments)+1+len(segmentsMerged)), tempDir)
					if err != nil {
						return err
					}

				}
				n, err := mergedDb.out.Write(e.Encode())
				if err == nil {
					mergedDb.index[key] = mergedDb.outOffset
					mergedDb.outOffset += int64(n)
				}
				file.Close()
			}
			for i := 0; i <= *segIndex; i++ {
				_, ok := segments[i].index[key]
				if ok {
					delete(segments[i].index, key)
				}
			}
		}
		if len(el.index) == 0 {
			os.Remove(el.outPath)
		}
	}
	mergedDb.Close()
	segments = append(segmentsMerged, *mergedDb)
	for i, el := range segments {
		err := os.Rename(el.outPath, tempDir+`\`+outFileName+strconv.Itoa(i+1))
		if err != nil {
			return err
		}
		segments[i].outPath = tempDir + `\` + outFileName + strconv.Itoa(i+1)
	}
	return nil
}

func getLastFromSegments(key string) (*int, int64, bool) {
	var currentSegment *int
	i := len(segments) - 1
	for i >= 0 {
		currentSegment = &i
		position, ok := segments[i].index[key]
		if ok {
			return currentSegment, position, ok

		} else {
			i--
		}
	}
	return nil, 0, false
}

const bufSize = 200

func (db *Db) recover() error {
	input, err := os.Open(db.outPath)
	if err != nil {
		return err
	}
	defer input.Close()

	var buf [bufSize]byte
	in := bufio.NewReaderSize(input, bufSize)
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			db.index[e.key] = db.outOffset
			db.outOffset += int64(n)
		}
	}
	return err
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	var currentSegment *int
	var file *os.File
	var err error

	position, ok := db.index[key]
	if !ok {
		currentSegment, position, ok = getLastFromSegments(key)
		if !ok {
			return "", ErrNotFound
		}
	}

	if currentSegment != nil {
		file, err = os.Open(segments[*currentSegment].outPath)
	} else {
		file, err = os.Open(db.outPath)
	}
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}

	return value, nil
}

func (db *Db) Put(key, value string) (*Db, error) {
	e := entry{
		key:   key,
		value: value,
	}
	encoded := e.Encode()
	if int(db.outOffset)+len(encoded) > bufSize {
		db.Close()
		err := os.Rename(db.outPath, tempDir+`\`+outFileName+strconv.Itoa(len(segments)+1))
		if err != nil {
			return nil, err
		}
		db.outPath = tempDir + `\` + outFileName + strconv.Itoa(len(segments)+1)
		segments = append(segments, *db)
		db, err = NewDb(currentFile, tempDir)
		if err != nil {
			return nil, err
		}
	}

	n, err := db.out.Write(e.Encode())
	if err == nil {
		db.index[key] = db.outOffset
		db.outOffset += int64(n)
		return db, nil
	}
	return nil, err
}
