package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const currentFile = "current-data"
const outFileName = "segment-"

var tempDir string

var ErrNotFound = fmt.Errorf("record does not exist")
var ErrWrongDataType = fmt.Errorf("wrong data type")

type hashIndex map[string]int64

type entryWithResp struct {
	e        entry
	response chan error
}

type Segment struct {
	out       *os.File
	outPath   string
	outOffset int64
	index     hashIndex
}

type Db struct {
	mu        sync.RWMutex
	out       *os.File
	outPath   string
	outOffset int64
	segments  []Segment
	index     hashIndex
	queue     chan entryWithResp
}

func NewDb(filename, dir string) (*Db, error) {
	outputPath := filepath.Join(dir, filename)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		log.Fatal(err)
	}

	db := &Db{
		outPath:   outputPath,
		out:       f,
		outOffset: 0,
		index:     make(hashIndex),
		queue:     make(chan entryWithResp),
	}
	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}

	go func() {

		for el := range db.queue {
			db.mu.Lock()
			err := db.putIntoDataBase(el.e)
			db.mu.Unlock()
			if err != nil {
				el.response <- err
			}

			el.response <- nil
		}
	}()

	return db, nil
}

func (db *Db) mergeSegments() error {

	var segmentsMerged []Segment

	mergedSegment, err := createNewSegment(nil, outFileName+strconv.Itoa(len(db.segments)+1), 0, make(hashIndex))
	segmentLength := len(db.segments)
	mergedSegment.outOffset = 0
	if err != nil {
		return err
	}
	for _, el := range db.segments {
		for key := range el.index {
			segIndex, position, ok := db.getLastFromSegments(key)
			if segIndex != nil && ok {
				file, err := os.Open(db.segments[*segIndex].outPath)
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
				if int(mergedSegment.outOffset)+len(encoded) > bufSize {
					mergedSegment.out.Close()
					segmentLength++
					segmentsMerged = append(segmentsMerged, *mergedSegment)
					mergedSegment, err = createNewSegment(nil, outFileName+strconv.Itoa(segmentLength+1), 0, make(hashIndex))
					if err != nil {
						return err
					}

				}
				n, err := mergedSegment.out.Write(e.Encode())
				if err == nil {
					mergedSegment.index[key] = mergedSegment.outOffset
					mergedSegment.outOffset += int64(n)
				}
				file.Close()
			}
			for i := 0; i <= *segIndex; i++ {
				_, ok := db.segments[i].index[key]
				if ok {
					delete(db.segments[i].index, key)
				}
			}
		}
		if len(el.index) == 0 {
			os.Remove(el.outPath)
		}
	}
	mergedSegment.out.Close()
	db.segments = append(segmentsMerged, *mergedSegment)

	for i, el := range db.segments {
		err := os.Rename(el.outPath, tempDir+`\`+outFileName+strconv.Itoa(i+1))
		if err != nil {
			return err
		}
		db.segments[i].outPath = tempDir + `\` + outFileName + strconv.Itoa(i+1)
	}

	return nil
}

func (db *Db) getLastFromSegments(key string) (*int, int64, bool) {
	var currentSegment *int
	i := len(db.segments) - 1
	for i >= 0 {
		currentSegment = &i
		position, ok := db.segments[i].index[key]
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
	db.mu.Lock()
	defer db.mu.Unlock()
	var currentSegment *int
	var file *os.File
	var err error

	position, ok := db.index[key]
	if !ok {
		currentSegment, position, ok = db.getLastFromSegments(key)
		if !ok {
			return "", ErrNotFound
		}
	}

	if currentSegment != nil {
		file, err = os.Open(db.segments[*currentSegment].outPath)
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

func (db *Db) Put(key, value string) error {
	en := entry{
		key:   key,
		value: value,
	}

	i := entryWithResp{
		e:        en,
		response: make(chan error),
	}

	db.queue <- i
	return <-i.response
}

func (db *Db) putIntoDataBase(e entry) error {
	encoded := e.Encode()

	if int(db.outOffset)+len(encoded) > bufSize {
		db.Close()
		err := os.Rename(db.outPath, tempDir+`\`+outFileName+strconv.Itoa(len(db.segments)+1))
		if err != nil {
			return err
		}
		db.outPath = tempDir + `\` + outFileName + strconv.Itoa(len(db.segments)+1)
		newSeg, err := createNewSegment(db.out, db.outPath, int(db.outOffset), db.index)
		if err != nil {
			return err
		}
		newSeg.out.Close()

		db.segments = append(db.segments, *newSeg)

		outputPath := filepath.Join(tempDir, currentFile)
		f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)

		db.out = f
		db.outOffset = 0
		db.index = make(hashIndex)
		db.outPath = outputPath
		if err != nil {
			return err
		}
	}

	n, err := db.out.Write(e.Encode())
	if err == nil {
		db.index[e.key] = db.outOffset
		db.outOffset += int64(n)
		return nil
	}

	return err
}

func createNewSegment(outF *os.File, outPath string, outOffset int, index hashIndex) (*Segment, error) {
	if outF == nil {
		outPath = filepath.Join(tempDir, outPath)
		f, err := os.OpenFile(outPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			return nil, err
		}
		outF = f
	}
	newSeg := &Segment{
		out:       outF,
		outPath:   outPath,
		outOffset: int64(outOffset),
		index:     index,
	}

	return newSeg, nil
}

func (db *Db) GetInt64(key string) (int64, error) {
	stringValue, err := db.Get(key)
	if err != nil {
		return 0, err
	}

	value, err := strconv.ParseInt(stringValue, 10, 64)
	if err != nil {
		return 0, ErrWrongDataType
	}

	return value, nil
}

func (db *Db) PutInt64(key string, value int64) error {
	e := entry{
		key:   key,
		valueType: "int64",
		value: strconv.FormatInt(value, 10),
	}

	n, err := db.out.Write(e.Encode())
	if err == nil {
		db.index[key] = db.outOffset
		db.outOffset += int64(n)
	}

	return err
}