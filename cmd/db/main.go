package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/KPI-KMD/lab3-term2/datastore"
	"github.com/KPI-KMD/lab3-term2/httptools"
	"github.com/KPI-KMD/lab3-term2/signal"
)

var dbDir = flag.String("dir", ".", "database directory")
var port = flag.Int("port", 8070, "database server port")

const currentFile = "current-data"

func main() {
	flag.Parse()
	db, err := datastore.NewDb(currentFile, *dbDir, 10485760, true)
	if err != nil {
		log.Fatalf("Failed to start database: %s", err)
	}
	db.Put("test", "example")
	log.Printf("Database started at directory: %s", *dbDir)

	h := new(http.ServeMux)
	h.HandleFunc("/db/", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "application/json")
		k := strings.Split(r.URL.Path, "/")[2]
		encoder := json.NewEncoder(rw)

		if r.Method == http.MethodGet {
			log.Printf("GET request for %s", k)
			v, err := db.Get(k)
			if err != nil {
				//log.Printf("Failed to get %s: %s", k, err)
				if err == datastore.ErrNotFound {
					rw.WriteHeader(http.StatusNotFound)
				} else if err == datastore.ErrWrongDataType {
					v, err := db.GetInt64(k)
					if err != nil {
						log.Printf("Failed to load %s: %s", k, err)
						rw.WriteHeader(http.StatusInternalServerError)
						return
					}

					log.Printf("Got %s as int64", k)
					res := struct {
						Key   string `json:"key"`
						Value int64  `json:"value"`
					}{
						Key:   k,
						Value: v,
					}
					rw.WriteHeader(http.StatusOK)
					if err := encoder.Encode(res); err != nil {
						log.Printf("Failed to write response %d: %s", v, err)
					}
					return
				} else {
					rw.WriteHeader(http.StatusInternalServerError)
				}
				return
			}

			log.Printf("Got %s as string", k)
			res := struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}{
				Key:   k,
				Value: v,
			}
			rw.WriteHeader(http.StatusOK)
			if err := encoder.Encode(res); err != nil {
				log.Printf("Failed to write response %s: %s", v, err)
			}
		} else if r.Method == http.MethodPost {
			log.Printf("POST request for %s", k)

			bytes, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Printf("Error decoding input: %s", err)
				rw.WriteHeader(http.StatusBadRequest)
				return
			}

			var jsonFields map[string]*json.RawMessage
			err = json.Unmarshal(bytes, &jsonFields)
			if err != nil {
				log.Printf("Error decoding input: %s", err)
				rw.WriteHeader(http.StatusBadRequest)
				return
			}

			var int64Value int64
			err = json.Unmarshal(*jsonFields["value"], &int64Value)
			if err != nil {
				var stringValue string
				err = json.Unmarshal(*jsonFields["value"], &stringValue)
				if err != nil {
					log.Printf("Error decoding input: %s", err)
					rw.WriteHeader(http.StatusBadRequest)
					return
				}

				log.Printf("Decoded string: %s", stringValue)
				if err := db.Put(k, stringValue); err != nil {
					rw.WriteHeader(http.StatusInternalServerError)
					log.Printf("Failed to set %s -> \"%s\": %s", k, stringValue, err)
					return
				}
				rw.WriteHeader(http.StatusOK)
				return
			}

			log.Printf("Decoded int64: %d", int64Value)
			if err := db.PutInt64(k, int64Value); err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				log.Printf("Failed to set %s -> %d: %s", k, int64Value, err)
				return
			}

			rw.WriteHeader(http.StatusOK)
		}
	})

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
