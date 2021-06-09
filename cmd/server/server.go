package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/KPI-KMD/lab3-term2/httptools"
	"github.com/KPI-KMD/lab3-term2/signal"
)

var port = flag.Int("port", 8080, "server port")
var db = flag.String("db", "http://database:8070/db/", "database url")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

func main() {
	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		_ = os.Setenv(confResponseDelaySec, "1")
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)
		k, ok := r.URL.Query()["key"]
		rw.Header().Set("content-type", "application/json")
		if !ok || len(k[0]) < 1 {
			rw.WriteHeader(http.StatusBadRequest)
			return
		}

		v, err := http.Get(*db + k[0])
		if err != nil {
			log.Printf("Failed to get data from db: %s", err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer v.Body.Close()

		if v.StatusCode != http.StatusOK {
			if v.StatusCode == http.StatusNotFound {
				rw.WriteHeader(http.StatusNotFound)
			} else {
				rw.WriteHeader(http.StatusInternalServerError)
			}

			return
		}

		b, err := ioutil.ReadAll(v.Body)
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			log.Printf("Failed to read response body: %s", err)
			return
		}
		rw.WriteHeader(http.StatusOK)
		if _, err = rw.Write(b); err != nil {
			log.Printf("Failed to write response body: %s", err)
		}
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	t := time.Now().Format("2006-01-02")
	body := []byte(fmt.Sprintf(`{"value": "%s"}`, t))
	r, err := http.Post(
		*db+"KPI-KMD",
		"application/json",
		bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Failed to upload current time to database: %s", err)
	}

	if r.StatusCode != http.StatusOK {
		log.Printf("Failed to upload current time to database: %s", r.Status)
	}

	log.Print("Date uploaded")
	server.Start()
	signal.WaitForTerminationSignal()
}
