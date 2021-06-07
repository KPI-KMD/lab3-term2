package integration

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	gocheck "gopkg.in/check.v1"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func Test(t *testing.T) { gocheck.TestingT(t) }

type MySuite struct{}

var _ = gocheck.Suite(&MySuite{})

func (s *MySuite) TestBalancer(c *gocheck.C) {
	// TODO: Реалізуйте інтеграційний тест для балансувальникка.
	serverPool := []string{"server1:8080", "server2:8080", "server3:8080"}
	test := make(chan string, 10)

	for i := 0; i < 10; i++ {
		go func() {
			resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))

			if err != nil {
				c.Error(err)
			}

			test <- resp.Header.Get("lb-from")
		}()
		time.Sleep(time.Duration(10) * time.Millisecond)

	}
	for i := 0; i < 10; i++ {
		auth := <-test
		c.Assert(auth, gocheck.Equals, serverPool[i%3])
	}

}

func (s *MySuite) BenchmarkBalancer(c *gocheck.C) {
	for i := 0; i < c.N; i++ {
		client.Get(fmt.Sprintf("%s/api/v1/some-data?key=key1", baseAddress))
	}
}
