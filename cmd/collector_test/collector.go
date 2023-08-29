package main

import (
	"log"
	"os"
	"time"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/stats"
	"pgregory.net/rand"
)

func tillNextHalfPeriod(now time.Time) time.Duration {
	return now.Truncate(time.Second).Add(time.Second * 3 / 2).Sub(now)
}
func main() {

	m := map[int64]struct{}{}
	for i := 0; i < 1000000; i++ {
		m[rand.Int63n(1000000)] = struct{}{}
	}
	statshouse.Configure(log.Printf, statshouse.DefaultStatsHouseAddr, "staging")
	s := []int64{}
	for {
		time.Sleep(tillNextHalfPeriod(time.Now()))
		n := 50 + rand.Intn(50)
		s = s[:0]
		i := 0
		for k := range m {
			if i == n {
				break
			}
			s = append(s, k)
			i++
		}
		statshouse.Metric("test_uniq", statshouse.Tags{}).Uniques(s)
	}
	return
	host, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	collector, err := stats.NewCollectorManager(stats.CollectorManagerOptions{ScrapeInterval: time.Second, HostName: host}, nil, log.New(os.Stderr, "[collector]", 0))
	if err != nil {
		log.Panic(err)
	}
	defer collector.StopCollector()
	err = collector.RunCollector()
	if err != nil {
		panic(err)
	}
}
