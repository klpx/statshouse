package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

func main() {
	path := flag.String("json-path", "/Users/e.martyn/Documents/vk/statshouse/cmd/statshouse-metadata/delete.json", "")
	metaActor := flag.Uint64("metadata-actor-id", 0, "")
	metaPath := flag.String("metadata-addr", "", "")
	metsNet := flag.String("metadata-net", "", "")
	flag.Parse()
	var events []tlmetadata.EditEntityEvent
	file, err := os.ReadFile(*path)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(file, &events)
	if err != nil {
		panic(err)
	}
	log.Printf("parse %d records", len(events))
	m := map[string]tlmetadata.EditEntityEvent{}
	for _, e := range events {
		m[e.Metric.Name] = e
	}
	events = events[:0]
	for _, e := range m {
		events = append(events, e)
	}
	sort.Slice(events, func(i, j int) bool {
		return events[i].Metric.Version < events[j].Metric.Version
	})
	c := rpc.NewClient()
	client := &tlmetadata.Client{
		Client:  c,
		Network: *metsNet,
		Address: *metaPath,
		ActorID: *metaActor,
	}
	var version int64
	var allM = map[string]tlmetadata.Event{}
	for {
		var resp tlmetadata.GetJournalResponsenew
		err := client.GetJournalnew(context.Background(), tlmetadata.GetJournalnew{
			From:  version,
			Limit: 1000,
		}, nil, &resp)
		if err != nil {
			if strings.Contains(err.Error(), "Query timeout") {
				break
			}
			panic(err)
		}
		if len(resp.Events) == 0 {
			break
		}
		version = resp.Events[len(resp.Events)-1].Version
		for _, e := range resp.Events {
			allM[e.Name] = e
		}
	}
	log.Println("events len", len(events))
	log.Println("all map len", len(allM))
	log.Println(version)

	count := 0
	for _, e := range events {
		if e.Metric.Name != "storage2_uploader_file_size" {
			continue
		}
		_, ok := allM[e.Metric.Name]
		if ok {
			count++
		}
	}
	fmt.Println("matched", count)
	return
	for _, e := range events {
		if e.Metric.Name != "storage2_uploader_file_size" {
			continue
		}
		ver, ok := allM[e.Metric.Name]
		if !ok {
			fmt.Println("skip", e.Metric.Name)
			continue
		}
		fmt.Println("find version", ver)
		var resp tlmetadata.Event
		req := tlmetadata.EditEntitynew{
			Event: tlmetadata.Event{
				FieldMask:   e.Metric.FieldMask,
				Id:          e.Metric.Id,
				Name:        e.Metric.Name,
				NamespaceId: e.Metric.NamespaceId,
				EventType:   e.Metric.EventType,
				Unused:      e.Metric.Unused,
				Version:     ver.Version,
				UpdateTime:  e.Metric.UpdateTime,
				Data:        e.Metric.Data,
			},
		}
		fmt.Println(req)
		err = client.EditEntitynew(context.Background(), req, nil, &resp)
		if err != nil {
			panic(err)
		}
		fmt.Println("save", e.Metric.Name)
	}

}
