package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

func main() {
	net := flag.String("statshouse-net", "unix", "")
	addr := flag.String("statshouse-addr", "127.0.0.1:13347", "")
	actor := flag.Int("statshouse-actor", 0, "")

	flag.Parse()

	client := rpc.NewClient(rpc.ClientWithLogf(log.Printf))
	apiClient := tlmetadata.Client{
		Client:  client,
		Network: *net,
		Address: *addr,
		ActorID: int64(*actor),
	}
	for i := 50000; i < 100000; i++ {
		event := tlmetadata.Event{}
		err := apiClient.EditEntitynew(context.Background(), tlmetadata.EditEntitynew{
			FieldsMask: 0,
			Event: tlmetadata.Event{
				Id:        format.PrometheusConfigID,
				Name:      "prom-config",
				EventType: format.PromConfigEvent,
				Version:   int64(i),
				Data:      "",
			},
		}, nil, &event)
		if err == nil {
			fmt.Println("SAVE1", event.Version)
			break
		}
	}
	for i := 50000; i < 100000; i++ {
		event := tlmetadata.Event{}
		err := apiClient.EditEntitynew(context.Background(), tlmetadata.EditEntitynew{
			FieldsMask: 0,
			Event: tlmetadata.Event{
				Id:        format.PrometheusGeneratedConfigID,
				Name:      "prom-static-config",
				EventType: format.PromConfigEvent,
				Version:   int64(i),
				Data:      "",
			},
		}, nil, &event)
		if err == nil {
			fmt.Println("SAVE2", event.Version)
			break
		}
	}
	//if err != nil {
	//	panic(err)
	//	err := apiClient.GetEntity(context.Background(), tlmetadata.GetEntity{
	//		Id:      format.PrometheusGeneratedConfigID,
	//		Version: int64(i),
	//	}, nil, &event)
	//	if err == nil {
	//		fmt.Println("VERSION IS", event.Version)
	//	}
}
