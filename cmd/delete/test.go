package main

import (
	"context"
	"log"
	"strconv"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
)

func main() {

	c := &tlmetadata.Client{
		Client:  rpc.NewClient(rpc.ClientWithLogf(log.Printf)),
		Network: "tcp4",
		Address: "localhost:2442",
	}
	resp := tlmetadata.GetMappingResponse{}
	for i := 0; i < 9_000_000_0; i++ {
		req := tlmetadata.GetMapping{
			Metric: "metric",
			Key:    strconv.FormatInt(int64(i), 10),
		}
		req.SetCreateIfAbsent(true)
		err := c.GetMapping(context.Background(), req, nil, &resp)
		if err != nil {
			panic(err)
		}
		if !resp.IsCreated() {
			panic(resp.TLName())
		}
		statshouse.Metric("test_mapping", statshouse.Tags{}).Count(1)
	}
}
