package main

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	var (
		listenAddr = kingpin.Flag(
			"web.listen-address",
			"Address on which to listen for remote write requests.",
		).Default(":8080").String()
		clusterAddr = kingpin.Flag(
			"cluster.listen-address",
			"Address to listen on for cluster communication",
		).Default(":8081").String()
		peers = kingpin.Flag(
			"cluster.peers",
			"Peers in which to try to join the cluster",
		).Strings()
		penalty = kingpin.Flag(
			"penalty",
			"Grace period between the samples of a server that died and ingester samples from the duplicate server.",
		).Default("15s").Duration()
		failoverAfter = kingpin.Flag(
			"failover-after",
			"The amount of time for a server not to receive requests before failing over.",
		).Default("30s").Duration()
	)
	promlogConfig := &promlog.Config{}
	flag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.Parse()

	logger := promlog.New(promlogConfig)

	ctx := context.Background()
	p, err := newProxy(ctx, logger, proxyCfg{
		penalty:       *penalty,
		failoverAfter: *failoverAfter,
		clusterAddr:   *clusterAddr,
		peers:         *peers,
	})
	if err != nil {
		panic(err)
	}

	err = p.Join(ctx)
	if err != nil {
		panic(err)
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			members := []string{}
			for _, member := range p.memberlist.Members() {
				members = append(members, member.Name)
			}
			level.Info(logger).Log(
				"msg", "memberlist state",
				"members", strings.Join(members, ","),
				"state.ActiveMember", p.state.ActiveMember,
				"state.HighestSentTimestamp", p.state.HighestSentTimestamp,
				"state.RequestLastReceived", p.state.RequestLastReceived,
			)
		}
	}()
	http.ListenAndServe(*listenAddr, p)
}
