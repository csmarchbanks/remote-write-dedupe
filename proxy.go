package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/prompb"
)

type proxy struct {
	logger log.Logger
	client *http.Client

	memberlist *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue

	mux   sync.RWMutex
	state state

	// How long to penalize sending samples so as to not disrupt rates.
	// In milliseconds just like remote write protocol.
	penalty       int64
	failoverAfter time.Duration
}

type proxyCfg struct {
	clusterAddr   string
	peers         []string
	penalty       time.Duration
	failoverAfter time.Duration
}

func newProxy(ctx context.Context, logger log.Logger, cfg proxyCfg) (*proxy, error) {
	p := &proxy{
		logger:        logger,
		client:        &http.Client{},
		penalty:       cfg.penalty.Milliseconds(),
		failoverAfter: cfg.failoverAfter,
	}

	bindAddr, portStr, err := net.SplitHostPort(cfg.clusterAddr)
	if err != nil {
		return nil, errors.Wrap(err, "invalid clusterAddr")
	}
	bindPort, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid port for %s", cfg.clusterAddr)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, errors.Wrap(err, "could not get hostname")
	}

	listCfg := memberlist.DefaultLANConfig()
	listCfg.Delegate = p
	listCfg.BindAddr = bindAddr
	listCfg.BindPort = bindPort
	listCfg.Name = net.JoinHostPort(hostname, portStr)

	list, err := memberlist.Create(listCfg)
	if err != nil {
		return nil, err
	}
	p.memberlist = list

	members, err := p.resolveMembers(ctx, cfg.peers)
	if err != nil {
		return nil, errors.Wrap(err, "resolving members")
	}
	_, err = list.Join(members)
	if err != nil {
		return nil, err
	}

	p.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: list.NumMembers,
	}

	go p.loop(ctx)
	return p, nil
}

func (p *proxy) loop(ctx context.Context) {
	// Wait for a full failover interval to make sure everything is stable at
	// the beginning.
	timer := time.NewTimer(p.failoverAfter)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		now := time.Now()
		failoverTime := p.RequestLastReceived().Add(p.failoverAfter)

		// Failover if we are past the failover time.
		if now.After(failoverTime) {
			p.mux.Lock()
			newActive := p.nextActiveMember()
			level.Warn(p.logger).Log("msg", "Failing over", "from", p.state.ActiveMember, "to", newActive)
			p.state.ActiveMember = newActive
			// Set request last received to now so this write isn't quickly
			// changed by someone else. Another candidate will not be able to
			// try until another failover period is over.
			p.state.RequestLastReceived = time.Now()
			p.broadcasts.QueueBroadcast(p.state)
			p.mux.Unlock()

			timer.Reset(p.failoverAfter)
			continue
		}

		// Sleep until the next potential failover.
		targetDuration := failoverTime.Sub(now)
		timer.Reset(targetDuration)
	}
}

func (p *proxy) Ready() bool {
	p.mux.RLock()
	defer p.mux.RUnlock()
	return p.state.ActiveMember != ""
}

func (p *proxy) IsActive() bool {
	p.mux.RLock()
	defer p.mux.RUnlock()
	return p.state.ActiveMember == p.memberlist.LocalNode().Name
}

func (p *proxy) HighestSentTimestamp() int64 {
	p.mux.RLock()
	defer p.mux.RUnlock()
	return p.state.HighestSentTimestamp
}

func (p *proxy) RequestLastReceived() time.Time {
	p.mux.RLock()
	defer p.mux.RUnlock()
	return p.state.RequestLastReceived
}

// nextActiveMember must be used inside of a lock.
func (p *proxy) nextActiveMember() string {
	members := p.memberlist.Members()
	sort.Slice(members, func(i, j int) bool {
		return members[i].Name < members[j].Name
	})

	for _, member := range members {
		if member.Name > p.state.ActiveMember {
			return member.Name
		}
	}
	// If we make it through the whole loop without finding the next active we
	// need to choose the first.
	return members[0].Name
}

func (p *proxy) NodeMeta(limit int) []byte {
	return []byte{}
}

func (p *proxy) NotifyMsg(msg []byte) {
	p.MergeRemoteState(msg, false)
}

func (p *proxy) GetBroadcasts(overhead, limit int) [][]byte {
	return p.broadcasts.GetBroadcasts(overhead, limit)
}

func (p *proxy) LocalState(join bool) []byte {
	p.mux.RLock()
	state := p.state
	p.mux.RUnlock()
	return state.Message()
}

func (p *proxy) MergeRemoteState(buf []byte, join bool) {
	incoming, err := parseState(buf)
	if err != nil {
		panic(fmt.Sprintf("could not parse: %s", string(buf)))
	}
	level.Debug(p.logger).Log(
		"msg", "MergeRemoteState",
		"join", join,
		"incoming.ActiveMember", incoming.ActiveMember,
		"incoming.HighestSentTimestamp", incoming.HighestSentTimestamp,
		"incoming.RequestLastReceived", incoming.RequestLastReceived,
	)

	p.mux.Lock()
	defer p.mux.Unlock()

	if incoming.ActiveMember != "" && incoming.ActiveMember != p.state.ActiveMember {
		p.state.ActiveMember = incoming.ActiveMember
	}

	if incoming.HighestSentTimestamp > p.state.HighestSentTimestamp {
		p.state.HighestSentTimestamp = incoming.HighestSentTimestamp
	}
	if incoming.RequestLastReceived.After(p.state.RequestLastReceived) {
		p.state.RequestLastReceived = incoming.RequestLastReceived
	}
}

func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !p.Ready() {
		http.Error(w, "proxy not ready", http.StatusServiceUnavailable)
		return
	}
	start := time.Now()

	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var writeRequest prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &writeRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	maxTS := int64(math.MinInt64)
	for _, ts := range writeRequest.Timeseries {
		for _, sample := range ts.Samples {
			if sample.Timestamp > maxTS {
				maxTS = sample.Timestamp
			}
		}
	}
	if !p.IsActive() {
		// Mark samples that are within penalty of the highest timestamp to be
		// considered succesfully sent.
		if maxTS < p.HighestSentTimestamp()+p.penalty {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		// A 5XX error will tell Prometheus to retry.
		http.Error(w, "active replica has not sent these yet", http.StatusServiceUnavailable)
		return
	}

	proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), bytes.NewBuffer(compressed))
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	proxyReq.Header = r.Header

	resp, err := p.client.Do(proxyReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	io.Copy(w, resp.Body)
	resp.Body.Close()
	w.WriteHeader(resp.StatusCode)

	// TODO: Do the following in error cases too.
	p.mux.Lock()
	update := false
	if resp.StatusCode < 500 && maxTS > p.state.HighestSentTimestamp {
		p.state.HighestSentTimestamp = maxTS
		update = true
	}
	if start.After(p.state.RequestLastReceived) {
		p.state.RequestLastReceived = start
		update = true
	}
	if update {
		p.state.localVersion++
		p.broadcasts.QueueBroadcast(p.state)
	}
	p.mux.Unlock()
}

func (p *proxy) resolveMembers(ctx context.Context, members []string) ([]string, error) {
	var otherMembers []string

	me := p.memberlist.LocalNode().Addr

	for _, peer := range members {
		host, port, err := net.SplitHostPort(peer)
		if err != nil {
			return nil, errors.Wrapf(err, "split host/port for peer %s", peer)
		}

		// See if the member is a raw IP address and add it.
		if ip := net.ParseIP(host); ip != nil {
			if me.Equal(ip) {
				continue
			}
			otherMembers = append(otherMembers, net.JoinHostPort(ip.String(), port))
		}

		// Lookup DNS addresses and add any IP addresses found.
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			// XXX: Is this what we want? It could be dangerous and lead to
			// split-brain.  Perhaps a reconciliation loop that continues to
			// resolve members and add them in the future?
			//
			// Do not fail in case of DNS resolution errors.
			level.Warn(p.logger).Log("msg", "Unable to resolve host", "host", host, "err", err)
			continue
		}

		for _, ip := range ips {
			if me.Equal(ip.IP) {
				continue
			}
			otherMembers = append(otherMembers, net.JoinHostPort(ip.String(), port))
		}
	}

	return otherMembers, nil
}
