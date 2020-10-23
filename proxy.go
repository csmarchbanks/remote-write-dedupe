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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/prompb"
)

const namespace = "rwdedupe"

var (
	requests = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "proxy",
		Name:      "request_duration_seconds",
		Help:      "How long proxy requests take to complete by URL.",
	}, []string{"url"})
	failures = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "proxy",
		Name:      "request_failures_total",
		Help:      "The count of proxy failures, this does not include error codes from upstream.",
	}, []string{"url"})
)

type proxy struct {
	cfg proxyCfg

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

func newProxy(ctx context.Context, logger log.Logger, reg prometheus.Registerer, cfg proxyCfg) (*proxy, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	p := &proxy{
		cfg:           cfg,
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

	p.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: list.NumMembers,
	}

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "proxy",
		Name:      "active",
		Help:      "Whether this instance is considered to be the active replica.",
	}, func() float64 {
		if p.IsActive() {
			return 1.0
		}
		return 0.0
	})

	go p.loop(ctx)
	return p, nil
}

// Join joins the gossip group. It will retry upon failure or if not enough
// peers were found.
func (p *proxy) Join(ctx context.Context) error {
	members, err := p.resolveMembers(ctx, p.cfg.peers)
	if err != nil {
		return errors.Wrap(err, "resolving members")
	}
	backoff := 250 * time.Millisecond
	for i := 0; i < 5; i++ {
		var n int
		n, err = p.memberlist.Join(members)
		if err != nil {
			continue
		}
		if n >= len(members) {
			break
		}
		time.Sleep(backoff)
		backoff *= 2
	}

	return err
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

	if r.URL.Path == "/ready" {
		return
	}

	err := p.handleWriteRequest(w, r)
	if err != nil {
		level.Warn(p.logger).Log("msg", "error proxying to upstream", "url", r.URL.String(), "err", err)
		failures.WithLabelValues(r.URL.String()).Inc()
	}

}

func (p *proxy) handleWriteRequest(w http.ResponseWriter, r *http.Request) error {
	start := time.Now()
	defer func() {
		requests.WithLabelValues(r.URL.String()).Observe(time.Since(start).Seconds())
	}()
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return errors.Wrap(err, "reading request body")
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return errors.Wrap(err, "decompressing snappy")
	}

	var writeRequest prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &writeRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return errors.Wrap(err, "decoding write request")
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
			return nil
		}
		// A 5XX error will tell Prometheus to retry.
		http.Error(w, "active replica has not sent these yet", http.StatusServiceUnavailable)
		// This does not count as a failure since it is working as designed.
		return nil
	}

	statusCode, err := p.handleUpstream(w, r, bytes.NewBuffer(compressed))

	p.mux.Lock()
	update := false
	if statusCode < 500 && maxTS > p.state.HighestSentTimestamp {
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
	return nil
}

func (p *proxy) handleUpstream(w http.ResponseWriter, r *http.Request, data io.Reader) (int, error) {
	proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return 0, errors.Wrap(err, "creating proxy request")
	}
	proxyReq.Header = r.Header

	resp, err := p.client.Do(proxyReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return 0, errors.Wrap(err, "doing upstream request")
	}

	io.Copy(w, resp.Body)
	resp.Body.Close()
	w.WriteHeader(resp.StatusCode)
	return resp.StatusCode, nil
}

func (p *proxy) resolveMembers(ctx context.Context, members []string) ([]string, error) {
	var otherMembers []string

	me := p.memberlist.LocalNode().Addr
	myPort := p.memberlist.LocalNode().Port

	for _, peer := range members {
		host, port, err := net.SplitHostPort(peer)
		if err != nil {
			return nil, errors.Wrapf(err, "split host/port for peer %s", peer)
		}

		// See if the member is a raw IP address and add it.
		if ip := net.ParseIP(host); ip != nil {
			if me.Equal(ip) && port == fmt.Sprintf("%d", myPort) {
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
