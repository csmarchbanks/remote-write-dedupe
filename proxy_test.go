package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProxy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr1 := "127.0.0.1:34000"
	addr2 := "127.0.0.1:34001"
	p1, err := newProxy(ctx, nil, proxyCfg{
		clusterAddr:   addr1,
		peers:         []string{addr1, addr2},
		penalty:       1 * time.Second,
		failoverAfter: time.Second,
	})
	require.NoError(t, err)

	p2, err := newProxy(ctx, nil, proxyCfg{
		clusterAddr:   addr2,
		peers:         []string{addr1, addr2},
		penalty:       1 * time.Second,
		failoverAfter: time.Second,
	})
	require.NoError(t, err)

	s1 := httptest.NewServer(p1)
	defer s1.Close()
	s1URL, err := url.Parse(s1.URL)
	require.NoError(t, err)

	s2 := httptest.NewServer(p2)
	defer s2.Close()
	s2URL, err := url.Parse(s2.URL)
	require.NoError(t, err)

	go func() {
		err := p1.Join(ctx)
		require.NoError(t, err)
	}()
	go func() {
		err := p2.Join(ctx)
		require.NoError(t, err)
	}()

	time.Sleep(5 * time.Second)
	assert.NotEqual(t, p1.IsActive(), p2.IsActive())

	var requests int32
	remoteServer := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&requests, 1)
		}),
	)
	defer remoteServer.Close()

	remoteURL, err := url.Parse(remoteServer.URL)
	require.NoError(t, err)
	write1, err := remote.NewWriteClient("write1", &remote.ClientConfig{
		URL:     &config.URL{URL: remoteURL},
		Timeout: model.Duration(5 * time.Second),
		HTTPClientConfig: config.HTTPClientConfig{
			ProxyURL: config.URL{URL: s1URL},
		},
	})
	require.NoError(t, err)
	write2, err := remote.NewWriteClient("write2", &remote.ClientConfig{
		URL:     &config.URL{URL: remoteURL},
		Timeout: model.Duration(5 * time.Second),
		HTTPClientConfig: config.HTTPClientConfig{
			ProxyURL: config.URL{URL: s2URL},
		},
	})
	require.NoError(t, err)

	ts := time.Now()
	millis := ts.Unix() * 1000
	samples := []prompb.TimeSeries{
		{
			Labels:  []prompb.Label{{Name: "hello", Value: "world"}},
			Samples: []prompb.Sample{{Timestamp: millis, Value: 1.0}},
		},
	}
	require.NoError(t, err)

	err = sendWithRetries(ctx, write1, samples)
	assert.NoError(t, err)
	err = sendWithRetries(ctx, write2, samples)
	assert.NoError(t, err)

	time.Sleep(time.Second)
	assert.Equal(t, int32(1), atomic.LoadInt32(&requests))
	assert.Equal(t, millis, p1.HighestSentTimestamp())
	assert.Equal(t, millis, p2.HighestSentTimestamp())

	assert.True(t, p1.RequestLastReceived().After(ts))
	assert.True(t, p2.RequestLastReceived().After(ts))
}

func sendWithRetries(ctx context.Context, store remote.WriteClient, samples []prompb.TimeSeries) (err error) {
	backoff := 500 * time.Millisecond
	buf, err := writeRequest(samples)
	if err != nil {
		return err
	}

	for i := 0; i < 3; i++ {
		err = store.Store(ctx, buf)
		if err == nil {
			return nil
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	return err
}

func writeRequest(samples []prompb.TimeSeries) ([]byte, error) {
	req := &prompb.WriteRequest{
		Timeseries: samples,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	compressed := snappy.Encode(nil, data)
	return compressed, nil
}
