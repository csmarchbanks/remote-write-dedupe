# Remote Write Dedupe

Status: Proof of Concept, do not use in production yet :)

Remote write Dedupe is a deduplicating proxy for
[Prometheus](https://prometheus.io/) remote write. It is designed to be ran as
a sidecar to each Prometheus instance you want to deduplicate data from, and
will try to select only one active replica to forward metrics from. This is
accomplished using gossip, which means in certain scenarios it will be possible
for multiple proxies to think they are the active proxy, and each send data.
This is intentional as in many cases it is preferable to send inconsistent data
rather than block all writes in the case of failure.

To use this proxy, configure a `proxy_url` in your Prometheus remote write
configuration like:
```
remote_write:
  - url: http://some-remote-write-upstream/receive
    proxy_url: http://127.0.0.1:8080
```
