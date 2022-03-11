package libp2p

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/ipfs/go-ipfs/repo"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	rcmgr "github.com/libp2p/go-libp2p-resource-manager"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"
)

const NetLimitDefaultFilename = "limit.json"

func ResourceManager() func(fx.Lifecycle, repo.Repo) (network.ResourceManager, Libp2pOpts, error) {
	return func(lc fx.Lifecycle, repo repo.Repo) (network.ResourceManager, Libp2pOpts, error) {
		var limiter *rcmgr.BasicLimiter
		var opts Libp2pOpts

		limitFile, err := os.Open(NetLimitDefaultFilename)
		if errors.Is(err, os.ErrNotExist) {
			log.Debug("limit file %s not found, creating a default resource manager", NetLimitDefaultFilename)
			limiter = rcmgr.NewDefaultLimiter()
		} else {
			if err != nil {
				return nil, opts, fmt.Errorf("error opening limit JSON file %s: %w",
					NetLimitDefaultFilename, err)
			}

			defer limitFile.Close() //nolint:errcheck
			limiter, err = rcmgr.NewDefaultLimiterFromJSON(limitFile)
			if err != nil {
				return nil, opts, fmt.Errorf("error parsing limit file: %w", err)
			}
		}

		libp2p.SetDefaultServiceLimits(limiter)

		ropts := []rcmgr.Option{rcmgr.WithMetrics(&rcmgrMetrics{})}
		if os.Getenv("IPFS_DEBUG_RCMGR") != "" {
			ropts = append(ropts, rcmgr.WithTrace("rcmgr.json.gz"))
		}

		rcmgr, err := rcmgr.NewResourceManager(limiter, ropts...)
		if err != nil {
			return nil, opts, fmt.Errorf("error creating resource manager: %w", err)
		}
		opts.Opts = append(opts.Opts, libp2p.ResourceManager(rcmgr))

		lc.Append(fx.Hook{
			OnStop: func(_ context.Context) error {
				return rcmgr.Close()
			}})

		return rcmgr, opts, nil
	}
}

var (
	rcmgrConnAllowed         *prometheus.CounterVec
	rcmgrConnBlocked         *prometheus.CounterVec
	rcmgrStreamAllowed       *prometheus.CounterVec
	rcmgrStreamBlocked       *prometheus.CounterVec
	rcmgrPeerAllowed         prometheus.Counter
	rcmgrPeerBlocked         prometheus.Counter
	rcmgrProtocolAllowed     *prometheus.CounterVec
	rcmgrProtocolBlocked     *prometheus.CounterVec
	rcmgrProtocolPeerBlocked *prometheus.CounterVec
	rcmgrServiceAllowed      *prometheus.CounterVec
	rcmgrServiceBlocked      *prometheus.CounterVec
	rcmgrServicePeerBlocked  *prometheus.CounterVec
	rcmgrMemoryAllowed       prometheus.Counter
	rcmgrMemoryBlocked       prometheus.Counter
)

func init() {
	const (
		direction = "direction"
		usesFD    = "usesFD"
		protocol  = "protocol"
		service   = "service"
	)

	rcmgrConnAllowed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "libp2p_rcmgr_conns_allowed_total",
			Help: "allowed connections",
		},
		[]string{direction, usesFD},
	)
	prometheus.MustRegister(rcmgrConnAllowed)

	rcmgrConnBlocked = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "libp2p_rcmgr_conns_blocked_total",
			Help: "blocked connections",
		},
		[]string{direction, usesFD},
	)
	prometheus.MustRegister(rcmgrConnBlocked)

	rcmgrStreamAllowed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "libp2p_rcmgr_streams_allowed_total",
			Help: "allowed streams",
		},
		[]string{direction},
	)
	prometheus.MustRegister(rcmgrStreamAllowed)

	rcmgrStreamBlocked = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "libp2p_rcmgr_streams_blocked_total",
			Help: "blocked streams",
		},
		[]string{direction},
	)
	prometheus.MustRegister(rcmgrStreamBlocked)

	rcmgrPeerAllowed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "libp2p_rcmgr_peers_allowed_total",
		Help: "allowed peers",
	})
	prometheus.MustRegister(rcmgrPeerAllowed)

	rcmgrPeerBlocked = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "libp2p_rcmgr_peer_blocked_total",
		Help: "blocked peers",
	})
	prometheus.MustRegister(rcmgrPeerBlocked)

	rcmgrProtocolAllowed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "libp2p_rcmgr_protocols_allowed_total",
			Help: "allowed streams attached to a protocol",
		},
		[]string{protocol},
	)
	prometheus.MustRegister(rcmgrProtocolAllowed)

	rcmgrProtocolBlocked = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "libp2p_rcmgr_protocols_blocked_total",
			Help: "blocked streams attached to a protocol",
		},
		[]string{protocol},
	)
	prometheus.MustRegister(rcmgrProtocolBlocked)

	rcmgrProtocolPeerBlocked = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "libp2p_rcmgr_protocols_for_peer_blocked_total",
			Help: "blocked streams attached to a protocol for a specific peer",
		},
		[]string{protocol},
	)
	prometheus.MustRegister(rcmgrProtocolPeerBlocked)

	rcmgrServiceAllowed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "libp2p_rcmgr_services_allowed_total",
			Help: "allowed streams attached to a service",
		},
		[]string{service},
	)
	prometheus.MustRegister(rcmgrServiceAllowed)

	rcmgrServiceBlocked = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "libp2p_rcmgr_services_blocked_total",
			Help: "blocked streams attached to a service",
		},
		[]string{service},
	)
	prometheus.MustRegister(rcmgrServiceBlocked)

	rcmgrServicePeerBlocked = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "libp2p_rcmgr_service_for_peer_blocked_total",
			Help: "blocked streams attached to a service for a specific peer",
		},
		[]string{service},
	)
	prometheus.MustRegister(rcmgrServicePeerBlocked)

	rcmgrMemoryAllowed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "libp2p_rcmgr_memory_allocations_allowed_total",
		Help: "allowed memory allocations",
	})
	prometheus.MustRegister(rcmgrMemoryAllowed)

	rcmgrMemoryBlocked = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "libp2p_rcmgr_memory_allocations_blocked_total",
		Help: "blocked memory allocations",
	})
	prometheus.MustRegister(rcmgrMemoryBlocked)
}

type rcmgrMetrics struct{}

func getDirection(d network.Direction) string {
	switch d {
	default:
		return ""
	case network.DirInbound:
		return "inbound"
	case network.DirOutbound:
		return "outbound"
	}
}

func (r rcmgrMetrics) AllowConn(dir network.Direction, usefd bool) {
	rcmgrConnAllowed.WithLabelValues(getDirection(dir), strconv.FormatBool(usefd)).Add(1)
}

func (r rcmgrMetrics) BlockConn(dir network.Direction, usefd bool) {
	rcmgrConnBlocked.WithLabelValues(getDirection(dir), strconv.FormatBool(usefd)).Add(1)
}

func (r rcmgrMetrics) AllowStream(_ peer.ID, dir network.Direction) {
	rcmgrStreamAllowed.WithLabelValues(getDirection(dir)).Add(1)
}

func (r rcmgrMetrics) BlockStream(_ peer.ID, dir network.Direction) {
	rcmgrStreamBlocked.WithLabelValues(getDirection(dir)).Add(1)
}

func (r rcmgrMetrics) AllowPeer(_ peer.ID) {
	rcmgrPeerAllowed.Add(1)
}

func (r rcmgrMetrics) BlockPeer(_ peer.ID) {
	rcmgrPeerBlocked.Add(1)
}

func (r rcmgrMetrics) AllowProtocol(proto protocol.ID) {
	rcmgrProtocolAllowed.WithLabelValues(string(proto)).Add(1)
}

func (r rcmgrMetrics) BlockProtocol(proto protocol.ID) {
	rcmgrProtocolBlocked.WithLabelValues(string(proto)).Add(1)
}

func (r rcmgrMetrics) BlockProtocolPeer(proto protocol.ID, _ peer.ID) {
	rcmgrProtocolPeerBlocked.WithLabelValues(string(proto)).Add(1)
}

func (r rcmgrMetrics) AllowService(svc string) {
	rcmgrServiceAllowed.WithLabelValues(svc).Add(1)
}

func (r rcmgrMetrics) BlockService(svc string) {
	rcmgrServiceBlocked.WithLabelValues(svc).Add(1)
}

func (r rcmgrMetrics) BlockServicePeer(svc string, _ peer.ID) {
	rcmgrServicePeerBlocked.WithLabelValues(svc).Add(1)
}

func (r rcmgrMetrics) AllowMemory(_ int) {
	rcmgrMemoryAllowed.Add(1)
}

func (r rcmgrMetrics) BlockMemory(_ int) {
	rcmgrMemoryBlocked.Add(1)
}
