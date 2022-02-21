package libp2p

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/ipfs/go-ipfs/repo"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	rcmgr "github.com/libp2p/go-libp2p-resource-manager"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
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
	ServiceID, _  = tag.NewKey("svc")
	ProtocolID, _ = tag.NewKey("proto")
	Direction, _  = tag.NewKey("direction")
	UseFD, _      = tag.NewKey("use_fd")
	PeerID, _     = tag.NewKey("peer_id")
)

var (
	RcmgrAllowConn      = stats.Int64("rcmgr/allow_conn", "Number of allowed connections", stats.UnitDimensionless)
	RcmgrBlockConn      = stats.Int64("rcmgr/block_conn", "Number of blocked connections", stats.UnitDimensionless)
	RcmgrAllowStream    = stats.Int64("rcmgr/allow_stream", "Number of allowed streams", stats.UnitDimensionless)
	RcmgrBlockStream    = stats.Int64("rcmgr/block_stream", "Number of blocked streams", stats.UnitDimensionless)
	RcmgrAllowPeer      = stats.Int64("rcmgr/allow_peer", "Number of allowed peer connections", stats.UnitDimensionless)
	RcmgrBlockPeer      = stats.Int64("rcmgr/block_peer", "Number of blocked peer connections", stats.UnitDimensionless)
	RcmgrAllowProto     = stats.Int64("rcmgr/allow_proto", "Number of allowed streams attached to a protocol", stats.UnitDimensionless)
	RcmgrBlockProto     = stats.Int64("rcmgr/block_proto", "Number of blocked blocked streams attached to a protocol", stats.UnitDimensionless)
	RcmgrBlockProtoPeer = stats.Int64("rcmgr/block_proto", "Number of blocked blocked streams attached to a protocol for a specific peer", stats.UnitDimensionless)
	RcmgrAllowSvc       = stats.Int64("rcmgr/allow_svc", "Number of allowed streams attached to a service", stats.UnitDimensionless)
	RcmgrBlockSvc       = stats.Int64("rcmgr/block_svc", "Number of blocked blocked streams attached to a service", stats.UnitDimensionless)
	RcmgrBlockSvcPeer   = stats.Int64("rcmgr/block_svc", "Number of blocked blocked streams attached to a service for a specific peer", stats.UnitDimensionless)
	RcmgrAllowMem       = stats.Int64("rcmgr/allow_mem", "Number of allowed memory reservations", stats.UnitDimensionless)
	RcmgrBlockMem       = stats.Int64("rcmgr/block_mem", "Number of blocked memory reservations", stats.UnitDimensionless)
)

type rcmgrMetrics struct{}

func (r rcmgrMetrics) AllowConn(dir network.Direction, usefd bool) {
	ctx := context.Background()
	if dir == network.DirInbound {
		ctx, _ = tag.New(ctx, tag.Upsert(Direction, "inbound"))
	} else {
		ctx, _ = tag.New(ctx, tag.Upsert(Direction, "outbound"))
	}
	if usefd {
		ctx, _ = tag.New(ctx, tag.Upsert(UseFD, "true"))
	} else {
		ctx, _ = tag.New(ctx, tag.Upsert(UseFD, "false"))
	}
	stats.Record(ctx, RcmgrAllowConn.M(1))
}

func (r rcmgrMetrics) BlockConn(dir network.Direction, usefd bool) {
	ctx := context.Background()
	if dir == network.DirInbound {
		ctx, _ = tag.New(ctx, tag.Upsert(Direction, "inbound"))
	} else {
		ctx, _ = tag.New(ctx, tag.Upsert(Direction, "outbound"))
	}
	if usefd {
		ctx, _ = tag.New(ctx, tag.Upsert(UseFD, "true"))
	} else {
		ctx, _ = tag.New(ctx, tag.Upsert(UseFD, "false"))
	}
	stats.Record(ctx, RcmgrBlockConn.M(1))
}

func (r rcmgrMetrics) AllowStream(p peer.ID, dir network.Direction) {
	ctx := context.Background()
	if dir == network.DirInbound {
		ctx, _ = tag.New(ctx, tag.Upsert(Direction, "inbound"))
	} else {
		ctx, _ = tag.New(ctx, tag.Upsert(Direction, "outbound"))
	}
	ctx, _ = tag.New(ctx, tag.Upsert(PeerID, p.Pretty()))
	stats.Record(ctx, RcmgrAllowStream.M(1))
}

func (r rcmgrMetrics) BlockStream(p peer.ID, dir network.Direction) {
	ctx := context.Background()
	if dir == network.DirInbound {
		ctx, _ = tag.New(ctx, tag.Upsert(Direction, "inbound"))
	} else {
		ctx, _ = tag.New(ctx, tag.Upsert(Direction, "outbound"))
	}
	ctx, _ = tag.New(ctx, tag.Upsert(PeerID, p.Pretty()))
	stats.Record(ctx, RcmgrBlockStream.M(1))
}

func (r rcmgrMetrics) AllowPeer(p peer.ID) {
	ctx := context.Background()
	ctx, _ = tag.New(ctx, tag.Upsert(PeerID, p.Pretty()))
	stats.Record(ctx, RcmgrAllowPeer.M(1))
}

func (r rcmgrMetrics) BlockPeer(p peer.ID) {
	ctx := context.Background()
	ctx, _ = tag.New(ctx, tag.Upsert(PeerID, p.Pretty()))
	stats.Record(ctx, RcmgrBlockPeer.M(1))
}

func (r rcmgrMetrics) AllowProtocol(proto protocol.ID) {
	ctx := context.Background()
	ctx, _ = tag.New(ctx, tag.Upsert(ProtocolID, string(proto)))
	stats.Record(ctx, RcmgrAllowProto.M(1))
}

func (r rcmgrMetrics) BlockProtocol(proto protocol.ID) {
	ctx := context.Background()
	ctx, _ = tag.New(ctx, tag.Upsert(ProtocolID, string(proto)))
	stats.Record(ctx, RcmgrBlockProto.M(1))
}

func (r rcmgrMetrics) BlockProtocolPeer(proto protocol.ID, p peer.ID) {
	ctx := context.Background()
	ctx, _ = tag.New(ctx, tag.Upsert(ProtocolID, string(proto)))
	ctx, _ = tag.New(ctx, tag.Upsert(PeerID, p.Pretty()))
	stats.Record(ctx, RcmgrBlockProtoPeer.M(1))
}

func (r rcmgrMetrics) AllowService(svc string) {
	ctx := context.Background()
	ctx, _ = tag.New(ctx, tag.Upsert(ServiceID, svc))
	stats.Record(ctx, RcmgrAllowSvc.M(1))
}

func (r rcmgrMetrics) BlockService(svc string) {
	ctx := context.Background()
	ctx, _ = tag.New(ctx, tag.Upsert(ServiceID, svc))
	stats.Record(ctx, RcmgrBlockSvc.M(1))
}

func (r rcmgrMetrics) BlockServicePeer(svc string, p peer.ID) {
	ctx := context.Background()
	ctx, _ = tag.New(ctx, tag.Upsert(ServiceID, svc))
	ctx, _ = tag.New(ctx, tag.Upsert(PeerID, p.Pretty()))
	stats.Record(ctx, RcmgrBlockSvcPeer.M(1))
}

func (r rcmgrMetrics) AllowMemory(size int) {
	stats.Record(context.Background(), RcmgrAllowMem.M(1))
}

func (r rcmgrMetrics) BlockMemory(size int) {
	stats.Record(context.Background(), RcmgrBlockMem.M(1))
}
