package libp2p

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/benbjohnson/clock"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	rcmgrObs "github.com/libp2p/go-libp2p/p2p/host/resource-manager/obs"
	"github.com/multiformats/go-multiaddr"
	"go.opencensus.io/stats/view"
	"go.uber.org/fx"

	"github.com/ipfs/kubo/config"
	"github.com/ipfs/kubo/core/node/helpers"
	"github.com/ipfs/kubo/repo"
)

const NetLimitTraceFilename = "rcmgr.json.gz"

var ErrNoResourceMgr = fmt.Errorf("missing ResourceMgr: make sure the daemon is running with Swarm.ResourceMgr.Enabled")

func ResourceManager(cfg config.SwarmConfig) interface{} {
	return func(mctx helpers.MetricsCtx, lc fx.Lifecycle, repo repo.Repo) (network.ResourceManager, Libp2pOpts, error) {
		var manager network.ResourceManager
		var opts Libp2pOpts

		enabled := cfg.ResourceMgr.Enabled.WithDefault(true)

		//  ENV overrides Config (if present)
		switch os.Getenv("LIBP2P_RCMGR") {
		case "0", "false":
			enabled = false
		case "1", "true":
			enabled = true
		}

		if enabled {
			log.Debug("libp2p resource manager is enabled")

			repoPath, err := config.PathRoot()
			if err != nil {
				return nil, opts, fmt.Errorf("opening IPFS_PATH: %w", err)
			}

			limitConfig, msg, err := LimitConfig(cfg)
			if err != nil {
				return nil, opts, fmt.Errorf("creating final Resource Manager config: %w", err)
			}

			// We want to see this message on startup, that's why we are using fmt instead of log.
			fmt.Print(msg)

			if err := ensureConnMgrMakeSenseVsResourceMgr(limitConfig, cfg.ConnMgr); err != nil {
				return nil, opts, err
			}

			limiter := rcmgr.NewFixedLimiter(limitConfig)

			str, err := rcmgrObs.NewStatsTraceReporter()
			if err != nil {
				return nil, opts, err
			}

			ropts := []rcmgr.Option{rcmgr.WithMetrics(createRcmgrMetrics()), rcmgr.WithTraceReporter(str)}

			if len(cfg.ResourceMgr.Allowlist) > 0 {
				var mas []multiaddr.Multiaddr
				for _, maStr := range cfg.ResourceMgr.Allowlist {
					ma, err := multiaddr.NewMultiaddr(maStr)
					if err != nil {
						log.Errorf("failed to parse multiaddr=%v for allowlist, skipping. err=%v", maStr, err)
						continue
					}
					mas = append(mas, ma)
				}
				ropts = append(ropts, rcmgr.WithAllowlistedMultiaddrs(mas))
				log.Infof("Setting allowlist to: %v", mas)
			}

			err = view.Register(rcmgrObs.DefaultViews...)
			if err != nil {
				return nil, opts, fmt.Errorf("registering rcmgr obs views: %w", err)
			}

			if os.Getenv("LIBP2P_DEBUG_RCMGR") != "" {
				traceFilePath := filepath.Join(repoPath, NetLimitTraceFilename)
				ropts = append(ropts, rcmgr.WithTrace(traceFilePath))
			}

			manager, err = rcmgr.NewResourceManager(limiter, ropts...)
			if err != nil {
				return nil, opts, fmt.Errorf("creating libp2p resource manager: %w", err)
			}
			lrm := &loggingResourceManager{
				clock:    clock.New(),
				logger:   &logging.Logger("resourcemanager").SugaredLogger,
				delegate: manager,
			}
			lrm.start(helpers.LifecycleCtx(mctx, lc))
			manager = lrm
		} else {
			fmt.Println("go-libp2p resource manager protection disabled")
			manager = &network.NullResourceManager{}
		}

		opts.Opts = append(opts.Opts, libp2p.ResourceManager(manager))

		lc.Append(fx.Hook{
			OnStop: func(_ context.Context) error {
				return manager.Close()
			}})

		return manager, opts, nil
	}
}

// LimitConfig returns the actual computed limits depending on the configuration.
func LimitConfig(cfg config.SwarmConfig) (rcmgr.LimitConfig, string, error) {
	var limitConfig rcmgr.LimitConfig
	defaultComputedLimitConfig, msg, err := createDefaultLimitConfig(cfg)
	if err != nil {
		return limitConfig, msg, err
	}

	// The logic for defaults and overriding with specified SwarmConfig.ResourceMgr.Limits
	// is documented in docs/config.md.
	// Any changes here should be reflected there.
	if cfg.ResourceMgr.Limits != nil {
		userSuppliedOverrideLimitConfig := *cfg.ResourceMgr.Limits
		// This effectively overrides the computed default LimitConfig with any non-zero values from cfg.ResourceMgr.Limits.
		// Because of how how Apply works, any 0 value for a user supplied override
		// will be overriden with a computed default value.
		// There currently isn't a way for a user to supply a 0-value override.
		userSuppliedOverrideLimitConfig.Apply(defaultComputedLimitConfig)
		limitConfig = userSuppliedOverrideLimitConfig
	} else {
		limitConfig = defaultComputedLimitConfig
	}

	return limitConfig, msg, nil
}

// StatToLimitConfig converts a stats object into a LimitConfig one. This is useful when you want to generate
// JSONs with the same structure for current stats and limits.
func StatToLimitConfig(stats rcmgr.ResourceManagerStat) rcmgr.LimitConfig {
	result := rcmgr.LimitConfig{}

	result.Peer = make(map[peer.ID]rcmgr.BaseLimit)
	for i, p := range stats.Peers {
		result.Peer[i] = scopeStatToBaseLimit(p)
	}

	stats.Protocols = make(map[protocol.ID]network.ScopeStat)
	for i, p := range stats.Protocols {
		result.Protocol[i] = scopeStatToBaseLimit(p)
	}

	stats.Services = make(map[string]network.ScopeStat)
	for i, s := range stats.Services {
		result.Service[i] = scopeStatToBaseLimit(s)
	}

	result.System = scopeStatToBaseLimit(stats.System)
	result.Transient = scopeStatToBaseLimit(stats.Transient)

	return result
}

type ResourceInfos []*ResourceInfo

type ResourceInfo struct {
	ScopeName    string
	LimitName    string
	Limit        int64
	CurrentUsage int64
}

// LimitConfigsToInfo gets limits and stats and generates a list of scopes and limits to be printed.
func LimitConfigsToInfo(limits, stats rcmgr.LimitConfig) ResourceInfos {
	result := ResourceInfos{}

	result = append(result, baseLimitToResourceInfo(config.ResourceMgrSystemScope, limits.System, stats.System)...)
	result = append(result, baseLimitToResourceInfo(config.ResourceMgrTransientScope, limits.Transient, stats.Transient)...)

	for i, p := range stats.Peer {
		// check if we have specific limits for this peer
		var bl rcmgr.BaseLimit
		lp, ok := limits.Peer[i]
		if !ok {
			bl = limits.PeerDefault
		} else {
			bl = lp
		}

		result = append(result, baseLimitToResourceInfo(
			config.ResourceMgrPeerScopePrefix+i.Pretty(),
			bl,
			p,
		)...)
	}

	for i, p := range stats.Protocol {
		// check if we have specific limits for this protocol
		var bl rcmgr.BaseLimit
		lp, ok := limits.Protocol[i]
		if !ok {
			bl = limits.ProtocolDefault
		} else {
			bl = lp
		}

		result = append(result, baseLimitToResourceInfo(
			config.ResourceMgrProtocolScopePrefix+string(i),
			bl,
			p,
		)...)
	}

	for i, s := range stats.Service {
		// check if we have specific limits for this service
		var bl rcmgr.BaseLimit
		lp, ok := limits.Service[i]
		if !ok {
			bl = limits.ServiceDefault
		} else {
			bl = lp
		}

		result = append(result, baseLimitToResourceInfo(
			config.ResourceMgrServiceScopePrefix+i,
			bl,
			s,
		)...)
	}

	return result
}

const (
	limitNameStreams         = "Streams"
	limitNameStreamsInbound  = "StreamsInbound"
	limitNameStreamsOutbound = "StreamsOutbound"
	limitNameConns           = "Conns"
	limitNameConnsInbound    = "ConnsInbound"
	limitNameConnsOutbound   = "ConnsOutbound"
	limitNameFD              = "FD"
	limitNameMemory          = "Memory"
)

var limits = []string{
	limitNameStreams,
	limitNameStreamsInbound,
	limitNameStreamsOutbound,
	limitNameConns,
	limitNameConnsInbound,
	limitNameConnsOutbound,
	limitNameFD,
	limitNameMemory,
}

func baseLimitToResourceInfo(scopeName string, limit, stat rcmgr.BaseLimit) ResourceInfos {
	result := ResourceInfos{}
	for _, l := range limits {
		ri := &ResourceInfo{
			ScopeName: scopeName,
		}
		switch l {
		case limitNameStreams:
			ri.LimitName = limitNameStreams
			ri.Limit = int64(limit.Streams)
			ri.CurrentUsage = int64(stat.Streams)
		case limitNameStreamsInbound:
			ri.LimitName = limitNameStreamsInbound
			ri.Limit = int64(limit.StreamsInbound)
			ri.CurrentUsage = int64(stat.StreamsInbound)
		case limitNameStreamsOutbound:
			ri.LimitName = limitNameStreamsOutbound
			ri.Limit = int64(limit.StreamsOutbound)
			ri.CurrentUsage = int64(stat.StreamsOutbound)
		case limitNameConns:
			ri.LimitName = limitNameConns
			ri.Limit = int64(limit.Conns)
			ri.CurrentUsage = int64(stat.Conns)
		case limitNameConnsInbound:
			ri.LimitName = limitNameConnsInbound
			ri.Limit = int64(limit.ConnsInbound)
			ri.CurrentUsage = int64(stat.ConnsInbound)
		case limitNameConnsOutbound:
			ri.LimitName = limitNameConnsOutbound
			ri.Limit = int64(limit.ConnsOutbound)
			ri.CurrentUsage = int64(stat.ConnsOutbound)
		case limitNameFD:
			ri.LimitName = limitNameFD
			ri.Limit = int64(limit.FD)
			ri.CurrentUsage = int64(stat.FD)
		case limitNameMemory:
			ri.LimitName = limitNameMemory
			ri.Limit = limit.Memory
			ri.CurrentUsage = stat.Memory
		}

		if ri.Limit == 0 {
			continue
		}

		result = append(result, ri)
	}

	return result
}

func scopeStatToBaseLimit(ss network.ScopeStat) rcmgr.BaseLimit {
	return rcmgr.BaseLimit{
		Streams:         ss.NumStreamsInbound + ss.NumStreamsOutbound,
		StreamsInbound:  ss.NumStreamsInbound,
		StreamsOutbound: ss.NumStreamsOutbound,
		Conns:           ss.NumConnsInbound + ss.NumConnsOutbound,
		ConnsInbound:    ss.NumConnsInbound,
		ConnsOutbound:   ss.NumConnsOutbound,
		FD:              ss.NumFD,
		Memory:          ss.Memory,
	}
}

func ensureConnMgrMakeSenseVsResourceMgr(rcm rcmgr.LimitConfig, cmgr config.ConnMgr) error {
	if cmgr.Type.WithDefault(config.DefaultConnMgrType) == "none" {
		return nil // none connmgr, no checks to do
	}
	highWater := cmgr.HighWater.WithDefault(config.DefaultConnMgrHighWater)
	if rcm.System.ConnsInbound <= rcm.System.Conns {
		if int64(rcm.System.ConnsInbound) <= highWater {
			// nolint
			return fmt.Errorf(`
Unable to initialize libp2p due to conflicting limit configuration:
ResourceMgr.Limits.System.ConnsInbound (%d) must be bigger than ConnMgr.HighWater (%d)
`, rcm.System.ConnsInbound, highWater)
		}
	} else if int64(rcm.System.Conns) <= highWater {
		// nolint
		return fmt.Errorf(`
Unable to initialize libp2p due to conflicting limit configuration:
ResourceMgr.Limits.System.Conns (%d) must be bigger than ConnMgr.HighWater (%d)
`, rcm.System.Conns, highWater)
	}
	if rcm.System.StreamsInbound <= rcm.System.Streams {
		if int64(rcm.System.StreamsInbound) <= highWater {
			// nolint
			return fmt.Errorf(`
Unable to initialize libp2p due to conflicting limit configuration:
ResourceMgr.Limits.System.StreamsInbound (%d) must be bigger than ConnMgr.HighWater (%d)
`, rcm.System.StreamsInbound, highWater)
		}
	} else if int64(rcm.System.Streams) <= highWater {
		// nolint
		return fmt.Errorf(`
Unable to initialize libp2p due to conflicting limit configuration:
ResourceMgr.Limits.System.Streams (%d) must be bigger than ConnMgr.HighWater (%d)
`, rcm.System.Streams, highWater)
	}
	return nil
}
