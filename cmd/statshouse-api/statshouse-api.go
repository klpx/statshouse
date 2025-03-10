// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cloudflare/tableflip"
	"github.com/gorilla/handlers"
	"github.com/spf13/pflag"

	"github.com/vkcom/statshouse-go"
	"github.com/vkcom/statshouse/internal/api"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/util"
	"github.com/vkcom/statshouse/internal/vkgo/build"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
	"github.com/vkcom/statshouse/internal/vkgo/vkuth"
)

const (
	shutdownTimeout = 30 * time.Second
	exitTimeout     = 45 * time.Second
	upgradeTimeout  = 60 * time.Second

	httpReadHeaderTimeout = 10 * time.Second
	httpReadTimeout       = 30 * time.Second
	httpIdleTimeout       = 5 * time.Minute

	chDialTimeout = 5 * time.Second

	diskCacheTxDuration = 5 * time.Second
)

type args struct {
	api.HandlerOptions
	accessLog                bool
	rpcCryptoKeyPath         string
	brsMaxChunksCount        int
	chV1Addrs                []string
	chV1Debug                bool
	chV1MaxConns             int
	chV1Password             string
	chV1User                 string
	chV2Addrs                []string
	chV2Debug                bool
	chV2MaxLightFastConns    int
	chV2MaxHeavyFastConns    int
	chV2MaxHeavySlowConns    int
	chV2MaxLightSlowConns    int
	chV2MaxHardwareFastConns int
	chV2MaxHardwareSlowConns int

	chV2Password             string
	chV2User                 string
	defaultMetric            string
	defaultMetricFilterIn    []string
	defaultMetricFilterNotIn []string
	defaultMetricWhat        []string
	defaultMetricGroupBy     []string
	adminDash                int
	eventPreset              []string
	defaultNumSeries         int
	diskCache                string
	help                     bool
	listenHTTPAddr           string
	listenRPCAddr            string
	pidFile                  string
	pprofAddr                string
	pprofHTTP                bool
	showInvisible            bool
	slow                     time.Duration
	staticDir                string
	statsHouseNetwork        string
	statsHouseAddr           string
	statsHouseEnv            string
	utcOffsetHours           int // we can't support offsets not divisible by hour because we aggregate the data by hour
	version                  bool
	vkuthAppName             string
	vkuthPublicKeys          []string
	metadataActorID          int64
	metadataAddr             string
	metadataNet              string
}

func main() {
	log.SetPrefix("[statshouse-api] ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lmsgprefix)

	var argv args
	pflag.BoolVar(&argv.accessLog, "access-log", false, "write HTTP access log to stdout")
	pflag.StringVar(&argv.rpcCryptoKeyPath, "rpc-crypto-path", "", "path to RPC crypto key")
	pflag.IntVar(&argv.brsMaxChunksCount, "max-chunks-count", 1000, "in memory data chunks count limit for RPC server")
	var chMaxQueries int // not used any more, TODO - remove?
	pflag.IntVar(&chMaxQueries, "clickhouse-max-queries", 32, "maximum number of concurrent ClickHouse queries")
	pflag.StringSliceVar(&argv.chV1Addrs, "clickhouse-v1-addrs", nil, "comma-separated list of ClickHouse-v1 addresses")
	pflag.BoolVar(&argv.chV1Debug, "clickhouse-v1-debug", false, "ClickHouse-v1 debug mode")
	pflag.IntVar(&argv.chV1MaxConns, "clickhouse-v1-max-conns", 16, "maximum number of ClickHouse-v1 connections (fast and slow)")
	pflag.StringVar(&argv.chV1Password, "clickhouse-v1-password", "", "ClickHouse-v1 password")
	pflag.StringVar(&argv.chV1User, "clickhouse-v1-user", "", "ClickHouse-v1 user")
	pflag.StringSliceVar(&argv.chV2Addrs, "clickhouse-v2-addrs", nil, "comma-separated list of ClickHouse-v2 addresses")
	pflag.BoolVar(&argv.chV2Debug, "clickhouse-v2-debug", false, "ClickHouse-v2 debug mode")
	pflag.IntVar(&argv.chV2MaxLightFastConns, "clickhouse-v2-max-conns", 40, "maximum number of ClickHouse-v2 connections (light fast)")
	pflag.IntVar(&argv.chV2MaxLightSlowConns, "clickhouse-v2-max-light-slow-conns", 12, "maximum number of ClickHouse-v2 connections (light slow)")
	pflag.IntVar(&argv.chV2MaxHeavyFastConns, "clickhouse-v2-max-heavy-conns", 5, "maximum number of ClickHouse-v2 connections (heavy fast)")
	pflag.IntVar(&argv.chV2MaxHeavySlowConns, "clickhouse-v2-max-heavy-slow-conns", 1, "maximum number of ClickHouse-v2 connections (heavy slow)")
	pflag.IntVar(&argv.chV2MaxHardwareFastConns, "clickhouse-v2-max-hardware-fast-conns", 8, "maximum number of ClickHouse-v2 connections (hardware fast)")
	pflag.IntVar(&argv.chV2MaxHardwareSlowConns, "clickhouse-v2-max-hardware-slow-conns", 4, "maximum number of ClickHouse-v2 connections (hardware slow)")

	pflag.StringVar(&argv.chV2Password, "clickhouse-v2-password", "", "ClickHouse-v2 password")
	pflag.StringVar(&argv.chV2User, "clickhouse-v2-user", "", "ClickHouse-v2 user")
	pflag.StringVar(&argv.defaultMetric, "default-metric", format.BuiltinMetricNameAggBucketReceiveDelaySec, "default metric to show")
	pflag.StringSliceVar(&argv.defaultMetricFilterIn, "default-metric-filter-in", []string{}, "default metric filter in <key0>:value")
	pflag.StringSliceVar(&argv.defaultMetricFilterNotIn, "default-metric-filter-not-in", []string{}, "default metric filter not in <key0>:value")
	pflag.StringSliceVar(&argv.defaultMetricWhat, "default-metric-filter-what", []string{}, "default metric function")
	pflag.StringSliceVar(&argv.defaultMetricGroupBy, "default-metric-group-by", []string{"1"}, "default metric group by tags")
	pflag.IntVar(&argv.adminDash, "admin-dash-id", 0, "hardware metric dashboard")
	pflag.StringSliceVar(&argv.eventPreset, "event-preset", []string{}, "event preset")
	pflag.IntVar(&argv.defaultNumSeries, "default-num-series", 5, "default series number to request")
	pflag.StringVar(&argv.diskCache, "disk-cache", "statshouse_api_cache.db", "disk cache filename")
	pflag.BoolVarP(&argv.help, "help", "h", false, "print usage instructions and exit")
	pflag.StringVar(&argv.listenHTTPAddr, "listen-addr", "localhost:8080", "web server listen address")
	pflag.StringVar(&argv.listenRPCAddr, "listen-rpc-addr", "localhost:13347", "RPC server listen address")
	pflag.StringVar(&argv.pidFile, "pid-file", "statshouse_api.pid", "path to PID file") // fpr table flip

	pflag.StringVar(&argv.pprofAddr, "pprof-addr", "", "Go pprof HTTP listen address (deprecated)")
	pflag.BoolVar(&argv.pprofHTTP, "pprof-http", true, "Serve Go pprof HTTP on RPC port")
	pflag.BoolVar(&argv.showInvisible, "show-invisible", false, "show invisible metrics as well")
	pflag.DurationVar(&argv.slow, "slow", 0, "slow down all HTTP requests by this much")
	pflag.StringVar(&argv.staticDir, "static-dir", "", "directory with static assets")
	pflag.StringVar(&argv.statsHouseNetwork, "statshouse-network", statshouse.DefaultNetwork, "udp or unixgram")
	pflag.StringVar(&argv.statsHouseAddr, "statshouse-addr", statshouse.DefaultAddr, "address of udp socket or path to unix socket")
	pflag.StringVar(&argv.statsHouseEnv, "statshouse-env", "dev", "fill key0/environment with this value in StatHouse statistics")
	pflag.IntVar(&argv.utcOffsetHours, "utc-offset", 0, "UTC offset for aggregation, in hours")
	pflag.BoolVar(&argv.version, "version", false, "show version information and exit")
	pflag.StringVar(&argv.vkuthAppName, "vkuth-app-name", "statshouse-api", "vkuth application name (access bits namespace)")
	pflag.StringSliceVar(&argv.vkuthPublicKeys, "vkuth-public-keys", nil, "comma-separated list of trusted vkuth public keys; empty list disables token-based access control")

	pflag.Int64Var(&argv.metadataActorID, "metadata-actor-id", 0, "metadata engine actor id")
	pflag.StringVar(&argv.metadataAddr, "metadata-addr", "127.0.0.1:2442", "metadata engine address")
	pflag.StringVar(&argv.metadataNet, "metadata-net", "tcp4", "metadata engine network")
	argv.HandlerOptions.Bind(pflag.CommandLine)
	cfg := &api.Config{}
	cfg.Bind(pflag.CommandLine, api.DefaultConfig())
	pflag.Parse()

	if argv.help {
		pflag.Usage()
		return
	}
	if argv.version {
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", build.Info())
		return
	}

	if len(pflag.Args()) != 0 {
		log.Fatalf("unexpected command line arguments, check command line for typos: %q", pflag.Args())
	}
	if len(argv.chV2Addrs) == 0 {
		log.Fatal("--clickhouse-v2-addrs must be specified")
	}
	if math.Abs(float64(argv.utcOffsetHours)) > 168 { // hours in week (24*7=168)
		log.Fatal("invalid --utc-offset value")
	}

	if staticFS != nil && argv.staticDir != "" {
		log.Fatal("--static-dir must not be specified when static is embedded into the binary")
	}

	keys, err := vkuth.ParseVkuthKeys(argv.vkuthPublicKeys)
	if err != nil {
		log.Fatal(err)
	}

	err = run(argv, cfg, keys)
	if err != nil {
		log.Fatal(err)
	}
}

func run(argv args, cfg *api.Config, vkuthPublicKeys map[string][]byte) error {
	if err := argv.HandlerOptions.LoadLocation(); err != nil {
		return err
	}
	tf, err := tableflip.New(tableflip.Options{
		PIDFile:        argv.pidFile,
		UpgradeTimeout: upgradeTimeout,
	})
	if err != nil {
		return fmt.Errorf("failed to init tableflip: %w", err)
	}
	defer tf.Stop()

	go func() {
		ch := make(chan os.Signal, 3)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
		for sig := range ch {
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				log.Printf("got %v, exiting...", sig)
				tf.Stop()
			case syscall.SIGHUP:
				log.Printf("got %v, upgrading...", sig)
				err := tf.Upgrade()
				if err != nil {
					log.Printf("upgrade failed: %v", err)
				}
			}
		}
	}()

	httpLn, err := tf.Listen("tcp", argv.listenHTTPAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %q: %w", argv.listenHTTPAddr, err)
	}

	var chV1 *util.ClickHouse
	if len(argv.chV1Addrs) > 0 {
		// argv.chV1MaxConns, argv.chV2MaxHeavyConns, argv.chV1Addrs, argv.chV1User, argv.chV1Password, argv.chV1Debug, chDialTimeout
		chV1, err = util.OpenClickHouse(util.ChConnOptions{
			Addrs:             argv.chV1Addrs,
			User:              argv.chV1User,
			Password:          argv.chV1Password,
			DialTimeout:       chDialTimeout,
			FastLightMaxConns: argv.chV1MaxConns,
			FastHeavyMaxConns: argv.chV1MaxConns,
			SlowLightMaxConns: argv.chV1MaxConns,
			SlowHeavyMaxConns: argv.chV1MaxConns,
		})
		if err != nil {
			return fmt.Errorf("failed to open ClickHouse-v1: %w", err)
		}
		defer func() { chV1.Close() }()
	}
	// argv.chV2MaxLightFastConns, argv.chV2MaxHeavyConns, , , argv.chV2Password, argv.chV2Debug, chDialTimeout
	chV2, err := util.OpenClickHouse(util.ChConnOptions{
		Addrs:                argv.chV2Addrs,
		User:                 argv.chV2User,
		Password:             argv.chV2Password,
		DialTimeout:          chDialTimeout,
		FastLightMaxConns:    argv.chV2MaxLightFastConns,
		FastHeavyMaxConns:    argv.chV2MaxHeavyFastConns,
		SlowLightMaxConns:    argv.chV2MaxLightSlowConns,
		SlowHeavyMaxConns:    argv.chV2MaxHeavySlowConns,
		FastHardwareMaxConns: argv.chV2MaxHardwareFastConns,
		SlowHardwareMaxConns: argv.chV2MaxHardwareSlowConns,
	})
	if err != nil {
		return fmt.Errorf("failed to open ClickHouse-v2: %w", err)
	}
	defer func() { chV2.Close() }()
	c := rpc.NewClient(rpc.ClientWithLogf(log.Printf), rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups()))
	defer func() { _ = c.Close() }()

	dc, err := pcache.OpenDiskCache(argv.diskCache, diskCacheTxDuration)
	if err != nil {
		return fmt.Errorf("failed to open disk cache: %w", err)
	}
	defer func() {
		err := dc.Close()
		if err != nil {
			log.Printf("failed to close disk cache: %v", err)
		}
	}()

	statshouse.ConfigureNetwork(log.Printf, argv.statsHouseNetwork, argv.statsHouseAddr, argv.statsHouseEnv)
	defer func() { _ = statshouse.Close() }()
	var rpcCryptoKeys []string
	if argv.rpcCryptoKeyPath != "" {
		cryptoKey, err := os.ReadFile(argv.rpcCryptoKeyPath)
		if err != nil {
			return fmt.Errorf("could not read RPC crypto key file %q: %v", argv.rpcCryptoKeyPath, err)
		}
		rpcCryptoKeys = append(rpcCryptoKeys, string(cryptoKey))
	}
	rpcCryptoKey := ""
	if len(rpcCryptoKeys) > 0 {
		rpcCryptoKey = rpcCryptoKeys[0]
	}

	if staticFS == nil {
		staticFS = os.DirFS(argv.staticDir)
	}

	jwtHelper := vkuth.NewJWTHelper(vkuthPublicKeys, argv.vkuthAppName)
	defaultMetricFilterIn := map[string][]string{}
	defaultMetricFilterNotIn := map[string][]string{}
	for _, s := range argv.defaultMetricFilterIn {
		kv := strings.Split(s, ":")
		if len(kv) != 2 {
			log.Printf("[error] default-metric-filter-in invalid format: %s", s)
		}
		f := defaultMetricFilterIn[kv[0]]
		defaultMetricFilterIn[kv[0]] = append(f, kv[1])
	}
	for _, s := range argv.defaultMetricFilterNotIn {
		kv := strings.Split(s, ":")
		if len(kv) != 2 {
			log.Printf("[error] default-metric-filter-not-in invalid format: %s", s)
		}
		f := defaultMetricFilterNotIn[kv[0]]
		defaultMetricFilterNotIn[kv[0]] = append(f, kv[1])
	}
	jsSettings := api.JSSettings{
		VkuthAppName:             argv.vkuthAppName,
		DefaultMetric:            argv.defaultMetric,
		DefaultMetricGroupBy:     argv.defaultMetricGroupBy,
		DefaultMetricWhat:        argv.defaultMetricWhat,
		DefaultMetricFilterIn:    defaultMetricFilterIn,
		DefaultMetricFilterNotIn: defaultMetricFilterNotIn,
		EventPreset:              argv.eventPreset,
		DefaultNumSeries:         argv.defaultNumSeries,
		DisableV1:                len(argv.chV1Addrs) == 0,
		AdminDash:                argv.adminDash,
	}
	if argv.LocalMode {
		jsSettings.VkuthAppName = ""
	}
	f, err := api.NewHandler(
		staticFS,
		jsSettings,
		argv.showInvisible,
		chV1,
		chV2,
		&tlmetadata.Client{
			Client:  rpc.NewClient(rpc.ClientWithLogf(log.Printf), rpc.ClientWithCryptoKey(rpcCryptoKey), rpc.ClientWithTrustedSubnetGroups(build.TrustedSubnetGroups())),
			Network: argv.metadataNet,
			Address: argv.metadataAddr,
			ActorID: argv.metadataActorID,
		},
		dc,
		jwtHelper,
		argv.HandlerOptions,
		cfg,
	)
	if err != nil {
		return fmt.Errorf("failed to create handler: %w", err)
	}
	defer func() { _ = f.Close() }()

	m := api.NewHTTPRouter(f)
	a := m.PathPrefix(api.RoutePrefix).Subrouter()
	a.Router.Path("/"+api.EndpointLegacyRedirect).Methods("GET", "HEAD", "POST").HandlerFunc(f.HandleLegacyRedirect)
	a.Path("/" + api.EndpointMetricList).Methods("GET").HandlerFunc(api.HandleGetMetricsList)
	a.Path("/" + api.EndpointMetricTagValues).Methods("GET").HandlerFunc(api.HandleGetMetricTagValues)
	a.Path("/" + api.EndpointMetric).Methods("GET").HandlerFunc(api.HandleGetMetric)
	a.Path("/" + api.EndpointMetric).Methods("POST").HandlerFunc(api.HandlePostMetric)
	a.Path("/" + api.EndpointResetFlood).Methods("POST").HandlerFunc(api.HandlePostResetFlood)
	a.Path("/" + api.EndpointQuery).Methods("GET").HandlerFunc(api.HandleSeriesQuery)
	a.Path("/" + api.EndpointPoint).Methods("GET").HandlerFunc(api.HandlePointQuery)
	a.Path("/" + api.EndpointPoint).Methods("POST").HandlerFunc(api.HandlePointQuery)
	a.Path("/" + api.EndpointTable).Methods("GET").HandlerFunc(api.HandleGetTable)
	a.Path("/" + api.EndpointQuery).Methods("POST").HandlerFunc(api.HandleSeriesQuery)
	a.Path("/" + api.EndpointRender).Methods("GET").HandlerFunc(api.HandleGetRender)
	a.Path("/" + api.EndpointDashboard).Methods("GET").HandlerFunc(api.HandleGetDashboard)
	a.Path("/" + api.EndpointDashboardList).Methods("GET").HandlerFunc(api.HandleGetDashboardList)
	a.Path("/"+api.EndpointDashboard).Methods("POST", "PUT").HandlerFunc(api.HandlePutPostDashboard)
	a.Path("/" + api.EndpointGroup).Methods("GET").HandlerFunc(api.HandleGetGroup)
	a.Path("/" + api.EndpointGroupList).Methods("GET").HandlerFunc(api.HandleGetGroupsList)
	a.Path("/"+api.EndpointGroup).Methods("POST", "PUT").HandlerFunc(api.HandlePutPostGroup)
	a.Path("/"+api.EndpointNamespace).Methods("POST", "PUT").HandlerFunc(api.HandlePostNamespace)
	a.Path("/" + api.EndpointNamespace).Methods("GET").HandlerFunc(api.HandleGetNamespace)
	a.Path("/" + api.EndpointNamespaceList).Methods("GET").HandlerFunc(api.HandleGetNamespaceList)
	a.Path("/" + api.EndpointPrometheus).Methods("GET").HandlerFunc(api.HandleGetPromConfig)
	a.Path("/" + api.EndpointPrometheus).Methods("POST").HandlerFunc(api.HandlePostPromConfig)
	a.Path("/" + api.EndpointPrometheusGenerated).Methods("GET").HandlerFunc(api.HandleGetPromConfigGenerated)
	a.Path("/" + api.EndpointKnownTags).Methods("POST").HandlerFunc(api.HandlePostKnownTags)
	a.Path("/" + api.EndpointKnownTags).Methods("GET").HandlerFunc(api.HandleGetKnownTags)
	a.Path("/" + api.EndpointStatistics).Methods("POST").HandlerFunc(api.HandleFrontendStat)
	a.Path("/" + api.EndpointHistory).Methods("GET").HandlerFunc(api.HandleGetHistory)
	m.Path("/prom/api/v1/query").Methods("GET").HandlerFunc(api.HandleInstantQuery)
	m.Path("/prom/api/v1/query").Methods("POST").HandlerFunc(api.HandleInstantQuery)
	m.Path("/prom/api/v1/query_range").Methods("GET").HandlerFunc(api.HandleRangeQuery)
	m.Path("/prom/api/v1/query_range").Methods("POST").HandlerFunc(api.HandleRangeQuery)
	m.Path("/prom/api/v1/label/{name}/values").Methods("GET").HandlerFunc(api.HandlePromLabelValuesQuery)
	m.Path("/prom/api/v1/series").Methods("GET").HandlerFunc(api.HandlePromSeriesQuery)
	m.Path("/prom/api/v1/series").Methods("POST").HandlerFunc(api.HandlePromSeriesQuery)
	m.Path("/debug/pprof/").Methods("GET").HandlerFunc(api.HandleProf)
	m.Path("/debug/pprof/allocs").Methods("GET").HandlerFunc(api.HandleProf)
	m.Path("/debug/pprof/block").Methods("GET").HandlerFunc(api.HandleProf)
	m.Path("/debug/pprof/cmdline").Methods("GET").HandlerFunc(api.HandleProfCmdline)
	m.Path("/debug/pprof/goroutine").Methods("GET").HandlerFunc(api.HandleProf)
	m.Path("/debug/pprof/heap").Methods("GET").HandlerFunc(api.HandleProf)
	m.Path("/debug/pprof/mutex").Methods("GET").HandlerFunc(api.HandleProf)
	m.Path("/debug/pprof/profile").Methods("GET").HandlerFunc(api.HandleProfProfile)
	m.Path("/debug/pprof/threadcreate").Methods("GET").HandlerFunc(api.HandleProf)
	m.Path("/debug/pprof/trace").Methods("GET").HandlerFunc(api.HandleProfTrace)
	m.Path("/debug/pprof/symbol").Methods("GET").HandlerFunc(api.HandleProfSymbol)
	m.Path("/debug/500").Methods("GET").HandlerFunc(api.DumpInternalServerErrors)
	m.Router.PathPrefix("/").Methods("GET", "HEAD").HandlerFunc(f.HandleStatic)

	h := http.Handler(m)
	h = handlers.RecoveryHandler(handlers.PrintRecoveryStack(true))(h)
	h = handlers.CompressHandler(h)
	if argv.accessLog {
		h = handlers.CombinedLoggingHandler(os.Stdout, h)
	}
	h = handlers.ProxyHeaders(h)
	if argv.slow > 0 {
		prev := h
		h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(argv.slow)
			prev.ServeHTTP(w, r)
		})
	}

	s := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	go func() {
		err := s.Serve(httpLn)
		if err != http.ErrServerClosed {
			log.Printf("serving HTTP: %v", err)
		}
	}()

	brs := api.NewBigResponseStorage(argv.brsMaxChunksCount, time.Second)
	defer brs.Close()

	chunksCountMeasurementID := statshouse.StartRegularMeasurement(api.CurrentChunksCount(brs))
	defer statshouse.StopRegularMeasurement(chunksCountMeasurementID)

	startTimestamp := time.Now().Unix()
	heartbeatTags := statshouse.Tags{
		1: "4",
		2: fmt.Sprint(format.TagValueIDHeartbeatEventStart),
		5: fmt.Sprint(format.ISO8601Date2BuildDateKey(time.Unix(int64(build.CommitTimestamp()), 0).Format(time.RFC3339))),
		6: fmt.Sprint(build.CommitTimestamp()),
		7: srvfunc.HostnameForStatshouse(),
	}
	if build.Commit() != "?" {
		commitRaw, err := hex.DecodeString(build.Commit())
		if err == nil && len(commitRaw) >= 4 {
			heartbeatTags[4] = fmt.Sprint(int32(binary.BigEndian.Uint32(commitRaw)))
		}
	}
	statshouse.Value(format.BuiltinMetricNameHeartbeatVersion, heartbeatTags, 0)

	heartbeatTags[2] = fmt.Sprint(format.TagValueIDHeartbeatEventHeartbeat)
	defer statshouse.StopRegularMeasurement(statshouse.StartRegularMeasurement(func(c *statshouse.Client) {
		uptime := float64(time.Now().Unix() - startTimestamp)
		c.Value(format.BuiltinMetricNameHeartbeatVersion, heartbeatTags, uptime)
	}))

	handlerRPC := api.NewRPCRouter(f, brs)
	var hijackListener *rpc.HijackListener
	metrics := util.NewRPCServerMetrics("statshouse_api")
	srv := rpc.NewServer(
		rpc.ServerWithSocketHijackHandler(func(conn *rpc.HijackConnection) {
			hijackListener.AddConnection(conn)
		}),
		rpc.ServerWithLogf(log.Printf),
		rpc.ServerWithTrustedSubnetGroups(build.TrustedSubnetGroups()),
		rpc.ServerWithHandler(handlerRPC.Handle),
		rpc.ServerWithCryptoKeys(rpcCryptoKeys),
		metrics.ServerWithMetrics,
	)
	defer metrics.Run(srv)()
	defer func() { _ = srv.Close() }()

	rpcLn, err := tf.Listen("tcp4", argv.listenRPCAddr)
	if err != nil {
		return fmt.Errorf("could not listen RPC: %w", err)
	}

	hijackListener = rpc.NewHijackListener(rpcLn.Addr())
	defer func() { _ = hijackListener.Close() }()
	go func() {
		err := srv.Serve(rpcLn)
		if err != nil {
			log.Fatalln("RPC server failed:", err)
		}
	}()
	if argv.pprofHTTP {
		go func() { // serve pprof on RPC port
			m := http.NewServeMux()
			m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				http.DefaultServeMux.ServeHTTP(w, r)
			})
			log.Printf("serving Go pprof at %q", argv.listenRPCAddr)
			s := http.Server{Handler: m}
			_ = s.Serve(hijackListener)
		}()
	} else {
		_ = hijackListener.Close() // will close all incoming connections
	}
	err = tf.Ready()
	if err != nil {
		return fmt.Errorf("failed to become ready: %w", err)
	}

	log.Printf("version %v listening HTTP at %q listening RPC at %q", build.Version(), httpLn.Addr().String(), rpcLn.Addr().String())
	<-tf.Exit()

	time.AfterFunc(exitTimeout, func() {
		log.Printf("graceful shutdown timeout; exiting")
		os.Exit(1)
	})

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	err = s.Shutdown(ctx)
	if err != nil {
		return fmt.Errorf("server shutdown failed: %w", err)
	}

	return nil
}
