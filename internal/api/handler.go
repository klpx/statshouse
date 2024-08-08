// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	ttemplate "text/template"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/sync/semaphore"

	"github.com/vkcom/statshouse-go"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	_ "github.com/mailru/easyjson/gen" // https://github.com/mailru/easyjson/issues/293

	"github.com/vkcom/statshouse/internal/config"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/pcache"
	"github.com/vkcom/statshouse/internal/promql"
	"github.com/vkcom/statshouse/internal/util"
	"github.com/vkcom/statshouse/internal/vkgo/srvfunc"
	"github.com/vkcom/statshouse/internal/vkgo/vkuth"

	"pgregory.net/rand"
)

//go:generate easyjson -no_std_marshalers httputil.go handler.go
// after generating, you should manually change
//	out.Float64(float64(v17))
//	...
//	out.Float64(float64(v36))
// to
//	if math.IsNaN(float64(v17)) {
//		out.RawString("null")
//	} else {
//		out.Float64(float64(v17))
//	}
//	...
//	if math.IsNaN(float64(v36)) {
//		out.RawString("null")
//	} else {
//		out.Float64(float64(v36))
//	}

// also remove code which saves and loads UpdateTime

const (
	ParamVersion       = "v"
	ParamNumResults    = "n"
	ParamMetric        = "s"
	ParamID            = "id"
	ParamEntityVersion = "ver"
	ParamNamespace     = "namespace"

	ParamTagID        = "k"
	ParamFromTime     = "f"
	ParamToTime       = "t"
	ParamWidth        = "w"
	ParamWidthAgg     = "g" // supported only for better compatibility between UI and API URLs
	ParamTimeShift    = "ts"
	ParamQueryWhat    = "qw"
	ParamQueryBy      = "qb"
	ParamQueryFilter  = "qf"
	ParamQueryVerbose = "qv"
	ParamAvoidCache   = "ac"
	paramRenderWidth  = "rw"
	paramDataFormat   = "df"
	paramTabNumber    = "tn"
	paramMaxHost      = "mh"
	paramFromRow      = "fr"
	paramToRow        = "tr"
	paramPromQuery    = "q"
	paramFromEnd      = "fe"
	paramExcessPoints = "ep"
	paramLegacyEngine = "legacy"
	paramQueryType    = "qt"
	paramDashboardID  = "id"
	paramShowDisabled = "sd"
	paramPriority     = "priority"
	paramYL, paramYH  = "yl", "yh" // Y scale range

	Version1       = "1"
	Version2       = "2"
	dataFormatPNG  = "png"
	dataFormatSVG  = "svg"
	dataFormatText = "text"
	dataFormatCSV  = "csv"

	defSeries     = 10
	maxSeries     = 10_000
	defTagValues  = 100
	maxTagValues  = 100_000
	maxSeriesRows = 10_000_000
	maxTableRows  = 100_000

	maxTableRowsPage = 10_000
	maxTimeShifts    = 10
	maxFunctions     = 10

	cacheInvalidateCheckInterval = 1 * time.Second
	cacheInvalidateCheckTimeout  = 5 * time.Second
	cacheInvalidateMaxRows       = 100_000
	cacheDefaultDropEvery        = 90 * time.Second

	queryClientCache               = 1 * time.Second
	queryClientCacheStale          = 9 * time.Second // ~ v2 lag
	queryClientCacheImmutable      = 7 * 24 * time.Hour
	queryClientCacheStaleImmutable = 0

	QuerySelectTimeoutDefault = 55 * time.Second // TODO: querySelectTimeout must be longer than the longest normal query. And must be consistent with NGINX's or another reverse proxy's timeout
	fastQueryTimeInterval     = (86400 + 3600) * 2

	maxEntityHTTPBodySize     = 256 << 10
	maxPromConfigHTTPBodySize = 500 * 1024

	defaultCacheTTL = 1 * time.Second

	maxConcurrentPlots = 8
	plotRenderTimeout  = 5 * time.Second

	descriptionFieldName = "__description"
	journalUpdateTimeout = 2 * time.Second
)

type (
	JSSettings struct {
		VkuthAppName             string              `json:"vkuth_app_name"`
		DefaultMetric            string              `json:"default_metric"`
		DefaultMetricFilterIn    map[string][]string `json:"default_metric_filter_in"`
		DefaultMetricFilterNotIn map[string][]string `json:"default_metric_filter_not_in"`
		DefaultMetricWhat        []string            `json:"default_metric_what"`
		DefaultMetricGroupBy     []string            `json:"default_metric_group_by"`
		EventPreset              []string            `json:"event_preset"`
		DefaultNumSeries         int                 `json:"default_num_series"`
		DisableV1                bool                `json:"disabled_v1"`
		AdminDash                int                 `json:"admin_dash"`
	}

	Handler struct {
		HandlerOptions
		showInvisible         bool
		staticDir             http.FileSystem
		indexTemplate         *template.Template
		indexSettings         string
		ch                    map[string]*util.ClickHouse
		metricsStorage        *metajournal.MetricsStorage
		tagValueCache         *pcache.Cache
		tagValueIDCache       *pcache.Cache
		cache                 *tsCacheGroup
		pointsCache           *pointsCache
		pointFloatsPool       sync.Pool
		pointFloatsPoolSize   atomic.Int64
		cacheInvalidateTicker *time.Ticker
		cacheInvalidateStop   chan chan struct{}
		metadataLoader        *metajournal.MetricMetaLoader
		jwtHelper             *vkuth.JWTHelper
		plotRenderSem         *semaphore.Weighted
		plotTemplate          *ttemplate.Template
		rUsage                syscall.Rusage // accessed without lock by first shard addBuiltIns
		rmID                  int
		rmIDCache             int
		promEngine            promql.Engine
		bufferBytesAlloc      *statshouse.MetricRef
		bufferBytesFree       *statshouse.MetricRef
		bufferPoolBytesAlloc  *statshouse.MetricRef
		bufferPoolBytesFree   *statshouse.MetricRef
		bufferPoolBytesTotal  *statshouse.MetricRef
	}

	//easyjson:json
	GetMetricsListResp struct {
		Metrics []metricShortInfo `json:"metrics"`
	}

	//easyjson:json
	GetDashboardListResp struct {
		Dashboards []dashboardShortInfo `json:"dashboards"`
	}

	//easyjson:json
	GetGroupListResp struct {
		Groups []groupShortInfo `json:"groups"`
	}

	//easyjson:json
	GetNamespaceListResp struct {
		Namespaces []namespaceShortInfo `json:"namespaces"`
	}

	metricShortInfo struct {
		Name string `json:"name"`
	}

	dashboardShortInfo struct {
		Id          int32  `json:"id"`
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	groupShortInfo struct {
		Id      int32   `json:"id"`
		Name    string  `json:"name"`
		Weight  float64 `json:"weight"`
		Disable bool    `json:"disable"`
	}

	namespaceShortInfo struct {
		Id     int32   `json:"id"`
		Name   string  `json:"name"`
		Weight float64 `json:"weight"`
	}

	//easyjson:json
	MetricInfo struct {
		Metric format.MetricMetaValue `json:"metric"`
	}

	//easyjson:json
	DashboardInfo struct {
		Dashboard DashboardMetaInfo `json:"dashboard"`
		Delete    bool              `json:"delete_mark"`
	}

	//easyjson:json
	MetricsGroupInfo struct {
		Group   format.MetricsGroup `json:"group"`
		Metrics []string            `json:"metrics"`
	}

	//easyjson:json
	NamespaceInfo struct {
		Namespace format.NamespaceMeta `json:"namespace"`
	}

	DashboardMetaInfo struct {
		DashboardID int32                  `json:"dashboard_id"`
		Name        string                 `json:"name"`
		Version     int64                  `json:"version,omitempty"`
		UpdateTime  uint32                 `json:"update_time"`
		DeletedTime uint32                 `json:"deleted_time"`
		Description string                 `json:"description"`
		JSONData    map[string]interface{} `json:"data"`
	}

	//easyjson:json
	DashboardData struct {
		Plots      []DashboardPlot     `json:"plots"`
		Vars       []DashboardVar      `json:"variables"`
		TabNum     int                 `json:"tabNum"`
		TimeRange  DashboardTimeRange  `json:"timeRange"`
		TimeShifts DashboardTimeShifts `json:"timeShifts"`
	}

	DashboardPlot struct {
		UseV2       bool                `json:"useV2"`
		NumSeries   int                 `json:"numSeries"`
		MetricName  string              `json:"metricName"`
		CustomName  string              `json:"customName"`
		Width       int                 `json:"customAgg"`
		PromQL      string              `json:"promQL"`
		What        []string            `json:"what"`
		GroupBy     []string            `json:"groupBy"`
		FilterIn    map[string][]string `json:"filterIn"`
		FilterNotIn map[string][]string `json:"filterNotIn"`
		MaxHost     bool                `json:"maxHost"`
		Type        int                 `json:"type"`
	}

	DashboardVar struct {
		Name string           `json:"name"`
		Args DashboardVarArgs `json:"args"`
		Vals []string         `json:"values"`
		Link [][]int          `json:"link"`
	}

	DashboardVarArgs struct {
		Group  bool `json:"groupBy"`
		Negate bool `json:"negative"`
	}

	DashboardTimeRange struct {
		From int64
		To   string
	}

	DashboardTimeShifts []string

	getMetricTagValuesReq struct {
		ai                  accessInfo
		version             string
		numResults          string
		metricWithNamespace string
		tagID               string
		from                string
		to                  string
		what                string
		filter              []string
	}

	//easyjson:json
	GetMetricTagValuesResp struct {
		TagValues     []MetricTagValueInfo `json:"tag_values"`
		TagValuesMore bool                 `json:"tag_values_more"`
	}

	MetricTagValueInfo struct {
		Value string  `json:"value"`
		Count float64 `json:"count"`
	}

	seriesRequest struct {
		ai                  accessInfo
		version             string
		numResults          int
		metricWithNamespace string
		customMetricName    string
		from                time.Time
		to                  time.Time
		step                int64
		screenWidth         int64
		promQL              string
		shifts              []time.Duration
		what                []QueryFunc
		by                  []string
		filterIn            map[string][]string
		filterNotIn         map[string][]string
		vars                map[string]promql.Variable
		maxHost             bool
		avoidCache          bool
		verbose             bool
		excessPoints        bool
		format              string
		yl, yh              string // Y scale range

		// table query
		fromEnd bool
		fromRow RowMarker
		toRow   RowMarker
	}

	seriesRequestOptions struct {
		metricCallback func(*format.MetricMetaValue)
		rand           *rand.Rand
		timeNow        time.Time
		mode           data_model.QueryMode
		trace          bool
		strBucketLabel bool
	}

	//easyjson:json
	SeriesResponse struct {
		Series            querySeries             `json:"series"`
		SamplingFactorSrc float64                 `json:"sampling_factor_src"` // average
		SamplingFactorAgg float64                 `json:"sampling_factor_agg"` // average
		ReceiveErrors     float64                 `json:"receive_errors"`      // count/sec
		ReceiveWarnings   float64                 `json:"receive_warnings"`    // count/sec
		MappingErrors     float64                 `json:"mapping_errors"`      // count/sec
		PromQL            string                  `json:"promql"`              // equivalent PromQL query
		DebugQueries      []string                `json:"__debug_queries"`     // private, unstable: SQL queries executed
		ExcessPointLeft   bool                    `json:"excess_point_left"`
		ExcessPointRight  bool                    `json:"excess_point_right"`
		MetricMeta        *format.MetricMetaValue `json:"metric"`
		immutable         bool
	}

	//easyjson:json
	GetPointResp struct {
		PointMeta    []QueryPointsMeta `json:"point_meta"`      // M
		PointData    []float64         `json:"point_data"`      // M
		DebugQueries []string          `json:"__debug_queries"` // private, unstable: SQL queries executed
	}

	//easyjson:json
	GetTableResp struct {
		Rows         []queryTableRow `json:"rows"`
		What         []QueryFunc     `json:"what"`
		FromRow      string          `json:"from_row"`
		ToRow        string          `json:"to_row"`
		More         bool            `json:"more"`
		DebugQueries []string        `json:"__debug_queries"` // private, unstable: SQL queries executed, can be null
	}

	renderRequest struct {
		ai            accessInfo
		seriesRequest []seriesRequest
		renderWidth   string
		renderFormat  string
	}

	renderResponse struct {
		format string
		data   []byte
	}

	querySeries struct {
		Time       []int64             `json:"time"`        // N
		SeriesMeta []QuerySeriesMetaV2 `json:"series_meta"` // M
		SeriesData []*[]float64        `json:"series_data"` // MxN
	}

	//easyjson:json
	queryTableRow struct {
		Time    int64                    `json:"time"`
		Data    []float64                `json:"data"`
		Tags    map[string]SeriesMetaTag `json:"tags"`
		row     tsSelectRow
		rowRepr RowMarker
	}

	queryTableRows []queryTableRow

	QuerySeriesMeta struct {
		TimeShift int64             `json:"time_shift"`
		Tags      map[string]string `json:"tags"`
		MaxHosts  []string          `json:"max_hosts"` // max_host for now
		What      QueryFunc         `json:"what"`
	}

	QuerySeriesMetaV2 struct {
		TimeShift  int64                    `json:"time_shift"`
		Tags       map[string]SeriesMetaTag `json:"tags"`
		MaxHosts   []string                 `json:"max_hosts"` // max_host for now
		Name       string                   `json:"name"`
		Color      string                   `json:"color"`
		What       string                   `json:"what"`
		Total      int                      `json:"total"`
		MetricType string                   `json:"metric_type"`
	}

	QueryPointsMeta struct {
		TimeShift int64                    `json:"time_shift"`
		Tags      map[string]SeriesMetaTag `json:"tags"`
		MaxHost   string                   `json:"max_host"` // max_host for now
		Name      string                   `json:"name"`
		What      string                   `json:"what"`
		FromSec   int64                    `json:"from_sec"` // rounded from sec
		ToSec     int64                    `json:"to_sec"`   // rounded to sec
	}

	SeriesMetaTag struct {
		Value   string `json:"value"`
		Comment string `json:"comment,omitempty"`
		Raw     bool   `json:"raw,omitempty"`
		RawKind string `json:"raw_kind,omitempty"`
	}

	RawTag struct {
		Index int   `json:"index"`
		Value int32 `json:"value"`
	}

	RowMarker struct {
		Time int64    `json:"time"`
		Tags []RawTag `json:"tags"`
		SKey string   `json:"skey"`
	}

	cacheInvalidateLogRow struct {
		T  int64 `ch:"time"` // time of insert
		At int64 `ch:"key1"` // seconds inserted (changed), which should be invalidated
	}

	metadata struct {
		UserEmail string `json:"user_email"`
		UserName  string `json:"user_name"`
		UserRef   string `json:"user_ref"`
	}

	HistoryEvent struct {
		Version  int64    `json:"version"`
		Metadata metadata `json:"metadata"`
	}

	GetHistoryShortInfoResp struct {
		Events []HistoryEvent `json:"events"`
	}
)

var errTooManyRows = fmt.Errorf("can't fetch more than %v rows", maxSeriesRows)

func NewHandler(staticDir fs.FS, jsSettings JSSettings, showInvisible bool, chV1 *util.ClickHouse, chV2 *util.ClickHouse, metadataClient *tlmetadata.Client, diskCache *pcache.DiskCache, jwtHelper *vkuth.JWTHelper, opt HandlerOptions, cfg *Config) (*Handler, error) {
	metadataLoader := metajournal.NewMetricMetaLoader(metadataClient, metajournal.DefaultMetaTimeout)
	diskCacheSuffix := metadataClient.Address // TODO - use cluster name or something here

	tmpl, err := template.ParseFS(staticDir, "index.html")
	if err != nil {
		return nil, fmt.Errorf("failed to parse index.html template: %w", err)
	}
	settings, err := json.Marshal(jsSettings)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal settings to JSON: %w", err)
	}
	cl := config.NewConfigListener(data_model.APIRemoteConfig, cfg)
	metricStorage := metajournal.MakeMetricsStorage(diskCacheSuffix, diskCache, nil, cl.ApplyEventCB)
	metricStorage.Journal().Start(nil, nil, metadataLoader.LoadJournal)
	h := &Handler{
		HandlerOptions: opt,
		showInvisible:  showInvisible,
		staticDir:      http.FS(staticDir),
		indexTemplate:  tmpl,
		indexSettings:  string(settings),
		metadataLoader: metadataLoader,
		ch: map[string]*util.ClickHouse{
			Version1: chV1,
			Version2: chV2,
		},
		metricsStorage: metricStorage,
		tagValueCache: &pcache.Cache{
			Loader: tagValueInverseLoader{
				loadTimeout: metajournal.DefaultMetaTimeout,
				metaClient:  metadataClient,
			}.load,
			DiskCache:               diskCache,
			DiskCacheNamespace:      data_model.TagValueInvertDiskNamespace + diskCacheSuffix,
			MaxMemCacheSize:         data_model.MappingMaxMemCacheSize,
			SpreadCacheTTL:          true,
			DefaultCacheTTL:         data_model.MappingCacheTTLMinimum,
			DefaultNegativeCacheTTL: data_model.MappingNegativeCacheTTL,
			LoadMinInterval:         data_model.MappingMinInterval,
			LoadBurst:               1000,
			Empty: func() pcache.Value {
				var empty pcache.StringValue
				return &empty
			},
		},
		tagValueIDCache: &pcache.Cache{
			Loader: tagValueLoader{
				loadTimeout: metajournal.DefaultMetaTimeout,
				metaClient:  metadataClient,
			}.load,
			DiskCache:               diskCache,
			DiskCacheNamespace:      data_model.TagValueDiskNamespace + diskCacheSuffix,
			MaxMemCacheSize:         data_model.MappingMaxMemCacheSize,
			SpreadCacheTTL:          true,
			DefaultCacheTTL:         data_model.MappingCacheTTLMinimum,
			DefaultNegativeCacheTTL: data_model.MappingNegativeCacheTTL,
			LoadMinInterval:         data_model.MappingMinInterval,
			LoadBurst:               1000,
			Empty: func() pcache.Value {
				var empty pcache.Int32Value
				return &empty
			},
		},
		cacheInvalidateTicker: time.NewTicker(cacheInvalidateCheckInterval),
		cacheInvalidateStop:   make(chan chan struct{}),
		jwtHelper:             jwtHelper,
		plotRenderSem:         semaphore.NewWeighted(maxConcurrentPlots),
		plotTemplate:          ttemplate.Must(ttemplate.New("").Parse(gnuplotTemplate)),
		bufferBytesAlloc:      statshouse.Metric(format.BuiltinMetricAPIBufferBytesAlloc, statshouse.Tags{1: srvfunc.HostnameForStatshouse(), 2: "2"}),
		bufferBytesFree:       statshouse.Metric(format.BuiltinMetricAPIBufferBytesFree, statshouse.Tags{1: srvfunc.HostnameForStatshouse(), 2: "2"}),
		bufferPoolBytesAlloc:  statshouse.Metric(format.BuiltinMetricAPIBufferBytesAlloc, statshouse.Tags{1: srvfunc.HostnameForStatshouse(), 2: "1"}),
		bufferPoolBytesFree:   statshouse.Metric(format.BuiltinMetricAPIBufferBytesFree, statshouse.Tags{1: srvfunc.HostnameForStatshouse(), 2: "1"}),
		bufferPoolBytesTotal:  statshouse.Metric(format.BuiltinMetricAPIBufferBytesTotal, statshouse.Tags{1: srvfunc.HostnameForStatshouse()}),
	}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &h.rUsage)

	h.cache = newTSCacheGroup(cfg.ApproxCacheMaxSize, data_model.LODTables, h.utcOffset, h.loadPoints, cacheDefaultDropEvery)
	h.pointsCache = newPointsCache(cfg.ApproxCacheMaxSize, h.utcOffset, h.loadPoint, time.Now)
	cl.AddChangeCB(func(c config.Config) {
		cfg := c.(*Config)
		h.cache.changeMaxSize(cfg.ApproxCacheMaxSize)
	})
	go h.invalidateLoop()
	h.rmID = statshouse.StartRegularMeasurement(func(client *statshouse.Client) { // TODO - stop
		prevRUsage := h.rUsage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &h.rUsage)
		userTime := float64(h.rUsage.Utime.Nano()-prevRUsage.Utime.Nano()) / float64(time.Second)
		sysTime := float64(h.rUsage.Stime.Nano()-prevRUsage.Stime.Nano()) / float64(time.Second)

		userMetric := client.Metric(format.BuiltinMetricNameUsageCPU, statshouse.Tags{1: strconv.Itoa(format.TagValueIDComponentAPI), 2: strconv.Itoa(format.TagValueIDCPUUsageUser)})
		userMetric.Value(userTime)
		sysMetric := client.Metric(format.BuiltinMetricNameUsageCPU, statshouse.Tags{1: strconv.Itoa(format.TagValueIDComponentAPI), 2: strconv.Itoa(format.TagValueIDCPUUsageSys)})
		sysMetric.Value(sysTime)

		var rss float64
		if st, _ := srvfunc.GetMemStat(0); st != nil {
			rss = float64(st.Res)
		}
		memMetric := client.Metric(format.BuiltinMetricNameUsageMemory, statshouse.Tags{1: strconv.Itoa(format.TagValueIDComponentAPI)})
		memMetric.Value(rss)

		writeActiveQuieries := func(ch *util.ClickHouse, versionTag string) {
			if ch != nil {
				fastLight := client.Metric(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneFastLight), 4: srvfunc.HostnameForStatshouse()})
				fastLight.Value(float64(ch.SemaphoreCountFastLight()))

				fastHeavy := client.Metric(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneFastHeavy), 4: srvfunc.HostnameForStatshouse()})
				fastHeavy.Value(float64(ch.SemaphoreCountFastHeavy()))

				slowLight := client.Metric(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneSlowLight), 4: srvfunc.HostnameForStatshouse()})
				slowLight.Value(float64(ch.SemaphoreCountSlowLight()))

				slowHeavy := client.Metric(format.BuiltinMetricNameAPIActiveQueries, statshouse.Tags{2: versionTag, 3: strconv.Itoa(format.TagValueIDAPILaneSlowHeavy), 4: srvfunc.HostnameForStatshouse()})
				slowHeavy.Value(float64(ch.SemaphoreCountSlowHeavy()))
			}
		}
		writeActiveQuieries(chV1, "1")
		writeActiveQuieries(chV2, "2")
		if n := h.pointFloatsPoolSize.Load(); n != 0 {
			h.bufferPoolBytesTotal.Value(float64(n))
		}
	})
	h.rmIDCache = statshouse.StartRegularMeasurement(func(*statshouse.Client) {
		h.cache.reportStats()
	})
	h.promEngine = promql.NewEngine(h, h.location, h.utcOffset)
	return h, nil
}

func (h *Handler) Close() error {
	statshouse.StopRegularMeasurement(h.rmID)
	statshouse.StopRegularMeasurement(h.rmIDCache)
	h.cacheInvalidateTicker.Stop()

	ch := make(chan struct{})
	h.cacheInvalidateStop <- ch
	<-ch

	return nil
}

func (h *Handler) invalidateLoop() {
	var (
		from = time.Now().Unix()
		seen map[cacheInvalidateLogRow]struct{}
	)
	for {
		select {
		case ch := <-h.cacheInvalidateStop:
			close(ch)
			return
		case <-h.cacheInvalidateTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), cacheInvalidateCheckTimeout)
			from, seen = h.invalidateCache(ctx, from, seen)
			cancel()
		}
	}
}

func (h *Handler) invalidateCache(ctx context.Context, from int64, seen map[cacheInvalidateLogRow]struct{}) (int64, map[cacheInvalidateLogRow]struct{}) {
	uncertain := time.Now().Add(-invalidateLinger).Unix()
	if from > uncertain {
		from = uncertain
	}

	queryBody, err := util.BindQuery(fmt.Sprintf(`
SELECT
  toInt64(time) AS time, toInt64(key1) AS key1
FROM
  %s
WHERE
  metric == ? AND time >= ?
GROUP BY
  time, key1
ORDER BY
  time, key1
LIMIT
  ?
SETTINGS
  optimize_aggregation_in_order = 1
`, _1sTableSH2), format.BuiltinMetricIDContributorsLog, from, cacheInvalidateMaxRows)
	if err != nil {
		log.Printf("[error] cache invalidation log query failed: %v", err)
		return from, seen
	}
	// TODO - write metric with len(rows)
	// TODO - code that works if we hit limit above

	var (
		time    proto.ColInt64
		key1    proto.ColInt64
		todo    = map[int64][]int64{}
		newSeen = map[cacheInvalidateLogRow]struct{}{}
	)
	err = h.doSelect(ctx, util.QueryMetaInto{
		IsFast:  true,
		IsLight: true,
		User:    "cache-update",
		Metric:  format.BuiltinMetricIDContributorsLog,
		Table:   _1sTableSH2,
		Kind:    "cache-update",
	}, Version2, ch.Query{
		Body: queryBody,
		Result: proto.Results{
			{Name: "time", Data: &time},
			{Name: "key1", Data: &key1},
		},
		OnResult: func(_ context.Context, b proto.Block) error {
			for i := 0; i < b.Rows; i++ {
				r := cacheInvalidateLogRow{
					T:  time[i],
					At: key1[i],
				}
				newSeen[r] = struct{}{}
				from = r.T
				if _, ok := seen[r]; ok {
					continue
				}
				for lodLevel := range data_model.LODTables[Version2] {
					t := roundTime(r.At, lodLevel, h.utcOffset)
					w := todo[lodLevel]
					if len(w) == 0 || w[len(w)-1] != t {
						todo[lodLevel] = append(w, t)
					}
				}
			}
			return nil
		}})
	if err != nil {
		log.Printf("[error] cache invalidation log query failed: %v", err)
		return from, seen
	}

	for lodLevel, times := range todo {
		h.cache.Invalidate(lodLevel, times)
		if lodLevel == _1s {
			h.pointsCache.invalidate(times)
		}
	}

	return from, newSeen
}

func (h *Handler) doSelect(ctx context.Context, meta util.QueryMetaInto, version string, query ch.Query) error {
	if version == Version1 && h.ch[version] == nil {
		return fmt.Errorf("legacy ClickHouse database is disabled")
	}

	saveDebugQuery(ctx, query.Body)

	start := time.Now()
	reportQueryKind(ctx, meta.IsFast, meta.IsLight)
	info, err := h.ch[version].Select(ctx, meta, query)
	duration := time.Since(start)
	if h.verbose {
		log.Printf("[debug] SQL for %q done in %v, err: %v", meta.User, duration, err)
	}

	reportTiming(ctx, "ch-select", duration)
	ChSelectMetricDuration(info.Duration, meta.Metric, meta.User, meta.Table, meta.Kind, meta.IsFast, meta.IsLight, err)
	ChSelectProfile(meta.IsFast, meta.IsLight, info.Profile, err)

	return err
}

func (h *Handler) getMetricNameWithNamespace(metricID int32) (string, error) {
	if metricID == format.TagValueIDUnspecified {
		return format.CodeTagValue(format.TagValueIDUnspecified), nil
	}
	if m, ok := format.BuiltinMetrics[metricID]; ok {
		return m.Name, nil
	}
	v := h.metricsStorage.GetMetaMetric(metricID)
	if v == nil {
		return "", fmt.Errorf("metric name for ID %v not found", metricID)
	}
	return v.Name, nil
}

func (h *Handler) getMetricID(ai accessInfo, metricWithNamespace string) (int32, error) {
	if metricWithNamespace == format.CodeTagValue(format.TagValueIDUnspecified) {
		return format.TagValueIDUnspecified, nil
	}
	meta, err := h.getMetricMeta(ai, metricWithNamespace)
	if err != nil {
		return 0, err
	}
	return meta.MetricID, nil
}

// getMetricMeta only checks view access
func (h *Handler) getMetricMeta(ai accessInfo, metricWithNamespace string) (*format.MetricMetaValue, error) {
	if m, ok := format.BuiltinMetricByName[metricWithNamespace]; ok {
		return m, nil
	}
	v := h.metricsStorage.GetMetaMetricByName(metricWithNamespace)
	if v == nil {
		return nil, httpErr(http.StatusNotFound, fmt.Errorf("metric %q not found", metricWithNamespace))
	}
	if !ai.CanViewMetric(*v) { // We are OK with sharing this bit of information with clients
		return nil, httpErr(http.StatusForbidden, fmt.Errorf("metric %q forbidden", metricWithNamespace))
	}
	return v, nil
}

// For stats
func (h *Handler) getMetricIDForStat(metricWithNamespace string) int32 {
	if m, ok := format.BuiltinMetricByName[metricWithNamespace]; ok {
		return m.MetricID
	}
	v := h.metricsStorage.GetMetaMetricByName(metricWithNamespace)
	if v == nil {
		return 0
	}
	return v.MetricID
}

func (h *Handler) getTagValue(tagValueID int32) (string, error) {
	r := h.tagValueCache.GetOrLoad(time.Now(), strconv.FormatInt(int64(tagValueID), 10), nil)
	return pcache.ValueToString(r.Value), r.Err
}

func (h *Handler) getRichTagValue(metricMeta *format.MetricMetaValue, version string, tagID string, tagValueID int32) string {
	// Rich mapping between integers and strings must be perfect (with no duplicates on both sides)
	tag, ok := metricMeta.Name2Tag[tagID]
	if !ok {
		return format.CodeTagValue(tagValueID)
	}
	if tag.IsMetric {
		v, err := h.getMetricNameWithNamespace(tagValueID)
		if err != nil {
			return format.CodeTagValue(tagValueID)
		}
		return v
	}
	if tag.IsNamespace {
		if tagValueID != format.TagValueIDUnspecified {
			if meta := h.metricsStorage.GetNamespace(tagValueID); meta != nil {
				return meta.Name
			}
		}
		return format.CodeTagValue(tagValueID)
	}
	if tag.IsGroup {
		if tagValueID != format.TagValueIDUnspecified {
			if meta := h.metricsStorage.GetGroup(tagValueID); meta != nil {
				return meta.Name
			}
		}
		return format.CodeTagValue(tagValueID)
	}
	if tag.Raw {
		base := int32(0)
		if version == Version1 {
			base = format.TagValueIDRawDeltaLegacy
		}
		return format.CodeTagValue(tagValueID - base)
	}
	if tagValueID == format.TagValueIDMappingFloodLegacy && version == Version1 {
		return format.CodeTagValue(format.TagValueIDMappingFlood)
	}
	switch tagValueID {
	case format.TagValueIDUnspecified, format.TagValueIDMappingFlood:
		return format.CodeTagValue(tagValueID)
	default:
		v, err := h.getTagValue(tagValueID)
		if err != nil {
			return format.CodeTagValue(tagValueID)
		}
		return v
	}
}

func (h *Handler) getTagValueID(tagValue string) (int32, error) {
	r := h.tagValueIDCache.GetOrLoad(time.Now(), tagValue, nil)
	return pcache.ValueToInt32(r.Value), r.Err
}

func (h *Handler) getRichTagValueID(tag *format.MetricMetaTag, version string, tagValue string) (int32, error) {
	id, err := format.ParseCodeTagValue(tagValue)
	if err == nil {
		if version == Version1 && tag.Raw {
			id += format.TagValueIDRawDeltaLegacy
		}
		return id, nil
	}
	if tag.IsMetric {
		return h.getMetricID(accessInfo{insecureMode: true}, tagValue) // we don't consider metric ID to be private
	}
	if tag.IsNamespace {
		if tagValue == format.CodeTagValue(format.TagValueIDUnspecified) {
			return format.TagValueIDUnspecified, nil
		}
		if meta := h.metricsStorage.GetNamespaceByName(tagValue); meta != nil {
			return meta.ID, nil
		}
		return 0, httpErr(http.StatusNotFound, fmt.Errorf("namespace %q not found", tagValue))
	}
	if tag.IsGroup {
		if tagValue == format.CodeTagValue(format.TagValueIDUnspecified) {
			return format.TagValueIDUnspecified, nil
		}
		if meta := h.metricsStorage.GetGroupByName(tagValue); meta != nil {
			return meta.ID, nil
		}
		return 0, httpErr(http.StatusNotFound, fmt.Errorf("group %q not found", tagValue))
	}
	if tag.Raw {
		value, ok := tag.Comment2Value[tagValue]
		if ok {
			id, err = format.ParseCodeTagValue(value)
			return id, err
		}
		// We could return error, but this will stop rendering, so we try conventional mapping also, even for raw tags
	}
	return h.getTagValueID(tagValue)
}

func (h *Handler) getRichTagValueIDs(metricMeta *format.MetricMetaValue, version string, tagID string, tagValues []string) ([]int32, error) {
	tag, ok := metricMeta.Name2Tag[tagID]
	if !ok {
		return nil, fmt.Errorf("tag with name %s not found for metric %s", tagID, metricMeta.Name)
	}
	ids := make([]int32, 0, len(tagValues))
	for _, v := range tagValues {
		id, err := h.getRichTagValueID(&tag, version, v)
		if err != nil {
			if httpCode(err) == http.StatusNotFound {
				continue // ignore values with no mapping
			}
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func formValueParamMetric(r *http.Request) string {
	const formerBuiltin = "__builtin_" // we renamed builtin metrics, removing prefix
	str := r.FormValue(ParamMetric)
	if strings.HasPrefix(str, formerBuiltin) {
		str = "__" + str[len(formerBuiltin):]
	}
	ns := r.FormValue(ParamNamespace)
	return mergeMetricNamespace(ns, str)
}

func (h *Handler) resolveFilter(metricMeta *format.MetricMetaValue, version string, f map[string][]string) (map[string][]interface{}, error) {
	m := make(map[string][]interface{}, len(f))
	for k, values := range f {
		if version == Version1 && k == format.EnvTagID {
			continue // we only support production tables for v1
		}
		if k == format.StringTopTagID {
			for _, val := range values {
				m[k] = append(m[k], unspecifiedToEmpty(val))
			}
		} else {
			ids, err := h.getRichTagValueIDs(metricMeta, version, k, values)
			if err != nil {
				return nil, err
			}
			m[k] = []interface{}{}
			for _, id := range ids {
				m[k] = append(m[k], id)
			}
		}
	}
	return m, nil
}

func (h *Handler) HandleStatic(w http.ResponseWriter, r *http.Request) {
	origPath := r.URL.Path
	switch r.URL.Path {
	case "/":
	case "/index.html":
		r.URL.Path = "/"
	default:
		f, err := h.staticDir.Open(r.URL.Path) // stat is more efficient, but will require manual path manipulations
		if f != nil {
			_ = f.Close()
		}

		// 404 -> index.html, for client-side routing
		if err != nil && os.IsNotExist(err) { // TODO - replace with errors.Is(err, fs.ErrNotExist) when jessie is upgraded to go 1.16
			r.URL.Path = "/"
		}
	}

	switch {
	case r.URL.Path == "/":
		// make sure browser does not use stale versions
		w.Header().Set("Cache-Control", "public, no-cache, must-revalidate")
	case strings.HasPrefix(r.URL.Path, "/static/"):
		// everything under /static/ can be cached indefinitely (filenames contain content hashes)
		w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", cacheMaxAgeSeconds))
	}

	w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
	w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if origPath != "/embed" {
		w.Header().Set("X-Frame-Options", "deny")
	}

	if r.URL.Path == "/" {
		data := struct {
			OpenGraph *openGraphInfo
			Settings  string
		}{
			getOpenGraphInfo(r, origPath),
			h.indexSettings,
		}
		if err := h.indexTemplate.Execute(w, data); err != nil {
			log.Printf("[error] failed to write index.html: %v", err)
		}
	} else {
		http.FileServer(h.staticDir).ServeHTTP(w, r)
	}
}

func (h *Handler) parseAccessToken(r *http.Request, es *endpointStat) (accessInfo, error) {
	ai, err := parseAccessToken(h.jwtHelper, vkuth.GetAccessToken(r), h.protectedMetricPrefixes, h.LocalMode, h.insecureMode)
	if es != nil {
		es.setAccessInfo(ai)
	}
	return ai, err
}

type selectRow struct {
	valID int32
	val   string
	cnt   float64
}

type tagValuesSelectCols struct {
	meta  tagValuesQueryMeta
	valID proto.ColInt32
	val   proto.ColStr
	cnt   proto.ColFloat64
	res   proto.Results
}

func newTagValuesSelectCols(meta tagValuesQueryMeta) *tagValuesSelectCols {
	// NB! Keep columns selection order and names is sync with sql.go code
	c := &tagValuesSelectCols{meta: meta}
	if meta.stringValue {
		c.res = append(c.res, proto.ResultColumn{Name: "_string_value", Data: &c.val})
	} else {
		c.res = append(c.res, proto.ResultColumn{Name: "_value", Data: &c.valID})
	}
	c.res = append(c.res, proto.ResultColumn{Name: "_count", Data: &c.cnt})
	return c
}

func (c *tagValuesSelectCols) rowAt(i int) selectRow {
	row := selectRow{cnt: c.cnt[i]}
	if c.meta.stringValue {
		pos := c.val.Pos[i]
		row.val = string(c.val.Buf[pos.Start:pos.End])
	} else {
		row.valID = c.valID[i]
	}
	return row
}

func sumSeries(data *[]float64, missingValue float64) float64 {
	result := 0.0
	for _, c := range *data {
		if math.IsNaN(c) {
			result += missingValue
		} else {
			result += c
		}
	}
	return result
}

func (h *Handler) HandleFrontendStat(w http.ResponseWriter, r *http.Request) {
	ai, err := h.parseAccessToken(r, nil)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	if ai.service {
		// statistics from bots isn't welcome
		respondJSON(w, nil, 0, 0, httpErr(404, fmt.Errorf("")), h.verbose, ai.user, nil)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	var batch tlstatshouse.AddMetricsBatchBytes
	err = batch.UnmarshalJSON(body)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, nil)
		return
	}
	for _, v := range batch.Metrics {
		if string(v.Name) != format.BuiltinMetricNameIDUIErrors { // metric whitelist
			respondJSON(w, nil, 0, 0, fmt.Errorf("metric is not in whitelist"), h.verbose, ai.user, nil)
			return
		}
		tags := make(statshouse.NamedTags, 0, len(v.Tags))
		for _, v := range v.Tags {
			tags = append(tags, [2]string{string(v.Key), string(v.Value)})
		}
		metric := statshouse.MetricNamed(string(v.Name), tags)
		switch {
		case v.IsSetUnique():
			metric.Uniques(v.Unique)
		case v.IsSetValue():
			metric.Values(v.Value)
		default:
			metric.Count(v.Counter)
		}
	}
	respondJSON(w, nil, 0, 0, nil, h.verbose, ai.user, nil)
}

func getDashboardMetaInfo(d *format.DashboardMeta) DashboardMetaInfo {
	data := map[string]interface{}{}
	var description string
	for k, v := range d.JSONData {
		if k == descriptionFieldName {
			description, _ = v.(string)
		} else {
			data[k] = v
		}
	}
	return DashboardMetaInfo{
		DashboardID: d.DashboardID,
		Name:        d.Name,
		Version:     d.Version,
		UpdateTime:  d.UpdateTime,
		DeletedTime: d.DeleteTime,
		Description: description,
		JSONData:    data,
	}
}

func (h *Handler) getFloatsSlice(n int) *[]float64 {
	sizeInBytes := n * 8
	if n > data_model.MaxSlice {
		s := make([]float64, n)
		h.bufferBytesAlloc.Value(float64(sizeInBytes))
		h.pointFloatsPoolSize.Add(int64(sizeInBytes))
		return &s // should not happen: we should never return more than maxSlice points
	}

	v := h.pointFloatsPool.Get()
	if v == nil {
		s := make([]float64, 0, data_model.MaxSlice)
		v = &s
	}
	ret := v.(*[]float64)
	*ret = (*ret)[:n]
	h.bufferPoolBytesAlloc.Value(float64(sizeInBytes))
	h.pointFloatsPoolSize.Add(int64(sizeInBytes))
	return ret
}

func (h *Handler) putFloatsSlice(s *[]float64) {
	sizeInBytes := len(*s) * 8
	*s = (*s)[:0]

	if cap(*s) <= data_model.MaxSlice {
		h.pointFloatsPool.Put(s)
		h.bufferPoolBytesFree.Value(float64(sizeInBytes))
	} else {
		h.bufferBytesFree.Value(float64(sizeInBytes))
	}
	h.pointFloatsPoolSize.Sub(int64(sizeInBytes))
}

func (h *Handler) maybeAddQuerySeriesTagValue(m map[string]SeriesMetaTag, metricMeta *format.MetricMetaValue, version string, by []string, tagIndex int, tagValueID int32) bool {
	tagID := format.TagID(tagIndex)
	if !containsString(by, tagID) {
		return false
	}
	metaTag := SeriesMetaTag{Value: h.getRichTagValue(metricMeta, version, tagID, tagValueID)}
	if tag, ok := metricMeta.Name2Tag[tagID]; ok {
		metaTag.Comment = tag.ValueComments[metaTag.Value]
		metaTag.Raw = tag.Raw
		metaTag.RawKind = tag.RawKind
	}
	m[format.TagIDLegacy(tagIndex)] = metaTag
	return true
}

type pointsSelectCols struct {
	time         proto.ColInt64
	step         proto.ColInt64
	cnt          proto.ColFloat64
	val          []proto.ColFloat64
	tag          []proto.ColInt32
	tagIx        []int
	tagStr       proto.ColStr
	minMaxHostV1 [2]proto.ColUInt8 // "min" at [0], "max" at [1]
	minMaxHostV2 [2]proto.ColInt32 // "min" at [0], "max" at [1]
	shardNum     proto.ColUInt32
	res          proto.Results
}

func newPointsSelectCols(meta pointsQueryMeta, useTime bool) *pointsSelectCols {
	// NB! Keep columns selection order and names is sync with sql.go code
	c := &pointsSelectCols{
		val:   make([]proto.ColFloat64, meta.vals),
		tag:   make([]proto.ColInt32, 0, len(meta.tags)),
		tagIx: make([]int, 0, len(meta.tags)),
	}
	if useTime {
		c.res = proto.Results{
			{Name: "_time", Data: &c.time},
			{Name: "_stepSec", Data: &c.step},
		}
	}
	for _, id := range meta.tags {
		switch id {
		case format.StringTopTagID:
			c.res = append(c.res, proto.ResultColumn{Name: "key_s", Data: &c.tagStr})
		case format.ShardTagID:
			c.res = append(c.res, proto.ResultColumn{Name: "key_shard_num", Data: &c.shardNum})
		default:
			c.tag = append(c.tag, proto.ColInt32{})
			c.res = append(c.res, proto.ResultColumn{Name: "key" + id, Data: &c.tag[len(c.tag)-1]})
			c.tagIx = append(c.tagIx, format.TagIndex(id))
		}
	}
	c.res = append(c.res, proto.ResultColumn{Name: "_count", Data: &c.cnt})
	for i := 0; i < meta.vals; i++ {
		c.res = append(c.res, proto.ResultColumn{Name: "_val" + strconv.Itoa(i), Data: &c.val[i]})
	}
	if meta.minMaxHost {
		if meta.version == Version1 {
			c.res = append(c.res, proto.ResultColumn{Name: "_minHost", Data: &c.minMaxHostV1[0]})
			c.res = append(c.res, proto.ResultColumn{Name: "_maxHost", Data: &c.minMaxHostV1[1]})
		} else {
			c.res = append(c.res, proto.ResultColumn{Name: "_minHost", Data: &c.minMaxHostV2[0]})
			c.res = append(c.res, proto.ResultColumn{Name: "_maxHost", Data: &c.minMaxHostV2[1]})
		}
	}
	return c
}

func (c *pointsSelectCols) rowAt(i int) tsSelectRow {
	row := tsSelectRow{
		time:     c.time[i],
		stepSec:  c.step[i],
		tsValues: tsValues{countNorm: c.cnt[i]},
	}
	for j := 0; j < len(c.val); j++ {
		row.val[j] = c.val[j][i]
	}
	for j := range c.tag {
		row.tag[c.tagIx[j]] = c.tag[j][i]
	}
	if c.tagStr.Pos != nil && i < len(c.tagStr.Pos) {
		copy(row.tagStr[:], c.tagStr.Buf[c.tagStr.Pos[i].Start:c.tagStr.Pos[i].End])
	}
	if len(c.minMaxHostV2[0]) != 0 {
		row.host[0] = c.minMaxHostV2[0][i]
	}
	if len(c.minMaxHostV2[1]) != 0 {
		row.host[1] = c.minMaxHostV2[1][i]
	}
	if c.shardNum != nil {
		row.shardNum = c.shardNum[i]
	}
	return row
}

func (c *pointsSelectCols) rowAtPoint(i int) pSelectRow {
	row := pSelectRow{
		tsValues: tsValues{countNorm: c.cnt[i]},
	}
	for j := 0; j < len(c.val); j++ {
		row.val[j] = c.val[j][i]
	}
	for j := range c.tag {
		row.tag[c.tagIx[j]] = c.tag[j][i]
	}
	if c.tagStr.Pos != nil && i < len(c.tagStr.Pos) {
		copy(row.tagStr[:], c.tagStr.Buf[c.tagStr.Pos[i].Start:c.tagStr.Pos[i].End])
	}
	if len(c.minMaxHostV2[0]) != 0 {
		row.host[0] = c.minMaxHostV2[0][i]
	}
	if len(c.minMaxHostV2[1]) != 0 {
		row.host[1] = c.minMaxHostV2[1][i]
	}
	return row
}

func maybeAddQuerySeriesTagValueString(m map[string]SeriesMetaTag, by []string, tagValuePtr *stringFixed) string {
	tagValue := skeyFromFixedString(tagValuePtr)

	if containsString(by, format.StringTopTagID) {
		m[format.LegacyStringTopTagID] = SeriesMetaTag{Value: emptyToUnspecified(tagValue)}
		return tagValue
	}
	return ""
}

func skeyFromFixedString(tagValuePtr *stringFixed) string {
	tagValue := ""
	nullIx := bytes.IndexByte(tagValuePtr[:], 0)
	switch nullIx {
	case 0: // do nothing
	case -1:
		tagValue = string(tagValuePtr[:])
	default:
		tagValue = string(tagValuePtr[:nullIx])
	}
	return tagValue
}
func replaceInfNan(v *float64) {
	if math.IsNaN(*v) {
		*v = -1.111111 // Motivation - 99.9% of our graphs are >=0, -1.111111 will stand out. But we do not expect NaNs.
		return
	}
	if math.IsInf(*v, 1) {
		*v = -2.222222 // Motivation - as above, distinct value for debug
		return
	}
	if math.IsInf(*v, -1) {
		*v = -3.333333 // Motivation - as above, distinct value for debug
		return
	}
	// Motivation - we store some values as float32 anyway. Also, most code does not work well, if close to float64 limits
	if *v > math.MaxFloat32 {
		*v = math.MaxFloat32
		return
	}
	if *v < -math.MaxFloat32 {
		*v = -math.MaxFloat32
		return
	}
}

func (h *Handler) loadPoints(ctx context.Context, pq *preparedPointsQuery, lod data_model.LOD, ret [][]tsSelectRow, retStartIx int) (int, error) {
	query, args, err := loadPointsQuery(pq, lod, h.utcOffset)
	if err != nil {
		return 0, err
	}

	rows := 0
	cols := newPointsSelectCols(args, true)
	isFast := lod.IsFast()
	isLight := pq.isLight()
	metric := pq.metricID
	table := lod.Table
	kind := pq.kind
	start := time.Now()
	err = h.doSelect(ctx, util.QueryMetaInto{
		IsFast:  isFast,
		IsLight: isLight,
		User:    pq.user,
		Metric:  metric,
		Table:   table,
		Kind:    kind.String(),
	}, pq.version, ch.Query{
		Body:   query,
		Result: cols.res,
		OnResult: func(_ context.Context, block proto.Block) error {
			for i := 0; i < block.Rows; i++ {
				replaceInfNan(&cols.cnt[i])
				for j := 0; j < len(cols.val); j++ {
					replaceInfNan(&cols.val[j][i])
				}
				row := cols.rowAt(i)
				ix, err := lod.IndexOf(row.time)
				if err != nil {
					return err
				}
				ix += retStartIx
				ret[ix] = append(ret[ix], row)
			}
			rows += block.Rows
			return nil
		}})
	duration := time.Since(start)
	if err != nil {
		return 0, err
	}

	if rows == maxSeriesRows {
		return rows, errTooManyRows // prevent cache being populated by incomplete data
	}
	if h.verbose {
		log.Printf("[debug] loaded %v rows from %v (%v timestamps, %v to %v step %v) for %q in %v",
			rows,
			lod.Table,
			(lod.ToSec-lod.FromSec)/lod.StepSec,
			time.Unix(lod.FromSec, 0),
			time.Unix(lod.ToSec, 0),
			time.Duration(lod.StepSec)*time.Second,
			pq.user,
			duration,
		)
	}

	return rows, nil
}

func (h *Handler) loadPoint(ctx context.Context, pq *preparedPointsQuery, lod data_model.LOD) ([]pSelectRow, error) {
	query, args, err := loadPointQuery(pq, lod, h.utcOffset)
	if err != nil {
		return nil, err
	}
	ret := make([]pSelectRow, 0)
	rows := 0
	cols := newPointsSelectCols(args, false)
	isFast := lod.IsFast()
	isLight := pq.isLight()
	metric := pq.metricID
	table := lod.Table
	kind := pq.kind
	err = h.doSelect(ctx, util.QueryMetaInto{
		IsFast:  isFast,
		IsLight: isLight,
		User:    pq.user,
		Metric:  metric,
		Table:   table,
		Kind:    kind.String(),
	}, pq.version, ch.Query{
		Body:   query,
		Result: cols.res,
		OnResult: func(_ context.Context, block proto.Block) error {
			for i := 0; i < block.Rows; i++ {
				//todo check
				replaceInfNan(&cols.cnt[i])
				for j := 0; j < len(cols.val); j++ {
					replaceInfNan(&cols.val[j][i])
				}
				row := cols.rowAtPoint(i)
				ret = append(ret, row)
			}
			rows += block.Rows
			return nil
		}})
	if err != nil {
		return nil, err
	}

	if rows == maxSeriesRows {
		return ret, errTooManyRows // prevent cache being populated by incomplete data
	}
	if h.verbose {
		log.Printf("[debug] loaded %v rows from %v (%v to %v) for %q in",
			rows,
			lod.Table,
			time.Unix(lod.FromSec, 0),
			time.Unix(lod.ToSec, 0),
			pq.user,
		)
	}

	return ret, nil
}

func stableMulDiv(v float64, mul int64, div int64) float64 {
	// Often desiredStepMul is multiple of row.StepSec
	if mul%div == 0 {
		// so we make FP desiredStepMul by row.StepSec division first which often gives us whole number, even 1 in many cases
		return v * float64(mul/div)
	}
	// if we do multiplication first, (a * 360) might lose mantissa bits so next division by 360 will lose precision
	// hopefully 2x divisions on this code path will not slow us down too much.
	return v * float64(mul) / float64(div)
}

func selectTSValue(what data_model.DigestWhat, maxHost bool, desiredStepMul int64, row *tsSelectRow) float64 {
	switch what {
	case data_model.DigestCount:
		return stableMulDiv(row.countNorm, desiredStepMul, row.stepSec)
	case data_model.DigestCountRaw:
		return row.countNorm
	case data_model.DigestCountSec:
		return row.countNorm / float64(row.stepSec)
	case data_model.DigestCardinality:
		if maxHost {
			return stableMulDiv(row.val[5], desiredStepMul, row.stepSec)
		}
		return stableMulDiv(row.val[0], desiredStepMul, row.stepSec)
	case data_model.DigestCardinalityRaw:
		if maxHost {
			return row.val[5]
		}
		return row.val[0]
	case data_model.DigestCardinalitySec:
		if maxHost {
			return row.val[5] / float64(row.stepSec)
		}
		return row.val[0] / float64(row.stepSec)
	case data_model.DigestMin:
		return row.val[0]
	case data_model.DigestMax:
		return row.val[1]
	case data_model.DigestAvg:
		return row.val[2]
	case data_model.DigestSum:
		return stableMulDiv(row.val[3], desiredStepMul, row.stepSec)
	case data_model.DigestSumRaw:
		return row.val[3]
	case data_model.DigestSumSec:
		return row.val[3] / float64(row.stepSec)
	case data_model.DigestStdDev:
		return row.val[4]
	case data_model.DigestStdVar:
		return row.val[4] * row.val[4]
	case data_model.DigestP0_1:
		return row.val[0]
	case data_model.DigestP1:
		return row.val[1]
	case data_model.DigestP5:
		return row.val[2]
	case data_model.DigestP10:
		return row.val[3]
	case data_model.DigestP25:
		return row.val[0]
	case data_model.DigestP50:
		return row.val[1]
	case data_model.DigestP75:
		return row.val[2]
	case data_model.DigestP90:
		return row.val[3]
	case data_model.DigestP95:
		return row.val[4]
	case data_model.DigestP99:
		return row.val[5]
	case data_model.DigestP999:
		return row.val[6]
	case data_model.DigestUnique:
		return stableMulDiv(row.val[0], desiredStepMul, row.stepSec)
	case data_model.DigestUniqueSec:
		return row.val[0] / float64(row.stepSec)
	default:
		return math.NaN()
	}
}

func toSec(d time.Duration) int64 {
	return int64(d / time.Second)
}

func containsString(s []string, v string) bool {
	for _, sv := range s {
		if sv == v {
			return true
		}
	}
	return false
}

func emptyToUnspecified(s string) string {
	if s == "" {
		return format.CodeTagValue(format.TagValueIDUnspecified)
	}
	return s
}

func unspecifiedToEmpty(s string) string {
	if s == format.CodeTagValue(format.TagValueIDUnspecified) {
		return ""
	}
	return s
}

func (h *Handler) checkReadOnlyMode(w http.ResponseWriter, _ *http.Request) (readOnlyMode bool) {
	if h.readOnly {
		w.WriteHeader(406)
		_, _ = w.Write([]byte("readonly mode"))
		return true
	}
	return false
}

func queryClientCacheDuration(immutable bool) (cache time.Duration, cacheStale time.Duration) {
	if immutable {
		return queryClientCacheImmutable, queryClientCacheStaleImmutable
	}
	return queryClientCache, queryClientCacheStale
}

func lessThan(l RowMarker, r tsSelectRow, skey string, orEq bool, fromEnd bool) bool {
	if fromEnd {
		if l.Time != r.time {
			return l.Time > r.time
		}
		for i := range l.Tags {
			lv := l.Tags[i].Value
			rv := r.tag[l.Tags[i].Index]
			if lv != rv {
				return lv > rv
			}
		}
		if orEq {
			return l.SKey >= skey
		}
		return l.SKey > skey
	} else {
		if l.Time != r.time {
			return l.Time < r.time
		}
		for i := range l.Tags {
			lv := l.Tags[i].Value
			rv := r.tag[l.Tags[i].Index]
			if lv != rv {
				return lv < rv
			}
		}
		if orEq {
			return l.SKey <= skey
		}
		return l.SKey < skey
	}
}

func (s queryTableRows) Less(i, j int) bool {
	l, r := s[i].rowRepr, s[j].rowRepr
	if l.Time != r.Time {
		return l.Time < r.Time
	}
	if len(l.Tags) != len(r.Tags) {
		return len(l.Tags) < len(r.Tags)
	}
	for i := range l.Tags {
		lv := l.Tags[i].Value
		rv := r.Tags[i].Value
		if lv != rv {
			return lv < rv
		}
	}

	return l.SKey < r.SKey
}

func (s queryTableRows) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s queryTableRows) Len() int {
	return len(s)
}

func (r *DashboardTimeRange) UnmarshalJSON(bs []byte) error {
	var m map[string]any
	if err := json.Unmarshal(bs, &m); err != nil {
		return err
	}
	if from, ok := m["from"].(float64); ok {
		r.From = int64(from)
	}
	switch to := m["to"].(type) {
	case float64:
		r.To = strconv.Itoa(int(to))
	case string:
		r.To = to
	}
	return nil
}

func (r *DashboardTimeShifts) UnmarshalJSON(bs []byte) error {
	var s []any
	if err := json.Unmarshal(bs, &s); err != nil {
		return err
	}
	for _, v := range s {
		if n, ok := v.(float64); ok {
			*r = append(*r, strconv.Itoa(int(n)))
		}
	}
	return nil
}

func (r *seriesRequest) validate() error {
	if r.avoidCache && !r.ai.isAdmin() {
		return httpErr(404, fmt.Errorf(""))
	}
	if r.step == _1M {
		for _, v := range r.shifts {
			if (v/time.Second)%_1M != 0 {
				return httpErr(http.StatusBadRequest, fmt.Errorf("time shift %v can't be used with month interval", v))
			}
		}
	}
	return nil
}

func mergeMetricNamespace(namespace string, metric string) string {
	if strings.Contains(namespace, format.NamespaceSeparator) {
		return metric
	}
	return format.NamespaceName(namespace, metric)
}
