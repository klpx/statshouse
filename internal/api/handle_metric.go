// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/mailru/easyjson"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/promql"
)

type (
	seriesResponse struct {
		*promql.TimeSeries
		metric          *format.MetricMetaValue
		promQL          string
		trace           []string
		extraPointLeft  bool
		extraPointRight bool
	}
)

func (h *Handler) HandleGetMetricsList(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStatHTTP(EndpointMetricList, r.Method, 0, "", r.FormValue(paramPriority))
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	resp, cache, err := h.handleGetMetricsList(ai)
	respondJSON(w, resp, cache, queryClientCacheStale, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandleGetMetric(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStatHTTP(EndpointMetric, r.Method, h.getMetricIDForStat(r.FormValue(ParamMetric)), "", r.FormValue(paramPriority))
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	resp, cache, err := h.handleGetMetric(r.Context(), ai, formValueParamMetric(r), r.FormValue(ParamID), r.FormValue(ParamEntityVersion))
	respondJSON(w, resp, cache, 0, err, h.verbose, ai.user, sl) // we don't want clients to see stale metadata
}

func (h *Handler) HandleSeriesQuery(w http.ResponseWriter, r *http.Request) {
	var err error
	var req seriesRequest
	sl := newEndpointStatHTTP(EndpointQuery, r.Method, h.getMetricIDForStat(r.FormValue(ParamMetric)), r.FormValue(paramDataFormat), r.FormValue(paramPriority))
	if req, err = h.parseHTTPRequest(r); err == nil {
		if req.ai, err = h.parseAccessToken(r, sl); err == nil {
			err = req.validate()
		}
	}
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, req.ai.user, sl)
		return
	}
	s, cancel, err := h.handleSeriesRequestS(withEndpointStat(r.Context(), sl), req, sl, make([]seriesResponse, 2))
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, req.ai.user, sl)
		return
	}
	defer cancel()
	switch {
	case r.FormValue(paramDataFormat) == dataFormatCSV:
		exportCSV(w, h.buildSeriesResponse(s...), req.metricWithNamespace, sl)
	default:
		res := h.buildSeriesResponse(s...)
		cache, cacheStale := queryClientCacheDuration(res.immutable)
		respondJSON(w, res, cache, cacheStale, nil, h.verbose, req.ai.user, sl)
	}
}

func (h *Handler) HandleGetTable(w http.ResponseWriter, r *http.Request) {
	var err error
	var req seriesRequest
	sl := newEndpointStatHTTP(EndpointTable, r.Method, h.getMetricIDForStat(r.FormValue(ParamMetric)), r.FormValue(paramDataFormat), r.FormValue(paramPriority))
	if req, err = h.parseHTTPRequest(r); err == nil {
		if req.ai, err = h.parseAccessToken(r, sl); err == nil {
			err = req.validate()
		}
	}
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, req.ai.user, sl)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.querySelectTimeout)
	defer cancel()

	if req.numResults <= 0 || maxTableRowsPage < req.numResults {
		req.numResults = maxTableRowsPage
	}
	respTable, immutable, err := h.handleGetTable(ctx, req.ai, true, req)
	if h.verbose && err == nil {
		log.Printf("[debug] handled query (%v rows) for %q in %v", len(respTable.Rows), req.ai.user, time.Since(sl.timestamp))
	}

	cache, cacheStale := queryClientCacheDuration(immutable)
	respondJSON(w, respTable, cache, cacheStale, err, h.verbose, req.ai.user, sl)
}

func (h *Handler) HandlePointQuery(w http.ResponseWriter, r *http.Request) {
	var err error
	var req seriesRequest
	sl := newEndpointStatHTTP(EndpointPoint, r.Method, h.getMetricIDForStat(r.FormValue(ParamMetric)), r.FormValue(paramDataFormat), r.FormValue(paramPriority))
	if req, err = h.parseHTTPRequest(r); err == nil {
		if req.ai, err = h.parseAccessToken(r, sl); err == nil {
			err = req.validate()
		}
	}
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, req.ai.user, sl)
		return
	}
	s, cancel, err := h.handleSeriesRequest(
		withEndpointStat(r.Context(), sl), req,
		seriesRequestOptions{mode: data_model.PointQuery, trace: true})
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, req.ai.user, sl)
		return
	}
	defer cancel()
	switch {
	case r.FormValue(paramDataFormat) == dataFormatCSV:
		respondJSON(w, h.buildPointResponse(s), 0, 0, httpErr(http.StatusBadRequest, nil), h.verbose, req.ai.user, sl)
	default:
		immutable := req.to.Before(time.Now().Add(invalidateFrom))
		cache, cacheStale := queryClientCacheDuration(immutable)
		respondJSON(w, h.buildPointResponse(s), cache, cacheStale, err, h.verbose, req.ai.user, sl)
	}
}

func (h *Handler) HandleGetRender(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStatHTTP(EndpointRender, r.Method, h.getMetricIDForStat(r.FormValue(ParamMetric)), r.FormValue(paramDataFormat), r.FormValue(paramPriority))
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.querySelectTimeout)
	defer cancel()

	s, err := h.parseHTTPRequestS(r, 12)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}

	resp, immutable, err := h.handleGetRender(
		ctx, ai,
		renderRequest{
			seriesRequest: s,
			renderWidth:   r.FormValue(paramRenderWidth),
			renderFormat:  r.FormValue(paramDataFormat),
		}, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}

	cache, cacheStale := queryClientCacheDuration(immutable)
	respondPlot(w, resp.format, resp.data, cache, cacheStale, h.verbose, ai.user, sl)
}

func (s seriesResponse) queryFuncShiftAndTagsAt(i int) (string, int64, map[string]SeriesMetaTag) {
	d := s.Series.Data[i]
	tags := make(map[string]SeriesMetaTag, len(d.Tags.ID2Tag))
	timsShift := -d.Offset
	for id, tag := range d.Tags.ID2Tag {
		if len(tag.SValue) == 0 || tag.ID == labels.MetricName || tag.ID == promql.LabelWhat {
			continue
		}
		if tag.ID == promql.LabelOffset {
			timsShift = -(d.Offset + int64(tag.Value))
			continue
		}
		k := id
		v := SeriesMetaTag{Value: tag.SValue}
		if tag.Index != 0 {
			var name string
			index := tag.Index - promql.SeriesTagIndexOffset
			if index == format.StringTopTagIndex {
				k = format.LegacyStringTopTagID
				name = format.StringTopTagID
			} else {
				k = format.TagIDLegacy(index)
				name = format.TagID(index)
			}
			if s.Series.Meta.Metric != nil {
				if meta, ok := s.Series.Meta.Metric.Name2Tag[name]; ok {
					v.Comment = meta.ValueComments[v.Value]
					v.Raw = meta.Raw
					v.RawKind = meta.RawKind
				}
			}
		}
		tags[k] = v

	}
	queryFunc := d.What.QueryF
	if queryFunc == "" {
		queryFunc = promql.DigestWhatString(d.What.Digest)
	}
	return queryFunc, timsShift, tags
}

func (h *Handler) handleGetMetricsList(ai accessInfo) (*GetMetricsListResp, time.Duration, error) {
	ret := &GetMetricsListResp{
		Metrics: []metricShortInfo{},
	}
	for _, m := range format.BuiltinMetrics {
		if !h.showInvisible && !m.Visible { // we have invisible builtin metrics
			continue
		}
		ret.Metrics = append(ret.Metrics, metricShortInfo{Name: m.Name})
	}
	for _, v := range h.metricsStorage.GetMetaMetricList(h.showInvisible) {
		if ai.CanViewMetric(*v) {
			ret.Metrics = append(ret.Metrics, metricShortInfo{Name: v.Name})
		}
	}

	sort.Slice(ret.Metrics, func(i int, j int) bool { return ret.Metrics[i].Name < ret.Metrics[j].Name })

	return ret, defaultCacheTTL, nil
}

func (h *Handler) handleGetMetric(ctx context.Context, ai accessInfo, metricName string, metricIDStr string, versionStr string) (*MetricInfo, time.Duration, error) {
	if metricIDStr != "" {
		metricID, err := strconv.ParseInt(metricIDStr, 10, 32)
		if err != nil {
			return nil, 0, fmt.Errorf("can't parse %s", metricIDStr)
		}
		if versionStr == "" {
			metricName = h.getMetricNameByID(int32(metricID))
			if metricName == "" {
				return nil, 0, fmt.Errorf("can't find metric %d", metricID)
			}
		} else {
			version, err := strconv.ParseInt(versionStr, 10, 64)
			if err != nil {
				return nil, 0, fmt.Errorf("can't parse %s", versionStr)
			}
			m, err := h.metadataLoader.GetMetric(ctx, metricID, version)
			if err != nil {
				return nil, 0, err
			}
			return &MetricInfo{
				Metric: m,
			}, defaultCacheTTL, nil
		}
	}
	v, err := h.getMetricMeta(ai, metricName)
	if err != nil {
		return nil, 0, err
	}
	return &MetricInfo{
		Metric: *v,
	}, defaultCacheTTL, nil
}

func (h *Handler) handleSeriesRequestS(ctx context.Context, req seriesRequest, es *endpointStat, s []seriesResponse) ([]seriesResponse, func(), error) {
	var err error
	var cancelT func()
	var freeBadges func()
	var freeRes func()
	ctx, cancelT = context.WithTimeout(ctx, h.querySelectTimeout)
	cancel := func() {
		cancelT()
		if freeBadges != nil {
			freeBadges()
		}
		if freeRes != nil {
			freeRes()
		}
	}
	if req.verbose && len(s) > 1 {
		var g *errgroup.Group
		g, ctx = errgroup.WithContext(ctx)
		g.Go(func() (err error) {
			s[0], freeRes, err = h.handleSeriesRequest(withEndpointStat(ctx, es), req, seriesRequestOptions{
				trace: true,
				metricCallback: func(meta *format.MetricMetaValue) {
					req.metricWithNamespace = meta.Name
					if meta.MetricID != format.BuiltinMetricIDBadges {
						g.Go(func() (err error) {
							s[1], freeBadges, err = h.queryBadges(ctx, req, meta)
							return err
						})
					}
				},
			})
			return err
		})
		err = g.Wait()
	} else {
		s[0], freeRes, err = h.handleSeriesRequest(withEndpointStat(ctx, es), req, seriesRequestOptions{
			trace: true,
		})
	}
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return s, cancel, nil
}

func (h *Handler) queryBadges(ctx context.Context, req seriesRequest, meta *format.MetricMetaValue) (seriesResponse, func(), error) {
	var res seriesResponse
	ctx = debugQueriesContext(ctx, &res.trace)
	ctx = promql.TraceContext(ctx, &res.trace)
	req.ai.skipBadgesValidation = true
	v, cleanup, err := h.promEngine.Exec(
		withAccessInfo(ctx, &req.ai),
		promql.Query{
			Start: req.from.Unix(),
			End:   req.to.Unix(),
			Step:  req.step,
			Expr:  fmt.Sprintf(`%s{@what="countraw,avg",@by="1,2",2=" 0",2=" %d"}`, format.BuiltinMetricNameBadges, meta.MetricID),
			Options: promql.Options{
				ExplicitGrouping: true,
				QuerySequential:  h.querySequential,
				ScreenWidth:      req.screenWidth,
			},
		})
	if err != nil {
		return seriesResponse{}, nil, err
	}
	res.TimeSeries, _ = v.(*promql.TimeSeries)
	return res, cleanup, nil
}

func (h *Handler) getMetricNameByID(metricID int32) string {
	meta := format.BuiltinMetrics[metricID]
	if meta != nil {
		return meta.Name
	}
	meta = h.metricsStorage.GetMetaMetric(metricID)
	if meta != nil {
		return meta.Name
	}
	return ""
}

func (h *Handler) handleSeriesRequest(ctx context.Context, req seriesRequest, opt seriesRequestOptions) (seriesResponse, func(), error) {
	err := req.validate()
	if err != nil {
		return seriesResponse{}, nil, err
	}
	var limit int
	var promqlGenerated bool
	if len(req.promQL) == 0 {
		req.promQL, err = h.getPromQuery(req)
		if err != nil {
			return seriesResponse{}, nil, httpErr(http.StatusBadRequest, err)
		}
		promqlGenerated = true
	} else {
		limit = req.numResults
	}
	if opt.timeNow.IsZero() {
		opt.timeNow = time.Now()
	}
	var offsets = make([]int64, 0, len(req.shifts))
	for _, v := range req.shifts {
		offsets = append(offsets, -toSec(v))
	}
	var res seriesResponse
	if opt.trace {
		ctx = debugQueriesContext(ctx, &res.trace)
		ctx = promql.TraceContext(ctx, &res.trace)
	}
	v, cleanup, err := h.promEngine.Exec(
		withAccessInfo(ctx, &req.ai),
		promql.Query{
			Start: req.from.Unix(),
			End:   req.to.Unix(),
			Step:  req.step,
			Expr:  req.promQL,
			Options: promql.Options{
				Version:          req.version,
				Mode:             opt.mode,
				AvoidCache:       req.avoidCache,
				TimeNow:          opt.timeNow.Unix(),
				Extend:           req.excessPoints,
				ExplicitGrouping: true,
				RawBucketLabel:   !opt.strBucketLabel,
				QuerySequential:  h.querySequential,
				TagWhat:          promqlGenerated,
				ScreenWidth:      req.screenWidth,
				MaxHost:          req.maxHost,
				Offsets:          offsets,
				Limit:            limit,
				Rand:             opt.rand,
				ExprQueriesSingleMetricCallback: func(metric *format.MetricMetaValue) {
					res.metric = metric
					if opt.metricCallback != nil {
						opt.metricCallback(metric)
					}
				},
				Vars: req.vars,
			},
		})
	if err != nil {
		return seriesResponse{}, nil, err
	}
	if res.TimeSeries, _ = v.(*promql.TimeSeries); res.TimeSeries == nil {
		cleanup()
		return seriesResponse{}, nil, fmt.Errorf("string literals are not supported")
	}
	if promqlGenerated {
		res.promQL = req.promQL
	}
	if len(res.Time) != 0 {
		res.extraPointLeft = res.Time[0] < req.from.Unix()
		res.extraPointRight = req.to.Unix() <= res.Time[len(res.Time)-1]
	}
	// clamp values because JSON doesn't support "Inf" values,
	// extra large values usually don't make sense.
	// TODO: don't lose values, pass large values (including "Inf" as is)
	for _, d := range res.Series.Data {
		for i, v := range *d.Values {
			if v < -math.MaxFloat32 {
				(*d.Values)[i] = -math.MaxFloat32
			} else if v > math.MaxFloat32 {
				(*d.Values)[i] = math.MaxFloat32
			}
		}
	}
	return res, cleanup, nil
}

func (h *Handler) buildPointResponse(s seriesResponse) *GetPointResp {
	res := &GetPointResp{
		PointMeta:    make([]QueryPointsMeta, 0),
		PointData:    make([]float64, 0),
		DebugQueries: s.trace,
	}
	for i, d := range s.Series.Data {
		meta := QueryPointsMeta{
			FromSec: s.Time[0],
			ToSec:   s.Time[1],
		}
		meta.What, meta.TimeShift, meta.Tags = s.queryFuncShiftAndTagsAt(i)
		if s.metric != nil {
			meta.Name = s.metric.Name
		}
		if maxHost := d.GetSMaxHosts(h); len(maxHost) != 0 {
			meta.MaxHost = maxHost[0]
		}
		res.PointMeta = append(res.PointMeta, meta)
		res.PointData = append(res.PointData, (*d.Values)[0])
	}
	return res
}

func (h *Handler) handleGetRender(ctx context.Context, ai accessInfo, req renderRequest, es *endpointStat) (*renderResponse, bool, error) {
	width, err := parseRenderWidth(req.renderWidth)
	if err != nil {
		return nil, false, err
	}

	format_, err := parseRenderFormat(req.renderFormat)
	if err != nil {
		return nil, false, err
	}

	var (
		s         = make([]*SeriesResponse, len(req.seriesRequest))
		immutable = true
		seriesNum = 0
		pointsNum = 0
	)
	for i, r := range req.seriesRequest {
		r.ai = ai
		if r.numResults == 0 || r.numResults > 15 {
			// Limit number of plots on the preview because
			// there is no point in drawing a lot on a small canvas
			// (it takes time and doesn't look good)
			r.numResults = 15
		}
		start := time.Now()
		v, cancel, err := h.handleSeriesRequest(withEndpointStat(ctx, es), r, seriesRequestOptions{
			metricCallback: func(meta *format.MetricMetaValue) {
				req.seriesRequest[i].metricWithNamespace = meta.Name
			},
			strBucketLabel: true,
		})
		if err != nil {
			return nil, false, err
		}
		defer cancel() // hold until plot call
		res := h.buildSeriesResponse(v)
		immutable = immutable && res.immutable
		if h.verbose {
			log.Printf("[debug] handled render query (%v series x %v points each) for %q in %v", len(res.Series.SeriesMeta), len(res.Series.Time), ai.user, time.Since(start))
			seriesNum += len(res.Series.SeriesMeta)
			pointsNum += len(res.Series.SeriesMeta) * len(res.Series.Time)
		}
		h.colorize(res)
		s[i] = res
	}

	ctx, cancel := context.WithTimeout(ctx, plotRenderTimeout)
	defer cancel()

	err = h.plotRenderSem.Acquire(ctx, 1)
	if err != nil {
		return nil, false, err
	}
	defer h.plotRenderSem.Release(1)

	start := time.Now()
	png, err := plot(ctx, format_, true, s, h.utcOffset, req.seriesRequest, width, h.plotTemplate)
	es.timings.Report("plot", time.Since(start))
	if err != nil {
		return nil, false, err
	}
	if h.verbose {
		log.Printf("[debug] handled render plot (%v series, %v points) for %q in %v", seriesNum, pointsNum, req.ai.user, time.Since(start))
	}

	return &renderResponse{
		format: format_,
		data:   png,
	}, immutable, nil
}

func (h *Handler) handleGetTable(ctx context.Context, ai accessInfo, debugQueries bool, req seriesRequest) (resp *GetTableResp, immutable bool, err error) {
	metricMeta, err := h.getMetricMeta(ai, req.metricWithNamespace)
	if err != nil {
		return nil, false, err
	}
	err = validateQuery(metricMeta, req.version)
	if err != nil {
		return nil, false, err
	}
	mappedFilterIn, err := h.resolveFilter(metricMeta, req.version, req.filterIn)
	if err != nil {
		return nil, false, err
	}
	mappedFilterNotIn, err := h.resolveFilter(metricMeta, req.version, req.filterNotIn)
	if err != nil {
		return nil, false, err
	}
	lods, err := data_model.GetLODs(data_model.GetTimescaleArgs{
		Version:     req.version,
		Start:       req.from.Unix(),
		End:         req.to.Unix(),
		Step:        req.step,
		ScreenWidth: req.screenWidth,
		TimeNow:     time.Now().Unix(),
		Metric:      metricMeta,
		Location:    h.location,
		UTCOffset:   h.utcOffset,
	})
	if err != nil {
		return nil, false, err
	}
	desiredStepMul := int64(1)
	if req.step != 0 {
		desiredStepMul = int64(req.step)
	}
	if req.fromEnd {
		for i := 0; i < len(lods)/2; i++ {
			temp := lods[i]
			j := len(lods) - i - 1
			lods[i] = lods[j]
			lods[j] = temp
		}
	}

	var sqlQueries []string
	if debugQueries {
		ctx = debugQueriesContext(ctx, &sqlQueries)
	}
	queryRows, hasMore, err := getTableFromLODs(ctx, lods, tableReqParams{
		req:               req,
		user:              ai.user,
		metricMeta:        metricMeta,
		isStringTop:       metricMeta.StringTopDescription != "",
		mappedFilterIn:    mappedFilterIn,
		mappedFilterNotIn: mappedFilterNotIn,
		rawValue:          req.screenWidth == 0 || req.step == _1M,
		desiredStepMul:    desiredStepMul,
		location:          h.location,
	}, h.cache.Get, h.maybeAddQuerySeriesTagValue)
	if err != nil {
		return nil, false, err
	}
	var firstRowStr, lastRowStr string
	if len(queryRows) > 0 {
		lastRowStr, err = encodeFromRows(&queryRows[len(queryRows)-1].rowRepr)
		if err != nil {
			return nil, false, err
		}
		firstRowStr, err = encodeFromRows(&queryRows[0].rowRepr)
		if err != nil {
			return nil, false, err
		}
	}
	immutable = req.to.Before(time.Now().Add(invalidateFrom))
	return &GetTableResp{
		Rows:         queryRows,
		What:         req.what,
		FromRow:      firstRowStr,
		ToRow:        lastRowStr,
		More:         hasMore,
		DebugQueries: sqlQueries,
	}, immutable, nil
}

func (h *Handler) buildSeriesResponse(s ...seriesResponse) *SeriesResponse {
	s0 := s[0]
	res := &SeriesResponse{
		Series: querySeries{
			Time:       s0.TimeSeries.Time,
			SeriesData: make([]*[]float64, 0, len(s0.Series.Data)),
			SeriesMeta: make([]QuerySeriesMetaV2, 0, len(s0.Series.Data)),
		},
		PromQL:           s0.promQL,
		MetricMeta:       s0.metric,
		DebugQueries:     s0.trace,
		ExcessPointLeft:  s0.extraPointLeft,
		ExcessPointRight: s0.extraPointRight,
	}
	for i, d := range s0.Series.Data {
		meta := QuerySeriesMetaV2{
			MaxHosts:   d.GetSMaxHosts(h),
			Total:      s0.Series.Meta.Total,
			MetricType: s0.Series.Meta.Units,
		}
		meta.What, meta.TimeShift, meta.Tags = s0.queryFuncShiftAndTagsAt(i)
		if s0.metric != nil {
			meta.Name = s0.metric.Name
		}
		if meta.Total == 0 {
			meta.Total = len(s0.Series.Data)
		}
		res.Series.SeriesMeta = append(res.Series.SeriesMeta, meta)
		res.Series.SeriesData = append(res.Series.SeriesData, d.Values)
	}
	if res.Series.SeriesData == nil {
		// frontend expects not "null" value
		res.Series.SeriesData = make([]*[]float64, 0)
	}
	// Add badges
	if s0.metric != nil && len(s) > 1 && s[1].TimeSeries != nil && len(s[1].Time) > 0 {
		s1 := s[1]
		for _, d := range s1.Series.Data {
			if t, ok := d.Tags.ID2Tag["2"]; !ok || t.SValue != s0.metric.Name {
				continue
			}
			if t, ok := d.Tags.ID2Tag["1"]; ok {
				badgeType := t.Value
				if t, ok = d.Tags.ID2Tag[promql.LabelWhat]; ok {
					what := data_model.DigestWhat(t.Value)
					switch {
					case what == data_model.DigestAvg && badgeType == format.TagValueIDBadgeAgentSamplingFactor:
						res.SamplingFactorSrc = sumSeries(d.Values, 1) / float64(len(s1.Time))
					case what == data_model.DigestAvg && badgeType == format.TagValueIDBadgeAggSamplingFactor:
						res.SamplingFactorAgg = sumSeries(d.Values, 1) / float64(len(s1.Time))
					case what == data_model.DigestCountRaw && badgeType == format.TagValueIDBadgeIngestionErrors:
						res.ReceiveErrors = sumSeries(d.Values, 0)
					case what == data_model.DigestCountRaw && badgeType == format.TagValueIDBadgeIngestionWarnings:
						res.ReceiveWarnings = sumSeries(d.Values, 0)
					case what == data_model.DigestCountRaw && badgeType == format.TagValueIDBadgeAggMappingErrors:
						res.MappingErrors = sumSeries(d.Values, 0)
					}
				}
			}
			// TODO - show badge if some heuristics on # of contributors is triggered
			// if format.IsValueCodeZero(metric) && meta.What.String() == ParamQueryFnCountNorm && badgeType == format.AddRawValuePrefix(strconv.Itoa(format.TagValueIDBadgeContributors)) {
			//	sumContributors := sumSeries(respIngestion.Series.SeriesData[i], 0)
			//	fmt.Printf("contributors sum %f\n", sumContributors)
			// }
		}
		res.DebugQueries = append(res.DebugQueries, "") // line break
		res.DebugQueries = append(res.DebugQueries, s1.trace...)
	}
	h.colorize(res)
	return res
}

func (h *Handler) parseHTTPRequest(r *http.Request) (seriesRequest, error) {
	res, err := h.parseHTTPRequestS(r, 1)
	if err != nil {
		return seriesRequest{}, err
	}
	if len(res) == 0 {
		return seriesRequest{}, httpErr(http.StatusBadRequest, fmt.Errorf("request is empty"))
	}
	return res[0], nil
}

func (h *Handler) parseHTTPRequestS(r *http.Request, maxTabs int) (res []seriesRequest, err error) {
	defer func() {
		var dummy httpError
		if err != nil && !errors.As(err, &dummy) {
			err = httpErr(http.StatusBadRequest, err)
		}
	}()
	type seriesRequestEx struct {
		seriesRequest
		strFrom       string
		strTo         string
		strWidth      string
		strWidthAgg   string
		strNumResults string
		strType       string
		width         int
		widthKind     int
	}
	var (
		dash  DashboardData
		first = func(s []string) string {
			if len(s) != 0 {
				return s[0]
			}
			return ""
		}
		env   = make(map[string]promql.Variable)
		tabs  = make([]seriesRequestEx, 0, maxTabs)
		tabX  = -1
		tabAt = func(i int) *seriesRequestEx {
			if i >= maxTabs {
				return nil
			}
			for j := len(tabs) - 1; j < i; j++ {
				tabs = append(tabs, seriesRequestEx{seriesRequest: seriesRequest{
					version: Version2,
					vars:    env,
				}})
			}
			return &tabs[i]
		}
		tab0 = tabAt(0)
	)
	// parse dashboard
	if id, err := strconv.Atoi(first(r.Form[paramDashboardID])); err == nil {
		var v *format.DashboardMeta
		if v = format.BuiltinDashboardByID[int32(id)]; v == nil {
			v = h.metricsStorage.GetDashboardMeta(int32(id))
		}
		if v != nil {
			// Ugly, but there is no other way because "metricsStorage" stores partially parsed dashboard!
			// TODO: either fully parse and validate dashboard JSON or store JSON string blindly.
			if bs, err := json.Marshal(v.JSONData); err == nil {
				easyjson.Unmarshal(bs, &dash)
			}
		}
	}
	var n int
	for i, v := range dash.Plots {
		tab := tabAt(i)
		if tab == nil {
			continue
		}
		if v.UseV2 {
			tab.version = Version2
		} else {
			tab.version = Version1
		}
		tab.numResults = v.NumSeries
		tab.metricWithNamespace = v.MetricName
		tab.customMetricName = v.CustomName
		if v.Width > 0 {
			tab.strWidth = fmt.Sprintf("%ds", v.Width)
		}
		if v.Width > 0 {
			tab.width = v.Width
		} else {
			tab.width = 1
		}
		tab.widthKind = widthLODRes
		tab.promQL = v.PromQL
		for _, v := range v.What {
			if fn, _ := ParseQueryFunc(v, &tab.maxHost); fn.What != data_model.DigestUnspecified {
				tab.what = append(tab.what, fn)
			}
		}
		tab.strType = strconv.Itoa(v.Type)
		for _, v := range v.GroupBy {
			if tid, err := parseTagID(v); err == nil {
				tab.by = append(tab.by, tid)
			}
		}
		if len(v.FilterIn) != 0 {
			tab.filterIn = make(map[string][]string)
			for k, v := range v.FilterIn {
				if tid, err := parseTagID(k); err == nil {
					tab.filterIn[tid] = v
				}
			}
		}
		if len(v.FilterNotIn) != 0 {
			tab.filterNotIn = make(map[string][]string)
			for k, v := range v.FilterNotIn {
				if tid, err := parseTagID(k); err == nil {
					tab.filterNotIn[tid] = v
				}
			}
		}
		tab.maxHost = v.MaxHost
		n++
	}
	for _, v := range dash.Vars {
		env[v.Name] = promql.Variable{
			Value:  v.Vals,
			Group:  v.Args.Group,
			Negate: v.Args.Negate,
		}
		for _, link := range v.Link {
			if len(link) != 2 {
				continue
			}
			tabX := link[0]
			if tabX < 0 || len(tabs) <= tabX {
				continue
			}
			var (
				tagX  = link[1]
				tagID string
			)
			if tagX < 0 {
				tagID = format.StringTopTagID
			} else if 0 <= tagX && tagX < format.MaxTags {
				tagID = format.TagID(tagX)
			} else {
				continue
			}
			tab := &tabs[tabX]
			if v.Args.Group {
				tab.by = append(tab.by, tagID)
			}
			if tab.filterIn != nil {
				delete(tab.filterIn, tagID)
			}
			if tab.filterNotIn != nil {
				delete(tab.filterNotIn, tagID)
			}
			if len(v.Vals) != 0 {
				if v.Args.Negate {
					if tab.filterNotIn == nil {
						tab.filterNotIn = make(map[string][]string)
					}
					tab.filterNotIn[tagID] = v.Vals
				} else {
					if tab.filterIn == nil {
						tab.filterIn = make(map[string][]string)
					}
					tab.filterIn[tagID] = v.Vals
				}
			}
		}
	}
	if n != 0 {
		switch dash.TimeRange.To {
		case "ed": // end of day
			year, month, day := time.Now().In(h.location).Date()
			tab0.to = time.Date(year, month, day, 0, 0, 0, 0, h.location).Add(24 * time.Hour).UTC()
			tab0.strTo = strconv.FormatInt(tab0.to.Unix(), 10)
		case "ew": // end of week
			var (
				year, month, day = time.Now().In(h.location).Date()
				dateNow          = time.Date(year, month, day, 0, 0, 0, 0, h.location)
				offset           = time.Duration(((time.Sunday - dateNow.Weekday() + 7) % 7) + 1)
			)
			tab0.to = dateNow.Add(offset * 24 * time.Hour).UTC()
			tab0.strTo = strconv.FormatInt(tab0.to.Unix(), 10)
		default:
			if n, err := strconv.ParseInt(dash.TimeRange.To, 10, 64); err == nil {
				if to, err := parseUnixTimeTo(n); err == nil {
					tab0.to = to
					tab0.strTo = dash.TimeRange.To
				}
			}
		}
		if from, err := parseUnixTimeFrom(dash.TimeRange.From, tab0.to); err == nil {
			tab0.from = from
			tab0.strFrom = strconv.FormatInt(dash.TimeRange.From, 10)
		}
		tab0.shifts, _ = parseTimeShifts(dash.TimeShifts)
		for i := 1; i < len(tabs); i++ {
			tabs[i].shifts = tab0.shifts
		}
	}
	// parse URL
	_ = r.ParseForm() // (*http.Request).FormValue ignores parse errors
	type (
		dashboardVar struct {
			name string
			link [][]int
		}
		dashboardVarM struct {
			val    []string
			group  string
			negate string
		}
	)
	var (
		parseTabX = func(s string) (int, error) {
			var i int
			if i, err = strconv.Atoi(s); err != nil {
				return 0, fmt.Errorf("invalid tab index %q", s)
			}
			return i, nil
		}
		vars  []dashboardVar
		varM  = make(map[string]*dashboardVarM)
		varAt = func(i int) *dashboardVar {
			for j := len(vars) - 1; j < i; j++ {
				vars = append(vars, dashboardVar{})
			}
			return &vars[i]
		}
		varByName = func(s string) (v *dashboardVarM) {
			if v = varM[s]; v == nil {
				v = &dashboardVarM{}
				varM[s] = v
			}
			return v
		}
	)
	for i, v := range dash.Vars {
		vv := varAt(i)
		vv.name = v.Name
		vv.link = append(vv.link, v.Link...)
	}
	for k, v := range r.Form {
		var i int
		if strings.HasPrefix(k, "t") {
			var dotX int
			if dotX = strings.Index(k, "."); dotX != -1 {
				var j int
				if j, err = parseTabX(k[1:dotX]); err == nil && j > 0 {
					i = j
					k = k[dotX+1:]
				}
			}
		} else if len(k) > 1 && k[0] == 'v' { // variables, not version
			var dotX int
			if dotX = strings.Index(k, "."); dotX != -1 {
				switch dotX {
				case 1: // e.g. "v.environment.g=1"
					s := strings.Split(k[dotX+1:], ".")
					switch len(s) {
					case 1:
						vv := varByName(s[0])
						vv.val = append(vv.val, v...)
					case 2:
						switch s[1] {
						case "g":
							varByName(s[0]).group = first(v)
						case "nv":
							varByName(s[0]).negate = first(v)
						}
					}
				default: // e.g. "v0.n=environment" or "v0.l=0.0-1.0"
					if varX, err := strconv.Atoi(k[1:dotX]); err == nil {
						vv := varAt(varX)
						switch k[dotX+1:] {
						case "n":
							vv.name = first(v)
						case "l":
							for _, s1 := range strings.Split(first(v), "-") {
								links := make([]int, 0, 2)
								for _, s2 := range strings.Split(s1, ".") {
									if n, err := strconv.Atoi(s2); err == nil {
										links = append(links, n)
									} else {
										break
									}
								}
								if len(links) == 2 {
									vv.link = append(vv.link, links)
								}
							}
						}
					}
				}
			}
			continue
		}
		t := tabAt(i)
		if t == nil {
			continue
		}
		switch k {
		case paramTabNumber:
			tabX, err = parseTabX(first(v))
		case ParamAvoidCache:
			t.avoidCache = true
		case ParamFromTime:
			t.strFrom = first(v)
		case ParamMetric:
			const formerBuiltin = "__builtin_" // we renamed builtin metrics, removing prefix
			name := first(v)
			if strings.HasPrefix(name, formerBuiltin) {
				name = "__" + name[len(formerBuiltin):]
			}
			ns := r.FormValue(ParamNamespace)
			t.metricWithNamespace = mergeMetricNamespace(ns, name)
		case ParamNumResults:
			t.strNumResults = first(v)
		case ParamQueryBy:
			for _, s := range v {
				var tid string
				tid, err = parseTagID(s)
				if err != nil {
					return nil, err
				}
				t.by = append(t.by, tid)
			}
		case ParamQueryFilter:
			t.filterIn, t.filterNotIn, err = parseQueryFilter(v)
		case ParamQueryVerbose:
			t.verbose = first(v) == "1"
		case ParamQueryWhat:
			for _, what := range v {
				if fn, _ := ParseQueryFunc(what, &t.maxHost); fn.What != data_model.DigestUnspecified {
					t.what = append(t.what, fn)
				}
			}
		case ParamTimeShift:
			t.shifts, err = parseTimeShifts(v)
		case ParamToTime:
			t.strTo = first(v)
		case ParamVersion:
			s := first(v)
			switch s {
			case Version1, Version2:
				t.version = s
			default:
				return nil, fmt.Errorf("invalid version: %q", s)
			}
		case ParamWidth:
			t.strWidth = first(v)
		case ParamWidthAgg:
			t.strWidthAgg = first(v)
		case paramMaxHost:
			t.maxHost = true
		case paramPromQuery:
			t.promQL = first(v)
		case paramDataFormat:
			t.format = first(v)
		case paramQueryType:
			t.strType = first(v)
		case paramExcessPoints:
			t.excessPoints = true
		case paramFromEnd:
			t.fromEnd = true
		case paramFromRow:
			t.fromRow, err = parseFromRows(first(v))
		case paramToRow:
			t.toRow, err = parseFromRows(first(v))
		case paramYL:
			t.yl = first(v)
		case paramYH:
			t.yh = first(v)
		}
		if err != nil {
			return nil, err
		}
	}
	if len(tabs) == 0 {
		return nil, nil
	}
	for _, v := range vars {
		vv := varM[v.name]
		if vv == nil {
			continue
		}
		env[v.name] = promql.Variable{
			Value:  vv.val,
			Group:  vv.group == "1",
			Negate: vv.negate == "1",
		}
		for _, link := range v.link {
			if len(link) != 2 {
				continue
			}
			tabX := link[0]
			if tabX < 0 || len(tabs) <= tabX {
				continue
			}
			var (
				tagX  = link[1]
				tagID string
			)
			if tagX < 0 {
				tagID = format.StringTopTagID
			} else if 0 <= tagX && tagX < format.MaxTags {
				tagID = format.TagID(tagX)
			} else {
				continue
			}
			tab := &tabs[tabX]
			if vv.group == "1" {
				tab.by = append(tab.by, tagID)
			}
			if tab.filterIn != nil {
				delete(tab.filterIn, tagID)
			}
			if tab.filterNotIn != nil {
				delete(tab.filterNotIn, tagID)
			}
			if len(vv.val) != 0 {
				if vv.negate == "1" {
					if tab.filterNotIn == nil {
						tab.filterNotIn = make(map[string][]string)
					}
					tab.filterNotIn[tagID] = vv.val
				} else {
					if tab.filterIn == nil {
						tab.filterIn = make(map[string][]string)
					}
					tab.filterIn[tagID] = vv.val
				}
			}
		}
	}
	// parse dependent paramemeters
	var (
		finalize = func(t *seriesRequestEx) error {
			numResultsMax := maxSeries
			if len(t.shifts) != 0 {
				numResultsMax /= len(t.shifts)
			}
			if t.strNumResults != "" {
				if t.numResults, err = parseNumResults(t.strNumResults, numResultsMax); err != nil {
					return err
				}
				if t.numResults == 0 {
					t.numResults = math.MaxInt
				}
			}
			if len(t.strWidth) != 0 || len(t.strWidthAgg) != 0 {
				t.width, t.widthKind, err = parseWidth(t.strWidth, t.strWidthAgg)
				if err != nil {
					return err
				}
			}
			if t.widthKind == widthAutoRes {
				t.screenWidth = int64(t.width)
			} else {
				t.step = int64(t.width)
			}
			return nil
		}
	)
	if len(tab0.strFrom) != 0 || len(tab0.strTo) != 0 {
		tab0.from, tab0.to, err = parseFromTo(tab0.strFrom, tab0.strTo)
		if err != nil {
			return nil, err
		}
	}
	err = finalize(tab0)
	if err != nil {
		return nil, err
	}
	for i := range tabs[1:] {
		t := &tabs[i+1]
		t.from = tab0.from
		t.to = tab0.to
		err = finalize(t)
		if err != nil {
			return nil, err
		}
	}
	// build resulting slice
	if tabX != -1 {
		if tabs[tabX].strType == "1" {
			return nil, nil
		}
		return []seriesRequest{tabs[tabX].seriesRequest}, nil
	}
	res = make([]seriesRequest, 0, len(tabs))
	for _, t := range tabs {
		if t.strType == "1" {
			continue
		}
		if len(t.metricWithNamespace) != 0 || len(t.promQL) != 0 {
			res = append(res, t.seriesRequest)
		}
	}
	return res, nil
}
