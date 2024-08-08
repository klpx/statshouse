// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/mailru/easyjson"

	"github.com/vkcom/statshouse/internal/aggregator"
	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/format"
	"github.com/vkcom/statshouse/internal/metajournal"
	"github.com/vkcom/statshouse/internal/util"
)

func (h *Handler) HandlePostMetric(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStatHTTP(EndpointMetric, r.Method, h.getMetricIDForStat(r.FormValue(ParamMetric)), "", r.FormValue(paramPriority))
	if h.checkReadOnlyMode(w, r) {
		return
	}
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	rd := &io.LimitedReader{
		R: r.Body,
		N: maxEntityHTTPBodySize,
	}
	defer func() { _ = r.Body.Close() }()
	res, err := io.ReadAll(rd)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	if len(res) >= maxEntityHTTPBodySize {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("metric body too big. Max size is %d bytes", maxEntityHTTPBodySize)), h.verbose, ai.user, sl)
		return
	}
	var metric MetricInfo
	if err := easyjson.Unmarshal(res, &metric); err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	m, err := h.handlePostMetric(r.Context(), ai, formValueParamMetric(r), metric.Metric)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	err = h.waitVersionUpdate(r.Context(), m.Version)
	respondJSON(w, &MetricInfo{Metric: m}, defaultCacheTTL, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandlePutPostGroup(w http.ResponseWriter, r *http.Request) {
	var groupInfo MetricsGroupInfo
	handlePostEntity(h, w, r, EndpointGroup, &groupInfo, func(ctx context.Context, ai accessInfo, entity *MetricsGroupInfo, create bool) (resp interface{}, versionToWait int64, err error) {
		response, err := h.handlePostGroup(ctx, ai, entity.Group, create)
		if err != nil {
			return nil, 0, err
		}
		return response, response.Group.Version, nil
	})
}

func (h *Handler) HandlePostNamespace(w http.ResponseWriter, r *http.Request) {
	var namespaceInfo NamespaceInfo
	handlePostEntity(h, w, r, EndpointNamespace, &namespaceInfo, func(ctx context.Context, ai accessInfo, entity *NamespaceInfo, create bool) (resp interface{}, versionToWait int64, err error) {
		response, err := h.handlePostNamespace(ctx, ai, entity.Namespace, create)
		if err != nil {
			return nil, 0, err
		}
		return response, response.Namespace.Version, nil
	})
}

func (h *Handler) HandlePostResetFlood(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStatHTTP(EndpointResetFlood, r.Method, 0, "", r.FormValue(paramPriority))
	if h.checkReadOnlyMode(w, r) {
		return
	}
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	if !ai.isAdmin() {
		err := httpErr(http.StatusForbidden, fmt.Errorf("admin access required"))
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	var limit int32
	if v := r.FormValue("limit"); v != "" {
		i, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, err), h.verbose, ai.user, sl)
			return
		}
		limit = int32(i)
	}
	del, before, after, err := h.metadataLoader.ResetFlood(r.Context(), formValueParamMetric(r), limit)
	if err == nil && !del {
		err = fmt.Errorf("metric flood counter was empty (no flood)")
	}
	respondJSON(w, &struct {
		Before int32 `json:"before"`
		After  int32 `json:"after"`
	}{Before: before, After: after}, 0, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandlePostKnownTags(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStatHTTP(EndpointPrometheus, r.Method, 0, "", r.FormValue(paramPriority))
	if h.checkReadOnlyMode(w, r) {
		return
	}
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	if !ai.isAdmin() {
		err = httpErr(http.StatusNotFound, fmt.Errorf("not found"))
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	rd := &io.LimitedReader{
		R: r.Body,
		N: maxPromConfigHTTPBodySize,
	}
	defer func() { _ = r.Body.Close() }()
	res, err := io.ReadAll(rd)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	if len(res) >= maxPromConfigHTTPBodySize {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("config body too big. Max size is %d bytes", maxPromConfigHTTPBodySize)), h.verbose, ai.user, sl)
		return
	}
	_, err = aggregator.ParseKnownTags(res, h.metricsStorage)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	event, err := h.metadataLoader.SaveKnownTagsConfig(r.Context(), h.metricsStorage.KnownTags().Version, string(res))
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	err = h.waitVersionUpdate(r.Context(), event.Version)
	respondJSON(w, struct {
		Version int64 `json:"version"`
	}{event.Version}, defaultCacheTTL, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandleGetKnownTags(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStatHTTP(EndpointPrometheus, r.Method, 0, "", r.FormValue(paramPriority))
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	if !ai.isAdmin() {
		err = httpErr(http.StatusNotFound, fmt.Errorf("config is not found"))
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	var res map[string]aggregator.KnownTagsJSON
	err = json.Unmarshal([]byte(h.metricsStorage.KnownTags().Data), &res)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	respondJSON(w, res, 0, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandleGetHistory(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStatHTTP(EndpointHistory, r.Method, 0, "", r.FormValue(paramPriority))
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	var id int64
	if idStr := r.FormValue(ParamID); idStr != "" {
		id, err = strconv.ParseInt(idStr, 10, 32)
		if err != nil {
			respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, err), h.verbose, ai.user, sl)
			return
		}
	} else {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("%s is must be set", ParamID)), h.verbose, ai.user, sl)
		return
	}
	hist, err := h.metadataLoader.GetShortHistory(r.Context(), id)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	resp := GetHistoryShortInfoResp{Events: make([]HistoryEvent, 0, len(hist.Events))}
	for _, h := range hist.Events {
		m := metadata{}
		_ = json.Unmarshal([]byte(h.Metadata), &m)
		resp.Events = append(resp.Events, HistoryEvent{
			Version:  h.Version,
			Metadata: m,
		})
	}
	respondJSON(w, resp, defaultCacheTTL, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandleGetMetricTagValues(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStatHTTP(EndpointMetricTagValues, r.Method, h.getMetricIDForStat(r.FormValue(ParamMetric)), "", r.FormValue(paramPriority))
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.querySelectTimeout)
	defer cancel()

	_ = r.ParseForm() // (*http.Request).FormValue ignores parse errors, too
	resp, immutable, err := h.handleGetMetricTagValues(
		ctx,
		getMetricTagValuesReq{
			ai:                  ai,
			version:             r.FormValue(ParamVersion),
			numResults:          r.FormValue(ParamNumResults),
			metricWithNamespace: formValueParamMetric(r),
			tagID:               r.FormValue(ParamTagID),
			from:                r.FormValue(ParamFromTime),
			to:                  r.FormValue(ParamToTime),
			what:                r.FormValue(ParamQueryWhat),
			filter:              r.Form[ParamQueryFilter],
		})

	cache, cacheStale := queryClientCacheDuration(immutable)
	respondJSON(w, resp, cache, cacheStale, err, h.verbose, ai.user, sl)
}

func HandleGetEntity[T any](w http.ResponseWriter, r *http.Request, h *Handler, endpointName string, handle func(ctx context.Context, ai accessInfo, id int32, version int64) (T, time.Duration, error)) {
	sl := newEndpointStatHTTP(endpointName, r.Method, 0, "", r.FormValue(paramPriority))
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	idStr := r.FormValue(ParamID)
	id, err := strconv.ParseInt(idStr, 10, 32)
	if err != nil {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, err), h.verbose, ai.user, sl)
		return
	}
	verStr := r.FormValue(ParamEntityVersion)
	var ver int64
	if verStr != "" {
		ver, err = strconv.ParseInt(verStr, 10, 64)
		if err != nil {
			respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, err), h.verbose, ai.user, sl)
			return
		}
	}
	resp, cache, err := handle(r.Context(), ai, int32(id), ver)
	respondJSON(w, resp, cache, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandleGetDashboard(w http.ResponseWriter, r *http.Request) {
	HandleGetEntity(w, r, h, EndpointDashboard, h.handleGetDashboard)
}

func (h *Handler) HandleGetGroup(w http.ResponseWriter, r *http.Request) {
	HandleGetEntity(w, r, h, EndpointGroup, func(ctx context.Context, ai accessInfo, id int32, _ int64) (*MetricsGroupInfo, time.Duration, error) {
		return h.handleGetGroup(ai, id)
	})
}

func (h *Handler) HandleGetNamespace(w http.ResponseWriter, r *http.Request) {
	HandleGetEntity(w, r, h, EndpointNamespace, func(_ context.Context, ai accessInfo, id int32, _ int64) (*NamespaceInfo, time.Duration, error) {
		return h.handleGetNamespace(ai, id)
	})
}

func HandleGetEntityList[T any](w http.ResponseWriter, r *http.Request, h *Handler, endpointName string, handle func(ai accessInfo, showInvisible bool) (T, time.Duration, error)) {
	sl := newEndpointStatHTTP(endpointName, r.Method, 0, "", r.FormValue(paramPriority))
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	sd := r.URL.Query().Has(paramShowDisabled)
	resp, cache, err := handle(ai, sd)
	respondJSON(w, resp, cache, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandleGetGroupsList(w http.ResponseWriter, r *http.Request) {
	HandleGetEntityList(w, r, h, EndpointGroup, h.handleGetGroupsList)
}

func (h *Handler) HandleGetDashboardList(w http.ResponseWriter, r *http.Request) {
	HandleGetEntityList(w, r, h, EndpointDashboard, h.handleGetDashboardList)
}

func (h *Handler) HandleGetNamespaceList(w http.ResponseWriter, r *http.Request) {
	HandleGetEntityList(w, r, h, EndpointNamespace, h.handleGetNamespaceList)
}

func (h *Handler) HandlePutPostDashboard(w http.ResponseWriter, r *http.Request) {
	var dashboard DashboardInfo
	handlePostEntity(h, w, r, EndpointDashboard, &dashboard, func(ctx context.Context, ai accessInfo, entity *DashboardInfo, create bool) (resp interface{}, versionToWait int64, err error) {
		response, err := h.handlePostDashboard(ctx, ai, entity.Dashboard, create, entity.Delete)
		if err != nil {
			return nil, 0, err
		}
		return response, response.Dashboard.Version, nil
	})
}

func (h *Handler) HandleGetPromConfig(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStatHTTP(EndpointPrometheus, r.Method, 0, "", r.FormValue(paramPriority))
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	if !ai.isAdmin() {
		err = httpErr(http.StatusNotFound, fmt.Errorf("config is not found"))
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	s, err := aggregator.DeserializeScrapeConfig([]byte(h.metricsStorage.PromConfig().Data), h.metricsStorage)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	respondJSON(w, s, 0, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandleGetPromConfigGenerated(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStatHTTP(EndpointPrometheus, r.Method, 0, "", r.FormValue(paramPriority))
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	if !ai.isAdmin() {
		err = httpErr(http.StatusNotFound, fmt.Errorf("config is not found"))
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	s, err := aggregator.DeserializeScrapeConfig([]byte(h.metricsStorage.PromConfigGenerated().Data), nil)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	respondJSON(w, s, 0, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) HandlePostPromConfig(w http.ResponseWriter, r *http.Request) {
	sl := newEndpointStatHTTP(EndpointPrometheus, r.Method, 0, "", r.FormValue(paramPriority))
	if h.checkReadOnlyMode(w, r) {
		return
	}
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	if !ai.isAdmin() {
		err = httpErr(http.StatusNotFound, fmt.Errorf("config is not found"))
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	rd := &io.LimitedReader{
		R: r.Body,
		N: maxPromConfigHTTPBodySize,
	}
	defer func() { _ = r.Body.Close() }()
	res, err := io.ReadAll(rd)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	if len(res) >= maxPromConfigHTTPBodySize {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("confog body too big. Max size is %d bytes", maxPromConfigHTTPBodySize)), h.verbose, ai.user, sl)
		return
	}
	_, err = aggregator.DeserializeScrapeConfig(res, h.metricsStorage)
	if err != nil {
		err = httpErr(http.StatusBadRequest, fmt.Errorf("invalid prometheus config syntax: %v", err))
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	event, err := h.metadataLoader.SaveScrapeConfig(r.Context(), h.metricsStorage.PromConfig().Version, string(res), ai.toMetadata())
	if err != nil {
		err = fmt.Errorf("failed to save prometheus config: %w", err)
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	err = h.waitVersionUpdate(r.Context(), event.Version)
	respondJSON(w, struct {
		Version int64 `json:"version"`
	}{event.Version}, defaultCacheTTL, 0, err, h.verbose, ai.user, sl)
}

// TODO - remove metric name from request
func (h *Handler) handlePostMetric(ctx context.Context, ai accessInfo, _ string, metric format.MetricMetaValue) (format.MetricMetaValue, error) {
	create := metric.MetricID == 0
	var resp format.MetricMetaValue
	var err error
	if metric.PreKeyOnly && (metric.PreKeyFrom == 0 || metric.PreKeyTagID == "") {
		return format.MetricMetaValue{}, httpErr(http.StatusBadRequest, fmt.Errorf("use prekey_only with non empty prekey_tag_id"))
	}
	if create {
		if !ai.CanEditMetric(true, metric, metric) {
			return format.MetricMetaValue{}, httpErr(http.StatusForbidden, fmt.Errorf("can't create metric %q", metric.Name))
		}
		resp, err = h.metadataLoader.SaveMetric(ctx, metric, ai.toMetadata())
		if err != nil {
			err = fmt.Errorf("error creating metric in sqlite engine: %w", err)
			log.Println(err.Error())
			return format.MetricMetaValue{}, fmt.Errorf("failed to create metric: %w", err)
		}
	} else {
		if _, ok := format.BuiltinMetrics[metric.MetricID]; ok {
			return format.MetricMetaValue{}, httpErr(http.StatusBadRequest, fmt.Errorf("builtin metric cannot be edited"))
		}
		old := h.metricsStorage.GetMetaMetric(metric.MetricID)
		if old == nil {
			return format.MetricMetaValue{}, httpErr(http.StatusNotFound, fmt.Errorf("metric %q not found (id %d)", metric.Name, metric.MetricID))
		}
		if !ai.CanEditMetric(false, *old, metric) {
			return format.MetricMetaValue{}, httpErr(http.StatusForbidden, fmt.Errorf("can't edit metric %q", old.Name))
		}
		resp, err = h.metadataLoader.SaveMetric(ctx, metric, ai.toMetadata())
		if err != nil {
			err = fmt.Errorf("error saving metric in sqllite: %w", err)
			log.Println(err.Error())
			return format.MetricMetaValue{}, fmt.Errorf("can't edit metric: %w", err)
		}
	}
	return resp, nil
}

func (h *Handler) handlePostGroup(ctx context.Context, ai accessInfo, group format.MetricsGroup, create bool) (*MetricsGroupInfo, error) {
	if !ai.isAdmin() {
		return nil, httpErr(http.StatusNotFound, fmt.Errorf("group %s not found", group.Name))
	}
	if !create {
		if h.metricsStorage.GetGroup(group.ID) == nil {
			return &MetricsGroupInfo{}, httpErr(http.StatusNotFound, fmt.Errorf("group %d not found", group.ID))
		}
	}
	if !h.metricsStorage.CanAddOrChangeGroup(group.Name, group.ID) {
		return &MetricsGroupInfo{}, httpErr(http.StatusBadRequest, fmt.Errorf("group name %s is not posible", group.Name))
	}
	var err error
	if group.ID >= 0 {
		group, err = h.metadataLoader.SaveMetricsGroup(ctx, group, create, ai.toMetadata())
	} else {
		group, err = h.metadataLoader.SaveBuiltInGroup(ctx, group)
	}
	if err != nil {
		s := "edit"
		if create {
			s = "create"
		}
		errReturn := fmt.Errorf("can't %s group: %w", s, err)
		if metajournal.IsUserRequestError(err) {
			return &MetricsGroupInfo{}, httpErr(http.StatusBadRequest, errReturn)
		}
		return &MetricsGroupInfo{}, errReturn
	}
	err = h.waitVersionUpdate(ctx, group.Version)
	if err != nil {
		return &MetricsGroupInfo{}, err
	}
	info, _ := h.metricsStorage.GetGroupWithMetricsList(group.ID)
	return &MetricsGroupInfo{Group: group, Metrics: info.Metrics}, nil
}

func (h *Handler) handlePostNamespace(ctx context.Context, ai accessInfo, namespace format.NamespaceMeta, create bool) (*NamespaceInfo, error) {
	if !ai.isAdmin() {
		return nil, httpErr(http.StatusNotFound, fmt.Errorf("namespace %s not found", namespace.Name))
	}
	if !create {
		if h.metricsStorage.GetNamespace(namespace.ID) == nil {
			return &NamespaceInfo{}, httpErr(http.StatusNotFound, fmt.Errorf("namespace %d not found", namespace.ID))
		}
	}
	var err error
	if namespace.ID >= 0 {
		namespace, err = h.metadataLoader.SaveNamespace(ctx, namespace, create, ai.toMetadata())
	} else {
		n := h.metricsStorage.GetNamespace(namespace.ID)
		if n == nil {
			return &NamespaceInfo{}, httpErr(http.StatusNotFound, fmt.Errorf("namespace %d not found", namespace.ID))
		}
		create := n == format.BuiltInNamespaceDefault[namespace.ID]
		namespace, err = h.metadataLoader.SaveBuiltinNamespace(ctx, namespace, create)
	}

	if err != nil {
		s := "edit"
		if create {
			s = "create"
		}
		errReturn := fmt.Errorf("can't %s namespace: %w", s, err)
		if metajournal.IsUserRequestError(err) {
			return &NamespaceInfo{}, httpErr(http.StatusBadRequest, errReturn)
		}
		return &NamespaceInfo{}, errReturn
	}
	return &NamespaceInfo{Namespace: namespace}, nil
}

func (h *Handler) handleGetMetricTagValues(ctx context.Context, req getMetricTagValuesReq) (resp *GetMetricTagValuesResp, immutable bool, err error) {
	version, err := parseVersion(req.version)
	if err != nil {
		return nil, false, err
	}

	var numResults int
	if req.numResults == "" || req.numResults == "0" {
		numResults = defTagValues
	} else if numResults, err = parseNumResults(req.numResults, maxTagValues); err != nil {
		return nil, false, err
	}

	metricMeta, err := h.getMetricMeta(req.ai, req.metricWithNamespace)
	if err != nil {
		return nil, false, err
	}

	err = validateQuery(metricMeta, version)
	if err != nil {
		return nil, false, err
	}

	tagID, err := parseTagID(req.tagID)
	if err != nil {
		return nil, false, err
	}

	from, to, err := parseFromTo(req.from, req.to)
	if err != nil {
		return nil, false, err
	}

	filterIn, filterNotIn, err := parseQueryFilter(req.filter)
	if err != nil {
		return nil, false, err
	}
	mappedFilterIn, err := h.resolveFilter(metricMeta, version, filterIn)
	if err != nil {
		return nil, false, err
	}
	mappedFilterNotIn, err := h.resolveFilter(metricMeta, version, filterNotIn)
	if err != nil {
		return nil, false, err
	}

	lods, err := data_model.GetLODs(data_model.GetTimescaleArgs{
		Version:     req.version,
		Start:       from.Unix(),
		End:         to.Unix(),
		ScreenWidth: 100, // really dumb
		TimeNow:     time.Now().Unix(),
		Metric:      metricMeta,
		Location:    h.location,
		UTCOffset:   h.utcOffset,
	})
	if err != nil {
		return nil, false, err
	}

	pq := &preparedTagValuesQuery{
		version:     version,
		metricID:    metricMeta.MetricID,
		preKeyTagID: metricMeta.PreKeyTagID,
		tagID:       tagID,
		numResults:  numResults,
		filterIn:    mappedFilterIn,
		filterNotIn: mappedFilterNotIn,
	}

	tagInfo := map[selectRow]float64{}
	if version == Version1 && tagID == format.EnvTagID {
		tagInfo[selectRow{valID: format.TagValueIDProductionLegacy}] = 100 // we only support production tables for v1
	} else {
		for _, lod := range lods {
			query, args, err := tagValuesQuery(pq, lod) // we set limit to numResult+1
			if err != nil {
				return nil, false, err
			}
			cols := newTagValuesSelectCols(args)
			isFast := lod.FromSec+fastQueryTimeInterval >= lod.ToSec
			err = h.doSelect(ctx, util.QueryMetaInto{
				IsFast:  isFast,
				IsLight: true,
				User:    req.ai.user,
				Metric:  metricMeta.MetricID,
				Table:   lod.Table,
				Kind:    "get_mapping",
			}, version, ch.Query{
				Body:   query,
				Result: cols.res,
				OnResult: func(_ context.Context, b proto.Block) error {
					for i := 0; i < b.Rows; i++ {
						tag := cols.rowAt(i)
						tagInfo[selectRow{valID: tag.valID, val: tag.val}] += tag.cnt
					}
					return nil
				}})
			if err != nil {
				return nil, false, err
			}
		}
	}

	data := make([]selectRow, 0, len(tagInfo))
	for k, count := range tagInfo {
		data = append(data, selectRow{valID: k.valID, val: k.val, cnt: count})
	}
	sort.Slice(data, func(i int, j int) bool { return data[i].cnt > data[j].cnt })

	ret := &GetMetricTagValuesResp{
		TagValues: []MetricTagValueInfo{},
	}
	if len(data) > numResults {
		data = data[:numResults]
		ret.TagValuesMore = true
	}
	for _, d := range data {
		v := d.val
		if pq.stringTag() {
			v = emptyToUnspecified(v)
		} else {
			v = h.getRichTagValue(metricMeta, version, tagID, d.valID)
		}
		ret.TagValues = append(ret.TagValues, MetricTagValueInfo{
			Value: v,
			Count: d.cnt,
		})
	}

	immutable = to.Before(time.Now().Add(invalidateFrom))
	return ret, immutable, nil
}

func (h *Handler) handleGetDashboard(ctx context.Context, ai accessInfo, id int32, version int64) (*DashboardInfo, time.Duration, error) {
	if id < 0 {
		if dash, ok := format.BuiltinDashboardByID[id]; ok {
			return &DashboardInfo{Dashboard: getDashboardMetaInfo(dash)}, defaultCacheTTL, nil
		} else {
			return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("dashboard %d not found", id))
		}
	}
	var dash *format.DashboardMeta
	if version == 0 {
		dash = h.metricsStorage.GetDashboardMeta(id)
		if dash == nil {
			return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("dashboard %d not found", id))
		}
	} else {
		dashI, err := h.metadataLoader.GetDashboard(ctx, int64(id), version)
		if err != nil {
			return nil, 0, err
		}
		dash = &dashI
	}
	return &DashboardInfo{Dashboard: getDashboardMetaInfo(dash)}, defaultCacheTTL, nil
}

func (h *Handler) handleGetDashboardList(ai accessInfo, showInvisible bool) (*GetDashboardListResp, time.Duration, error) {
	dashs := h.metricsStorage.GetDashboardList()
	for _, meta := range format.BuiltinDashboardByID {
		dashs = append(dashs, meta)
	}
	resp := &GetDashboardListResp{}
	for _, dash := range dashs {
		description := ""
		descriptionI := dash.JSONData[descriptionFieldName]
		if descriptionI != nil {
			description, _ = descriptionI.(string)
		}
		resp.Dashboards = append(resp.Dashboards, dashboardShortInfo{
			Id:          dash.DashboardID,
			Name:        dash.Name,
			Description: description,
		})
	}
	return resp, defaultCacheTTL, nil
}

func (h *Handler) handlePostDashboard(ctx context.Context, ai accessInfo, dash DashboardMetaInfo, create, delete bool) (*DashboardInfo, error) {
	if !create {
		if _, ok := format.BuiltinDashboardByID[dash.DashboardID]; ok {
			return &DashboardInfo{}, httpErr(http.StatusBadRequest, fmt.Errorf("can't edit builtin dashboard %d", dash.DashboardID))
		}
		if h.metricsStorage.GetDashboardMeta(dash.DashboardID) == nil {
			return &DashboardInfo{}, httpErr(http.StatusNotFound, fmt.Errorf("dashboard %d not found", dash.DashboardID))
		}
	}
	if dash.JSONData == nil {
		dash.JSONData = map[string]interface{}{}
	}
	dash.JSONData[descriptionFieldName] = dash.Description
	dashboard, err := h.metadataLoader.SaveDashboard(ctx, format.DashboardMeta{
		DashboardID: dash.DashboardID,
		Name:        dash.Name,
		Version:     dash.Version,
		UpdateTime:  dash.UpdateTime,
		DeleteTime:  dash.DeletedTime,
		JSONData:    dash.JSONData,
	}, create, delete, ai.toMetadata())
	if err != nil {
		s := "edit"
		if create {
			s = "create"
		}
		if metajournal.IsUserRequestError(err) {
			return &DashboardInfo{}, httpErr(http.StatusBadRequest, fmt.Errorf("can't %s dashboard: %w", s, err))
		}
		return &DashboardInfo{}, fmt.Errorf("can't %s dashboard: %w", s, err)
	}
	return &DashboardInfo{Dashboard: getDashboardMetaInfo(&dashboard)}, nil
}

func (h *Handler) handleGetGroup(_ accessInfo, id int32) (*MetricsGroupInfo, time.Duration, error) {
	group, ok := h.metricsStorage.GetGroupWithMetricsList(id)
	if !ok {
		return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("group %d not found", id))
	}
	return &MetricsGroupInfo{Group: *group.Group, Metrics: group.Metrics}, defaultCacheTTL, nil
}

func (h *Handler) handleGetGroupsList(ai accessInfo, showInvisible bool) (*GetGroupListResp, time.Duration, error) {
	groups := h.metricsStorage.GetGroupsList(showInvisible)
	resp := &GetGroupListResp{}
	for _, group := range groups {
		resp.Groups = append(resp.Groups, groupShortInfo{
			Id:      group.ID,
			Name:    group.Name,
			Weight:  group.Weight,
			Disable: group.Disable,
		})
	}
	return resp, defaultCacheTTL, nil
}

func (h *Handler) handleGetNamespace(_ accessInfo, id int32) (*NamespaceInfo, time.Duration, error) {
	namespace := h.metricsStorage.GetNamespace(id)
	if namespace == nil {
		return nil, 0, httpErr(http.StatusNotFound, fmt.Errorf("namespace %d not found", id))
	}
	return &NamespaceInfo{Namespace: *namespace}, defaultCacheTTL, nil
}

func (h *Handler) handleGetNamespaceList(ai accessInfo, showInvisible bool) (*GetNamespaceListResp, time.Duration, error) {
	namespaces := h.metricsStorage.GetNamespaceList()
	var namespacesResp []namespaceShortInfo
	for _, namespace := range namespaces {
		namespacesResp = append(namespacesResp, namespaceShortInfo{
			Id:     namespace.ID,
			Name:   namespace.Name,
			Weight: namespace.Weight,
		})
	}
	return &GetNamespaceListResp{Namespaces: namespacesResp}, defaultCacheTTL, nil
}

func handlePostEntity[T easyjson.Unmarshaler](h *Handler, w http.ResponseWriter, r *http.Request, endpoint string, entity T, handleCallback func(ctx context.Context, ai accessInfo, entity T, create bool) (resp interface{}, versionToWait int64, err error)) {
	sl := newEndpointStatHTTP(endpoint, r.Method, 0, "", r.FormValue(paramPriority))
	if h.checkReadOnlyMode(w, r) {
		return
	}
	ai, err := h.parseAccessToken(r, sl)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	rd := &io.LimitedReader{
		R: r.Body,
		N: maxEntityHTTPBodySize,
	}
	defer func() { _ = r.Body.Close() }()
	res, err := io.ReadAll(rd)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	if len(res) >= maxEntityHTTPBodySize {
		respondJSON(w, nil, 0, 0, httpErr(http.StatusBadRequest, fmt.Errorf("entity body too big. Max size is %d bytes", maxEntityHTTPBodySize)), h.verbose, ai.user, sl)
		return
	}
	if err := easyjson.Unmarshal(res, entity); err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	d, version, err := handleCallback(r.Context(), ai, entity, r.Method == http.MethodPut)
	if err != nil {
		respondJSON(w, nil, 0, 0, err, h.verbose, ai.user, sl)
		return
	}
	err = h.waitVersionUpdate(r.Context(), version)
	respondJSON(w, d, defaultCacheTTL, 0, err, h.verbose, ai.user, sl)
}

func (h *Handler) waitVersionUpdate(ctx context.Context, version int64) error {
	ctx, cancel := context.WithTimeout(ctx, journalUpdateTimeout)
	defer cancel()
	return h.metricsStorage.Journal().WaitVersion(ctx, version)
}
