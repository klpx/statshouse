package data_model

import (
	"cmp"
	"fmt"
	"sort"
	"strconv"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/vkcom/statshouse/internal/format"
)

type QueryFilter struct {
	FilterIn    TagFilters
	FilterNotIn TagFilters
	Resolution  int
}

type TagFilters struct {
	Metrics   MetricList
	StringTop []string
	Tags      [format.NewMaxTags]TagValues
}

type MetricList struct {
	Head *format.MetricMetaValue
	Tail []*format.MetricMetaValue
}

type TagValues []TagValue

type TagValue struct {
	Value  string
	Mapped int32
}

func MetricIDFilter(metricID int32) TagFilters {
	return MetricFilter(&format.MetricMetaValue{MetricID: metricID})
}

func MetricFilter(metric *format.MetricMetaValue) TagFilters {
	return TagFilters{Metrics: MetricList{Head: metric}}
}

func (f *QueryFilter) APICompatGetTag(tagNameOrID string) (format.MetricMetaTag, bool) {
	if f.FilterIn.Metrics.Count() == 1 {
		tag, ok, _ := f.FilterIn.Metrics.Head.APICompatGetTag(tagNameOrID)
		return tag, ok
	}
	if tagID, err := format.APICompatNormalizeTagID(tagNameOrID); err == nil {
		i := format.TagIndex(tagID)
		if 0 <= i && i < format.NewMaxTags {
			return format.MetricMetaTag{Index: i}, true
		}
	}
	return format.MetricMetaTag{}, false
}

func (f *QueryFilter) Add(metric *format.MetricMetaValue, matcher *labels.Matcher) {
	switch matcher.Type {
	case labels.MatchEqual, labels.MatchRegexp:
		if f.FilterIn.Metrics.Head != nil {
			f.FilterIn.Metrics.Tail = append(f.FilterIn.Metrics.Tail, metric)
		} else {
			f.FilterIn.Metrics.Head = metric
		}
	case labels.MatchNotEqual, labels.MatchNotRegexp:
		if f.FilterNotIn.Metrics.Head != nil {
			f.FilterNotIn.Metrics.Tail = append(f.FilterNotIn.Metrics.Tail, metric)
		} else {
			f.FilterNotIn.Metrics.Head = metric
		}
	default:
		panic(fmt.Sprintf("unknown label matcher type %v", matcher.Type))
	}
	if f.Resolution < metric.Resolution {
		f.Resolution = metric.Resolution
	}
}

func (f *QueryFilter) MatchingMetric() *format.MetricMetaValue {
	if f.FilterIn.Metrics.Count() == 1 {
		return f.FilterIn.Metrics.Head
	}
	return nil
}

func (f *QueryFilter) MatchingMetricCount() int {
	return f.FilterIn.Metrics.Count()
}

func (f *QueryFilter) MatchingMetrics() []*format.MetricMetaValue {
	if f.FilterIn.Metrics.Head == nil {
		return nil
	}
	res := make([]*format.MetricMetaValue, 0, 1+len(f.FilterIn.Metrics.Tail))
	res = append(res, f.FilterIn.Metrics.Head)
	res = append(res, f.FilterIn.Metrics.Tail...)
	return res
}

func (f *TagFilters) AppendValue(tag int, val ...string) {
	s := make(TagValues, len(val))
	for i := 0; i < len(val); i++ {
		s[i].Value = val[i]
	}
	f.Append(tag, s...)
}

func (f *TagFilters) AppendMapped(tag int, val ...int32) {
	s := make(TagValues, len(val))
	for i := 0; i < len(val); i++ {
		s[i].Mapped = val[i]
	}
	f.Append(tag, s...)
}

func (f *TagFilters) Append(tag int, filter ...TagValue) {
	f.Tags[tag] = append(f.Tags[tag], filter...)
}

func (f *TagFilters) Contains(tag int) bool {
	return 0 <= tag && tag < len(f.Tags) && len(f.Tags[tag]) != 0
}

func (m MetricList) Empty() bool {
	return m.Head == nil
}

func (m MetricList) Count() int {
	if m.Head == nil {
		return 0
	}
	return 1 + len(m.Tail)
}

func (v TagValues) Sort() {
	sort.Slice(v, func(i, j int) bool {
		if n := cmp.Compare(v[i].Value, v[j].Value); n != 0 {
			return n < 0
		}
		return v[i].Mapped < v[j].Mapped
	})
}

func (v TagValue) String() string {
	if v.Value != "" {
		return v.Value
	}
	if v.Mapped != 0 {
		return strconv.Itoa(int(v.Mapped))
	}
	return ""
}
