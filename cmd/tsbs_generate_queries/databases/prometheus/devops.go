package prometheus

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

// TODO: Remove the need for this by continuing to bubble up errors
func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

// Devops produces Influx-specific queries for all the devops query types.
type Devops struct {
	*devops.Core
}

// NewDevops makes an Devops object ready to generate Queries.
func NewDevops(start, end time.Time, scale int) *Devops {
	core, err := devops.NewCore(start, end, scale)
	panicIfErr(err)
	return &Devops{core}
}

// GenerateEmptyQuery returns an empty query.HTTP
func (d *Devops) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

func (d *Devops) getPredicateWithValues(vals []string) string {
	var predicate string
	if len(vals) == 1 {
		predicate = "=\"" + vals[0] + "\""
	} else {
		predicate = "=~\"^(" + strings.Join(vals, "|") + ")$\""
	}

	return predicate
}

func (d *Devops) getHostLabelWithHostnames(hostnames []string) string {
	if len(hostnames) == 0 {
		return ""
	}
	return "hostname" + d.getPredicateWithValues(hostnames)
}

func (d *Devops) getHostLabelString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	return d.getHostLabelWithHostnames(hostnames)
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts,
// e.g. in PromQL
//
// max(max_over_time(cpu{__metric=~"metric1|metric2|...|metricN", hostname=~"hostname1|...|hostnameN"}[1m])) by (hostname)
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)

	humanLabel := fmt.Sprintf("Prometheus %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	promql := fmt.Sprintf("max(max_over_time(cpu{%s, __metric__%s}[1m])) by (__metric__)", d.getHostLabelString(nHosts), d.getPredicateWithValues(metrics))
	d.fillInRangeQuery(qi, humanLabel, humanDesc, promql, interval.StartUnixMillis(), interval.EndUnixMillis(), 60)
}

// GroupByOrderByLimit benchmarks a query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// e.g. in PromQL
//
// max(max_over_time(cpu{__metric__="usage_user"}[1m]))
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)

	humanLabel := "Prometheus max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	promql := "max(max_over_time(cpu{__metric__=\"usage\"}[1m]))"
	d.fillInRangeQuery(qi, humanLabel, humanDesc, promql, interval.EndUnixMillis()-5*60*1000, interval.EndUnixMillis(), 60)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in PromQL
//
//
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)

	humanLabel := devops.GetDoubleGroupByLabel("Prometheus", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	promql := fmt.Sprintf("avg(avg_over_time(cpu{__metric__%s}[1h])) by (__metric__, hostname)", d.getPredicateWithValues(metrics))
	d.fillInRangeQuery(qi, humanLabel, humanDesc, promql, interval.StartUnixMillis(), interval.EndUnixMillis(), 60*60)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in PromQL
//
// max(max_over_time(cpu{hostname=~"hostname1|...|hostnameN"}[1h])) by (__metric__)
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.MaxAllDuration)

	humanLabel := devops.GetMaxAllLabel("Prometheus", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	promql := fmt.Sprintf("max(max_over_time(cpu{%s}[1h])) by (__metric__)", d.getHostLabelString(nHosts))
	d.fillInRangeQuery(qi, humanLabel, humanDesc, promql, interval.StartUnixMillis(), interval.EndUnixMillis(), 60*60)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	humanLabel := "Prometheus last row per host"
	humanDesc := humanLabel + ": cpu"
	promql := "cpu"
	d.fillInQuery(qi, humanLabel, humanDesc, promql, d.Interval.EndUnixMillis())
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in PromQL
//
// cpu{__metric__="usage_user", hostname=~"hostname1|...|hostnameN"} > 90.0
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)

	var hostLabelString string

	if nHosts == 0 {
		hostLabelString = ""
	} else {
		hostLabelString = "," + d.getHostLabelString(nHosts)
	}

	humanLabel, err := devops.GetHighCPULabel("Prometheus", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	promql := fmt.Sprintf("cpu{__metric__=\"usage_user\"%s} > 90.0", hostLabelString)
	d.fillInRangeQuery(qi, humanLabel, humanDesc, promql, interval.StartUnixMillis(), interval.EndUnixMillis(), 60)
}

func (d *Devops) fillInQuery(qi query.Query, humanLabel, humanDesc, promql string, time int64) {
	v := url.Values{}
	v.Set("query", promql)
	v.Set("time", fmt.Sprintf("%.3f", float64(time)/1000.0))
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/api/v1/query?%s", v.Encode()))
	q.Body = nil
}

func (d *Devops) fillInRangeQuery(qi query.Query, humanLabel, humanDesc, promql string, start int64, end int64, step int) {
	v := url.Values{}
	v.Set("query", promql)
	v.Set("start", fmt.Sprintf("%.3f", float64(start)/1000.0))
	v.Set("end", fmt.Sprintf("%.3f", float64(end)/1000.0))
	v.Set("step", strconv.Itoa(step))
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/api/v1/query_range?%s", v.Encode()))
	q.Body = nil
}
