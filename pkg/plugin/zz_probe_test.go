package plugin

import (
	"fmt"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

func TestZZProbeInterval(t *testing.T) {
	for _, in := range []string{"999999999999999999999s", "0s", "-1s", "1 MONTH", "1w", "1M", "1Min", " 10 s ", "1e3s", "500ms", "1mo", "07s", "9223372036854775807s", "4294967296s", "1 year", "2 MINUTES", "1minutes", "10", "s", "+5s", "1_000s", "3000000000s"} {
		s, ok := intervalToSeconds(in)
		fmt.Printf("INTERVAL %-26q -> %d ok=%v\n", in, s, ok)
	}
}

func TestZZProbeTimeGroupSQL(t *testing.T) {
	for _, in := range []string{"$__timeGroup(time, '3000000000s')", "$__timeGroup(time, '1w')", "$__timeGroup(time,'500ms')", "$__timeGroup(time, '5m', 0)"} {
		fmt.Printf("TG %-40q -> %s\n", in, expandTimeGroup(in))
	}
}

func TestZZProbeColumnArg(t *testing.T) {
	args := []string{
		"(SELECT x FROM secret)",
		"time) UNION SELECT (1",
		"time) OR (1=1",
		"time AT TIME ZONE UTC",
		"CASE WHEN 1=1 THEN time ELSE time END",
		"time::TIMESTAMP",
		"t.\"time\"",
		"time) AND (SELECT count(*) FROM secret)>0 AND (time",
		"time)) OR ((1=1",
		"time\nUNION ALL SELECT NULL,NULL",
		"time]",
		"a`b",
		"time#",
		"time%",
	}
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	for _, a := range args {
		err := validateColumnArg(a)
		src := "SELECT * FROM t WHERE $__timeFilter(" + a + ")"
		out := expandTimeFilter(src, from, to)
		fmt.Printf("COLARG %-50q err=%v\n   => %s\n", a, err, out)
	}
}

func TestZZProbeOptimize(t *testing.T) {
	cases := []string{
		"SELECT time, v FROM (SELECT time, v FROM t LIMIT 10) x",
		"WITH c AS (SELECT time FROM t LIMIT 5) SELECT time FROM c",
		"SELECT time, limit FROM t",
		"SELECT time FROM t OFFSET 5 LIMIT 10",
		"SELECT time FROM t LIMIT 10 OFFSET 5",
		"SELECT time, sum(v) OVER (ORDER BY time ROWS 3 PRECEDING) FROM t",
		"SELECT time FROM t FETCH FIRST 5 ROWS ONLY",
		"SELECT time FROM t -- trailing comment",
		"SELECT time, count(*) FROM t GROUP BY time HAVING count(*)>1",
		"SELECT time FROM t WHERE msg='limit 5'",
		"SELECT time FROM t WHERE msg='order by x'",
		"SELECT \"time\" FROM t",
		"SELECT t.time FROM t",
		"SELECT * FROM t WHERE time > now()",
		"SELECT time FROM t GROUP BY ALL",
		"SELECT time FROM t QUALIFY row_number() OVER (PARTITION BY id) = 1",
		"SELECT time FROM t LIMIT 10 -- c",
		"SELECT time FROM t\nLIMIT 10",
	}
	for _, c := range cases {
		fmt.Printf("OPT  in : %s\n     out: %s\n", c, OptimizeTimeSeriesQuery(c))
	}
}

func TestZZProbeSplitBounded(t *testing.T) {
	cases := []string{
		"SELECT * FROM t WHERE time BETWEEN (SELECT max(x) FROM u WHERE $__timeFilter(time)) AND now()",
		"WITH c AS (SELECT * FROM t WHERE $__timeFilter(time)) SELECT * FROM other",
		"SELECT * FROM t WHERE id IN (SELECT id FROM u WHERE $__timeFilter(time))",
		"SELECT * FROM t WHERE $__timeFrom() < time",
	}
	for _, c := range cases {
		fmt.Printf("SPLIT %-90q bounded=%v\n", c, hasTimeFilterMacro(newStrippedSQL(c)))
	}
	_ = backend.TimeRange{}
}
