package redis

import (
	chronodiumTier "chronodium/server/tier"
	redis "gopkg.in/redis.v5"
	"testing"
	"time"
)

func TestPersistMetricInTier(t *testing.T) {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6380",
		DB:   2,
	})
	redisClient.FlushAll()
	pipeline := redisClient.Pipeline()

	m := &metric{
		key:   "foo",
		value: 3.14159,
		ts:    time.Unix(1485004710, 0),
	}

	tier := &chronodiumTier.Tier{
		RawGranularity: "PT2S",
	}
	tier.Validate()

	r := &Redis{}
	r.persistMetricInTier(pipeline, m, tier)

	cmds, _ := pipeline.Exec()
	expected := []string{
		"zincrby chronodium-1-{metric-foo}-1485004709-2 3.14159 1485004876: 3.14159",
		"zadd chronodium-1-gc-2 1.485004995e+09 chronodium-1-{metric-foo}-1485004709-2: 1",
		"expireat chronodium-1-{metric-foo}-1485004709-2 1485005551: true",
	}

	for k, v := range cmds {
		//fmt.Printf("\"%s\",\n", v.String())
		if v.String() != expected[k] {
			t.Errorf("Expected '%s' but got '%s'", expected[k], v.String())
		}
	}
}

func TestPersistMetricsInTier(t *testing.T) {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6380",
		DB:   2,
	})
	redisClient.FlushAll()
	pipeline := redisClient.Pipeline()

	metrics := make([]*metric, 0)
	for i, measurement := range series() {
		metrics = append(metrics, &metric{
			key:   "foo",
			value: measurement,
			ts:    time.Unix(int64(i+1485004710), 0),
		})
	}

	tier := &chronodiumTier.Tier{
		RawGranularity: "PT3S",
	}
	tier.Validate()

	r := &Redis{}
	for _, m := range metrics {
		r.persistMetricInTier(pipeline, m, tier)
	}
	cmds, _ := pipeline.Exec()
	expected := []string{
		"zincrby chronodium-1-{metric-foo}-1485004581-3 0 1485005004: 0",
		"zadd chronodium-1-gc-3 1.485004995e+09 chronodium-1-{metric-foo}-1485004581-3: 1",
		"expireat chronodium-1-{metric-foo}-1485004581-3 1485005679: true",
		"zincrby chronodium-1-{metric-foo}-1485004581-3 0.0032 1485005005: 0.0032",
		"zadd chronodium-1-gc-3 1.485004995e+09 chronodium-1-{metric-foo}-1485004581-3: 0",
		"expireat chronodium-1-{metric-foo}-1485004581-3 1485005679: true",
		"zincrby chronodium-1-{metric-foo}-1485004581-3 0.0064 1485005006: 0.0064",
		"zadd chronodium-1-gc-3 1.485004995e+09 chronodium-1-{metric-foo}-1485004581-3: 0",
		"expireat chronodium-1-{metric-foo}-1485004581-3 1485005679: true",
		"zincrby chronodium-1-{metric-foo}-1485004581-3 0.009600000000000001 1485005010: 0.009600000000000001",
		"zadd chronodium-1-gc-3 1.485004995e+09 chronodium-1-{metric-foo}-1485004581-3: 0",
		"expireat chronodium-1-{metric-foo}-1485004581-3 1485005679: true",
		"zincrby chronodium-1-{metric-foo}-1485004581-3 0.0128 1485005011: 0.0128",
		"zadd chronodium-1-gc-3 1.485004995e+09 chronodium-1-{metric-foo}-1485004581-3: 0",
		"expireat chronodium-1-{metric-foo}-1485004581-3 1485005679: true",
		"zincrby chronodium-1-{metric-foo}-1485004581-3 0.016 1485005012: 0.016",
		"zadd chronodium-1-gc-3 1.485004995e+09 chronodium-1-{metric-foo}-1485004581-3: 0",
		"expireat chronodium-1-{metric-foo}-1485004581-3 1485005679: true",
		"zincrby chronodium-1-{metric-foo}-1485004581-3 0.019200000000000002 1485005016: 0.019200000000000002",
		"zadd chronodium-1-gc-3 1.485004995e+09 chronodium-1-{metric-foo}-1485004581-3: 0",
		"expireat chronodium-1-{metric-foo}-1485004581-3 1485005679: true",
		"zincrby chronodium-1-{metric-foo}-1485004581-3 0.0224 1485005017: 0.0224",
		"zadd chronodium-1-gc-3 1.485004995e+09 chronodium-1-{metric-foo}-1485004581-3: 0",
		"expireat chronodium-1-{metric-foo}-1485004581-3 1485005679: true",
		"zincrby chronodium-1-{metric-foo}-1485004581-3 0.0256 1485005018: 0.0256",
		"zadd chronodium-1-gc-3 1.485004995e+09 chronodium-1-{metric-foo}-1485004581-3: 0",
		"expireat chronodium-1-{metric-foo}-1485004581-3 1485005679: true",
		"zincrby chronodium-1-{metric-foo}-1485004581-3 0.028800000000000003 1485005022: 0.028800000000000003",
		"zadd chronodium-1-gc-3 1.485004995e+09 chronodium-1-{metric-foo}-1485004581-3: 0",
		"expireat chronodium-1-{metric-foo}-1485004581-3 1485005679: true",
	}

	for k, v := range cmds {
		//fmt.Printf("\"%s\",\n", v.String())
		if v.String() != expected[k] {
			t.Errorf("Expected '%s' but got '%s'", expected[k], v.String())
		}
	}
}

func series() (series []float64) {
	series = make([]float64, 0)
	val := 0.0
	for i := 0; i < 10; i++ {
		series = append(series, val+(float64(i)*0.0032))
	}

	return series
}

type metric struct {
	key   string
	value float64
	ts    time.Time
}

func (m *metric) Key() string {
	return m.key
}

func (m *metric) Value() float64 {
	return m.value
}

func (m *metric) Time() time.Time {
	return m.ts
}
