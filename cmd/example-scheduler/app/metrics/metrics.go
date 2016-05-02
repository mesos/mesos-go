package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	Subsystem = "example_scheduler"
)

// TODO(jdef) time in between offers

var (
	SubscriptionAttempts = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: Subsystem,
		Name:      "subscription_attempts",
		Help:      "The number of subscription attempts.",
	})
	APIErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: Subsystem,
		Name:      "api_error_count",
		Help:      "The number of unexpected http/v1 API errors.",
	}, []string{"call"})
	EventErrorCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: Subsystem,
		Name:      "event_error_count",
		Help:      "The number of event processing errors.",
	}, []string{"type"})
	EventReceivedCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: Subsystem,
		Name:      "event_received_count",
		Help:      "The number of events received.",
	}, []string{"type"})
	EventReceivedLatency = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Subsystem: Subsystem,
		Name:      "event_received_latency",
		Help:      "Time to process various events, by type.",
	}, []string{"type"})
	OffersReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: Subsystem,
		Name:      "offers_received",
		Help:      "The number of individual offers received.",
	})
	OffersDeclined = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: Subsystem,
		Name:      "offers_declined",
		Help:      "The number of offers declined.",
	})
	TasksFinished = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: Subsystem,
		Name:      "tasks_finished",
		Help:      "The number of tasks finished.",
	})
	TasksLaunched = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: Subsystem,
		Name:      "tasks_launched",
		Help:      "The number of tasks launched.",
	})
	JobStartCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: Subsystem,
		Name:      "job_start_count",
		Help:      "The number of internal background jobs started.",
	}, []string{"job"})
	ReviveCount = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: Subsystem,
		Name:      "revive_count",
		Help:      "The number of offer revive requests sent.",
	})
	OfferedResources = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Subsystem: Subsystem,
		Name:      "offered_resources",
		Help:      "Scalar resources offered by type.",
	}, []string{"type"})
	TasksLaunchedPerOfferCycle = prometheus.NewSummary(prometheus.SummaryOpts{
		Subsystem: Subsystem,
		Name:      "tasks_launched_per_cycle",
		Help:      "Number of tasks launched per-offers cycle (event).",
	})
	ArtifactDownloads = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: Subsystem,
		Name:      "artifact_downloads",
		Help:      "The number of artifacts served by the built-in http server.",
	})
)

var registerMetrics sync.Once

func Register() {
	registerMetrics.Do(func() {
		prometheus.MustRegister(SubscriptionAttempts)
		prometheus.MustRegister(APIErrorCount)
		prometheus.MustRegister(EventErrorCount)
		prometheus.MustRegister(EventReceivedCount)
		prometheus.MustRegister(EventReceivedLatency)
		prometheus.MustRegister(OffersReceived)
		prometheus.MustRegister(OffersDeclined)
		prometheus.MustRegister(ReviveCount)
		prometheus.MustRegister(JobStartCount)
		prometheus.MustRegister(TasksFinished)
		prometheus.MustRegister(TasksLaunched)
		prometheus.MustRegister(OfferedResources)
		prometheus.MustRegister(TasksLaunchedPerOfferCycle)
		prometheus.MustRegister(ArtifactDownloads)
	})
}

func InMicroseconds(d time.Duration) float64 {
	return float64(d.Nanoseconds() / time.Microsecond.Nanoseconds())
}
