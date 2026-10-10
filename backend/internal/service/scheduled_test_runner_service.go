package service

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/robfig/cron/v3"
)

const (
	scheduledTestDefaultMaxWorkers = 10
	scheduledTestTickDelay         = 10 * time.Second
	// Pelican/quality rounds run under this tick context; keep it below the
	// 15-minute ClaimPelican lease (pelican_scheduled.go).
	scheduledTestRunTimeout    = 12 * time.Minute
	scheduledTestFinishTimeout = 2 * time.Second
)

// ScheduledTestRunnerService periodically scans due test plans and executes them.
type ScheduledTestRunnerService struct {
	planRepo       ScheduledTestPlanRepository
	scheduledSvc   *ScheduledTestService
	accountTestSvc *AccountTestService
	rateLimitSvc   *RateLimitService
	cfg            *config.Config
	judgeQuality   func(context.Context, int64, *PelicanTestConfig, string) *QualityJudgment
	runPelican     func(context.Context, int64, string, *PelicanTestConfig) (*ScheduledTestResult, error)
	// groupTests runs the Pelican group tests on the same tick; nil disables them.
	groupTests   *PelicanGroupTestService
	candyMonitor *ChannelMonitorV2CandyService

	qualityTrigger *quality5xxTrigger
	triggerCancel  context.CancelFunc
	triggerWG      sync.WaitGroup
	planSlots      chan struct{}
	cron           *cron.Cron
	startOnce      sync.Once
	stopOnce       sync.Once
	// running guards the scan/dispatch phase only; see runScheduled.
	running atomic.Bool
}

// NewScheduledTestRunnerService creates a new runner.
func NewScheduledTestRunnerService(
	planRepo ScheduledTestPlanRepository,
	scheduledSvc *ScheduledTestService,
	accountTestSvc *AccountTestService,
	rateLimitSvc *RateLimitService,
	cfg *config.Config,
) *ScheduledTestRunnerService {
	return &ScheduledTestRunnerService{
		planRepo:       planRepo,
		planSlots:      make(chan struct{}, scheduledTestDefaultMaxWorkers),
		scheduledSvc:   scheduledSvc,
		accountTestSvc: accountTestSvc,
		rateLimitSvc:   rateLimitSvc,
		cfg:            cfg,
		runPelican:     accountTestSvc.RunPelicanBackground,
	}
}

// Start begins the cron ticker (every minute).
func (s *ScheduledTestRunnerService) Start() {
	if s == nil {
		return
	}
	s.startOnce.Do(func() {
		s.startQualityTriggers()
		loc := time.Local
		if s.cfg != nil {
			if parsed, err := time.LoadLocation(s.cfg.Timezone); err == nil && parsed != nil {
				loc = parsed
			}
		}

		c := cron.New(cron.WithParser(scheduledTestCronParser), cron.WithLocation(loc))
		_, err := c.AddFunc("* * * * *", func() { s.runScheduled() })
		if err != nil {
			logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] not started (invalid schedule): %v", err)
			return
		}
		s.cron = c
		s.cron.Start()
		logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] started (tick=every minute)")
	})
}

// Stop gracefully shuts down the cron scheduler.
func (s *ScheduledTestRunnerService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		if s.qualityTrigger != nil && s.qualityTrigger.queue != nil {
			defer func() { _ = s.qualityTrigger.queue.Close() }()
		}
		if s.triggerCancel != nil {
			s.triggerCancel()
			s.triggerWG.Wait()
		}
		if s.cron != nil {
			ctx := s.cron.Stop()
			select {
			case <-ctx.Done():
			case <-time.After(3 * time.Second):
				logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] cron stop timed out")
			}
		}
	})
}

func (s *ScheduledTestRunnerService) runScheduled() {
	if !s.running.CompareAndSwap(false, true) {
		return
	}
	// The guard prevents scans from piling up on one instance, but it is
	// released once every due plan is dispatched: long Pelican/quality rounds
	// must not hold back the next minute's tick. Cross-tick concurrency stays
	// bounded by planSlots, and duplicates are prevented by TryClaimDue /
	// ClaimPelican.
	var scanDone sync.Once
	endScan := func() { scanDone.Do(func() { s.running.Store(false) }) }
	defer endScan()
	// Delay 10s so execution lands at ~:10 of each minute instead of :00.
	time.Sleep(scheduledTestTickDelay)

	ctx, cancel := context.WithTimeout(context.Background(), scheduledTestRunTimeout)
	defer cancel()

	now := time.Now()
	if err := s.scheduledSvc.resultRepo.PruneExpiredPelican(ctx, now.Add(-7*24*time.Hour)); err != nil {
		logger.LegacyPrintf("service.scheduled_test_runner", "pelican history cleanup failed: %v", err)
	}
	s.groupTests.Cleanup(ctx, now)
	// Group tests run beside the account plans rather than after them.
	var groupRuns sync.WaitGroup
	groupRuns.Add(2)
	go func() { defer groupRuns.Done(); s.candyMonitor.RunDue(ctx, now) }()
	go func() {
		defer groupRuns.Done()
		s.groupTests.RunDue(ctx, now)
	}()
	defer groupRuns.Wait()
	// Group rules cover accounts that started matching since the last tick.
	if created, err := s.scheduledSvc.SyncQualityTemplates(ctx); err != nil {
		logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] quality template sync error: %v", err)
	} else if created > 0 {
		logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] quality templates added %d rules", created)
	}
	plans, err := s.planRepo.ListDue(ctx, now)
	if err != nil {
		logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] ListDue error: %v", err)
		endScan()
		return
	}
	if len(plans) == 0 {
		endScan()
		return
	}

	logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] found %d due plans", len(plans))

	sem := make(chan struct{}, scheduledTestDefaultMaxWorkers)
	var wg sync.WaitGroup

	for _, plan := range plans {
		select {
		case <-ctx.Done():
			endScan()
			wg.Wait()
			return
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(p *ScheduledTestPlan) {
			defer wg.Done()
			defer func() { <-sem }()
			s.runOnePlan(ctx, p)
		}(plan)
	}
	endScan()

	wg.Wait()
}

// runOnePlan executes one plan and reports whether a run actually completed
// (callers such as the quality 5xx trigger rely on false meaning "not run").
// Pelican/quality plans are claimed by runPelicanPlan via ClaimPelican, which
// also serves triggered runs whose next_run_at is not yet due. Ordinary plans
// are claimed with TryClaimDue, which advances next_run_at before any probe
// and holds a lease for the duration of the test.
func (s *ScheduledTestRunnerService) runOnePlan(ctx context.Context, plan *ScheduledTestPlan) (completed bool) {
	if plan == nil {
		return false
	}
	if s.planSlots != nil {
		select {
		case s.planSlots <- struct{}{}:
			defer func() { <-s.planSlots }()
		case <-ctx.Done():
			return false
		}
	}
	ctx = context.WithValue(ctx, qualityProbeContextKey{}, true)
	// Background plans do not run under the HTTP recovery middleware.
	defer func() {
		if recover() != nil {
			completed = false
			logger.LegacyPrintf("service.scheduled_test_runner", "scheduled plan=%d account=%d panicked", plan.ID, plan.AccountID)
		}
	}()
	if plan.PelicanConfig != nil {
		return s.runPelicanPlan(ctx, plan)
	}
	now := time.Now()
	if !plan.Enabled || plan.NextRunAt == nil || plan.NextRunAt.After(now) || ctx.Err() != nil {
		return false
	}
	nextRun, err := computeNextRun(plan.CronExpression, now)
	if err != nil || !nextRun.After(now) {
		logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] plan=%d invalid next run: %v", plan.ID, err)
		return false
	}
	lease, claimed, err := s.planRepo.TryClaimDue(ctx, plan, now, nextRun)
	if err != nil {
		logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] plan=%d claim error: %v", plan.ID, err)
		return false
	}
	if !claimed {
		return false
	}
	defer func() {
		if err := lease.Release(); err != nil {
			logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] plan=%d release error: %v", plan.ID, err)
		}
	}()
	if ctx.Err() != nil {
		return false
	}
	result, err := s.accountTestSvc.RunTestBackground(ctx, plan.AccountID, plan.ModelID)
	if err != nil {
		logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] plan=%d RunTestBackground error: %v", plan.ID, err)
		return false
	}
	defer func() {
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), scheduledTestFinishTimeout)
		defer cancel()
		if err := s.planRepo.MarkRunFinished(finishCtx, plan.ID, time.Now()); err != nil {
			logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] plan=%d finish error: %v", plan.ID, err)
		}
	}()
	if err := s.scheduledSvc.SaveResult(ctx, plan.ID, plan.MaxResults, result); err != nil {
		logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] plan=%d SaveResult error: %v", plan.ID, err)
	}

	// Auto-recover account if test succeeded and auto_recover is enabled. A
	// credentials-only success（双开账号的 GET /models 探针）only clears error status.
	if result.Status == "success" && plan.AutoRecover {
		s.tryRecoverAccount(ctx, plan.AccountID, plan.ID, result.CredentialsOnly)
	}

	return true
}

// tryRecoverAccount attempts to recover an account from recoverable runtime state.
func (s *ScheduledTestRunnerService) tryRecoverAccount(ctx context.Context, accountID int64, planID int64, credentialsOnly bool) {
	if s.rateLimitSvc == nil {
		return
	}

	recovery, err := s.rateLimitSvc.RecoverAccountAfterSuccessfulTest(ctx, accountID, credentialsOnly)
	if err != nil {
		logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] plan=%d auto-recover failed: %v", planID, err)
		return
	}
	if recovery == nil {
		return
	}

	if recovery.ClearedError {
		logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] plan=%d auto-recover: account=%d recovered from error status", planID, accountID)
	}
	if recovery.ClearedRateLimit {
		logger.LegacyPrintf("service.scheduled_test_runner", "[ScheduledTestRunner] plan=%d auto-recover: account=%d cleared rate-limit/runtime state", planID, accountID)
	}
}
