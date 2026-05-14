package poller

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	servicedto "cpa-usage-keeper/internal/service/dto"
	"github.com/sirupsen/logrus"
)

type redisDrainSyncStub struct {
	mu             sync.Mutex
	pullResults    []*servicedto.RedisInboxPullResult
	pullErrs       []error
	processResults []*servicedto.RedisBatchSyncResult
	processErrs    []error
	pullStarted    chan struct{}
	releasePull    chan struct{}
	pullCalls      int
	processCalls   int
}

func (s *redisDrainSyncStub) PullRedisUsageInbox(context.Context) (*servicedto.RedisInboxPullResult, error) {
	s.mu.Lock()
	s.pullCalls++
	call := s.pullCalls
	result := &servicedto.RedisInboxPullResult{Status: "completed", InsertedRows: 1}
	if len(s.pullResults) >= call {
		result = s.pullResults[call-1]
	} else if len(s.pullResults) > 0 {
		result = s.pullResults[len(s.pullResults)-1]
	}
	var err error
	if len(s.pullErrs) >= call {
		err = s.pullErrs[call-1]
	} else if len(s.pullErrs) > 0 {
		err = s.pullErrs[len(s.pullErrs)-1]
	}
	pullStarted := s.pullStarted
	releasePull := s.releasePull
	s.mu.Unlock()
	if pullStarted != nil {
		close(pullStarted)
	}
	if releasePull != nil {
		<-releasePull
	}
	return result, err
}

func (s *redisDrainSyncStub) ProcessRedisUsageInbox(ctx context.Context) (*servicedto.RedisBatchSyncResult, error) {
	s.mu.Lock()
	s.processCalls++
	call := s.processCalls
	result := &servicedto.RedisBatchSyncResult{Status: "completed", InsertedEvents: 1}
	if len(s.processResults) >= call {
		result = s.processResults[call-1]
	} else if len(s.processResults) > 0 {
		result = s.processResults[len(s.processResults)-1]
	}
	var err error
	if len(s.processErrs) >= call {
		err = s.processErrs[call-1]
	} else if len(s.processErrs) > 0 {
		err = s.processErrs[len(s.processErrs)-1]
	}
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return result, err
}

func (s *redisDrainSyncStub) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pullCalls, s.processCalls
}

func captureRedisDrainLogrusOutput(t *testing.T) *bytes.Buffer {
	t.Helper()
	previousOutput := logrus.StandardLogger().Out
	previousFormatter := logrus.StandardLogger().Formatter
	previousLevel := logrus.GetLevel()
	var logs bytes.Buffer
	logrus.SetOutput(&logs)
	logrus.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true})
	logrus.SetLevel(logrus.InfoLevel)
	t.Cleanup(func() {
		logrus.SetOutput(previousOutput)
		logrus.SetFormatter(previousFormatter)
		logrus.SetLevel(previousLevel)
	})
	return &logs
}

func TestRedisDrainLoopsLogTaskStarts(t *testing.T) {
	logs := captureRedisDrainLogrusOutput(t)
	syncer := &redisDrainSyncStub{pullResults: []*servicedto.RedisInboxPullResult{{Empty: true, Status: "empty"}}}
	drain := NewRedisDrain(syncer, RedisDrainConfig{IdleInterval: time.Hour, ErrorBackoff: time.Hour})

	pullCtx, cancelPull := context.WithCancel(context.Background())
	drain.sleep = func(context.Context, time.Duration) bool {
		cancelPull()
		return false
	}
	drain.runRedisInboxPullLoop(pullCtx)

	processCtx, cancelProcess := context.WithCancel(context.Background())
	drain.sleep = func(context.Context, time.Duration) bool {
		cancelProcess()
		return false
	}
	drain.runRedisInboxProcessLoop(processCtx)

	content := logs.String()
	for _, expected := range []string{"msg=\"redis inbox pull task started\"", "msg=\"redis inbox process task started\""} {
		if !strings.Contains(content, expected) {
			t.Fatalf("expected redis drain start log %q, got %q", expected, content)
		}
	}
}

func TestRedisDrainPullLoopDoesNotProcessInbox(t *testing.T) {
	syncer := &redisDrainSyncStub{pullResults: []*servicedto.RedisInboxPullResult{{Empty: true, Status: "empty"}}}
	drain := NewRedisDrain(syncer, RedisDrainConfig{IdleInterval: time.Hour, ErrorBackoff: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	drain.sleep = func(context.Context, time.Duration) bool {
		cancel()
		return false
	}

	drain.runRedisInboxPullLoop(ctx)

	pulls, processes := syncer.counts()
	if pulls != 1 || processes != 0 {
		t.Fatalf("expected pull loop to pull once and not process inbox, got pulls=%d processes=%d", pulls, processes)
	}
}

func TestRedisDrainProcessLoopRunsThenSleepsForOneSecond(t *testing.T) {
	syncer := &redisDrainSyncStub{}
	drain := NewRedisDrain(syncer, RedisDrainConfig{IdleInterval: time.Hour, ErrorBackoff: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	drain.sleep = func(_ context.Context, d time.Duration) bool {
		if d != redisInboxProcessInterval {
			t.Fatalf("expected process interval %s, got %s", redisInboxProcessInterval, d)
		}
		pulls, processes := syncer.counts()
		if pulls != 0 || processes != 1 {
			t.Fatalf("expected process loop to run before sleeping, got pulls=%d processes=%d", pulls, processes)
		}
		cancel()
		return false
	}

	drain.runRedisInboxProcessLoop(ctx)

	_, processes := syncer.counts()
	if processes != 1 {
		t.Fatalf("expected process loop to process once, got %d", processes)
	}
}

func TestRedisPullAndProcessRunnersRunIndependently(t *testing.T) {
	syncer := &redisDrainSyncStub{pullResults: []*servicedto.RedisInboxPullResult{{Empty: true, Status: "empty"}}}
	drain := NewRedisDrain(syncer, RedisDrainConfig{IdleInterval: time.Hour, ErrorBackoff: time.Hour})
	pullRunner := NewRedisPullRunner(drain)
	processRunner := NewRedisProcessRunner(drain)

	pullCtx, cancelPull := context.WithCancel(context.Background())
	drain.sleep = func(context.Context, time.Duration) bool {
		cancelPull()
		return false
	}
	if err := pullRunner.Run(pullCtx); err != nil {
		t.Fatalf("pull runner returned error: %v", err)
	}
	pulls, processes := syncer.counts()
	if pulls != 1 || processes != 0 {
		t.Fatalf("expected pull runner to only pull, got pulls=%d processes=%d", pulls, processes)
	}

	processCtx, cancelProcess := context.WithCancel(context.Background())
	drain.sleep = func(context.Context, time.Duration) bool {
		cancelProcess()
		return false
	}
	if err := processRunner.Run(processCtx); err != nil {
		t.Fatalf("process runner returned error: %v", err)
	}
	pulls, processes = syncer.counts()
	if pulls != 1 || processes != 1 {
		t.Fatalf("expected process runner to only process, got pulls=%d processes=%d", pulls, processes)
	}
}

func TestRedisDrainProcessSuccessDoesNotClearPullError(t *testing.T) {
	drain := NewRedisDrain(&redisDrainSyncStub{}, RedisDrainConfig{IdleInterval: time.Second, ErrorBackoff: time.Second})

	drain.recordRedisPullResult(nil, errors.New("redis unavailable"))
	drain.recordRedisProcessResult(&servicedto.RedisBatchSyncResult{Status: "empty"}, nil)

	status := drain.Status()
	if status.LastError != "redis unavailable" {
		t.Fatalf("expected process success not to clear pull error, got %+v", status)
	}
}

func TestRedisDrainPullSuccessDoesNotClearProcessError(t *testing.T) {
	drain := NewRedisDrain(&redisDrainSyncStub{}, RedisDrainConfig{IdleInterval: time.Second, ErrorBackoff: time.Second})

	drain.recordRedisProcessResult(&servicedto.RedisBatchSyncResult{Status: "failed"}, errors.New("aggregate failed"))
	drain.recordRedisPullResult(&servicedto.RedisInboxPullResult{Status: "empty"}, nil)

	status := drain.Status()
	if status.LastError != "aggregate failed" {
		t.Fatalf("expected pull success not to clear process error, got %+v", status)
	}
}

func TestRedisDrainRunningStatusTracksMultipleIndependentRunners(t *testing.T) {
	drain := NewRedisDrain(&redisDrainSyncStub{}, RedisDrainConfig{IdleInterval: time.Second, ErrorBackoff: time.Second})

	drain.setRunning(true)
	drain.setRunning(true)
	drain.setRunning(false)

	if !drain.Status().Running {
		t.Fatal("expected Redis status to stay running while one split runner is still active")
	}
	drain.setRunning(false)
	if drain.Status().Running {
		t.Fatal("expected Redis status to stop after all split runners exit")
	}
}

func TestRedisDrainSyncNowPullsThenProcesses(t *testing.T) {
	syncer := &redisDrainSyncStub{}
	drain := NewRedisDrain(syncer, RedisDrainConfig{IdleInterval: time.Hour, ErrorBackoff: time.Hour})

	if err := drain.SyncNow(context.Background()); err != nil {
		t.Fatalf("SyncNow returned error: %v", err)
	}

	pulls, processes := syncer.counts()
	if pulls != 1 || processes != 1 {
		t.Fatalf("expected SyncNow to pull and process once, got pulls=%d processes=%d", pulls, processes)
	}
}

func TestRedisDrainPullAndProcessCanRunIndependently(t *testing.T) {
	syncer := &redisDrainSyncStub{pullStarted: make(chan struct{}), releasePull: make(chan struct{})}
	drain := NewRedisDrain(syncer, RedisDrainConfig{IdleInterval: time.Hour, ErrorBackoff: time.Hour})
	ctx := context.Background()
	pullDone := make(chan error, 1)
	go func() {
		_, err := drain.pullRedisInboxOnce(ctx)
		pullDone <- err
	}()
	<-syncer.pullStarted

	if _, err := drain.processRedisInboxOnce(ctx); err != nil {
		close(syncer.releasePull)
		t.Fatalf("expected process to run while pull is active, got %v", err)
	}
	close(syncer.releasePull)
	if err := <-pullDone; err != nil {
		t.Fatalf("pull returned error: %v", err)
	}

	pulls, processes := syncer.counts()
	if pulls != 1 || processes != 1 {
		t.Fatalf("expected pull and process to each run once, got pulls=%d processes=%d", pulls, processes)
	}
}

func TestRedisDrainBacksOffAfterPullError(t *testing.T) {
	syncer := &redisDrainSyncStub{pullErrs: []error{errors.New("dial failed")}}
	drain := NewRedisDrain(syncer, RedisDrainConfig{IdleInterval: time.Hour, ErrorBackoff: 25 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	var slept time.Duration
	drain.sleep = func(_ context.Context, d time.Duration) bool {
		slept = d
		cancel()
		return false
	}

	drain.runRedisInboxPullLoop(ctx)

	if slept != 25*time.Millisecond {
		t.Fatalf("expected error backoff sleep, got %s", slept)
	}
	status := drain.Status()
	if status.LastError != "dial failed" {
		t.Fatalf("expected recorded pull error, got %+v", status)
	}
}
