package poller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	servicedto "cpa-usage-keeper/internal/service/dto"
	"cpa-usage-keeper/internal/timeutil"
	"github.com/sirupsen/logrus"
)

// Redis inbox pull/process 都使用单 goroutine 串行 loop；每轮执行完成后再等待 1 秒，避免任务重入。
const redisInboxProcessInterval = time.Second

type RedisBatchSyncer interface {
	PullRedisUsageInbox(ctx context.Context) (*servicedto.RedisInboxPullResult, error)
	ProcessRedisUsageInbox(ctx context.Context) (*servicedto.RedisBatchSyncResult, error)
}

type RedisDrainConfig struct {
	IdleInterval time.Duration
	ErrorBackoff time.Duration
}

type RedisDrain struct {
	syncer RedisBatchSyncer
	config RedisDrainConfig
	now    func() time.Time
	sleep  func(context.Context, time.Duration) bool

	mu             sync.Mutex
	running        bool
	runningRefs    int
	lastRunAt          time.Time
	lastError          string
	lastWarning        string
	lastStatus         string
	lastPullError      string
	lastProcessError   string
	lastProcessWarning string
	pullRunning        bool
	processRunning bool
}

func NewRedisDrain(syncer RedisBatchSyncer, cfg RedisDrainConfig) *RedisDrain {
	return &RedisDrain{
		syncer: syncer,
		config: cfg,
		now:    time.Now,
		sleep:  sleepContext,
	}
}

// RedisPullRunner 是独立的 Redis 拉取 runner，只负责 Redis 队列到 redis_usage_inboxes。
type RedisPullRunner struct {
	drain *RedisDrain // 共享 drain 状态和 syncer，但只调用 Pull 链路。
}

// RedisProcessRunner 是独立的 Redis 处理 runner，只负责 redis_usage_inboxes 到 usage_events。
type RedisProcessRunner struct {
	drain *RedisDrain // 共享 drain 状态和 syncer，但只调用 Process 链路。
}

// NewRedisPullRunner 用共享 RedisDrain 构造只拉取远端队列的后台 runner。
func NewRedisPullRunner(drain *RedisDrain) *RedisPullRunner {
	// 返回独立 Pull runner；它和 Process runner 共享状态但拥有独立 Run 入口。
	return &RedisPullRunner{drain: drain}
}

// NewRedisProcessRunner 用共享 RedisDrain 构造只处理本地 inbox 的后台 runner。
func NewRedisProcessRunner(drain *RedisDrain) *RedisProcessRunner {
	// 返回独立 Process runner；它和 Pull runner 共享状态但拥有独立 Run 入口。
	return &RedisProcessRunner{drain: drain}
}

// Run 是 App 后台任务接口入口；实际语义交给 RunPullLoop，避免调用方把它误解成完整 Redis 同步。
func (r *RedisPullRunner) Run(ctx context.Context) error {
	// 入口只转发到带业务名的方法，保证 App 统一 Runner 接口和 Redis Pull 语义同时清晰。
	return r.RunPullLoop(ctx)
}

// RunPullLoop 启动独立 Redis Pull runner，只负责 Redis 队列到本地 inbox 的搬运。
func (r *RedisPullRunner) RunPullLoop(ctx context.Context) error {
	// runner 自身为空或 drain 未注入时直接失败，避免后台 goroutine 静默退出。
	if r == nil || r.drain == nil {
		// 返回明确的 pull runner 初始化错误，方便启动日志定位是哪一个 runner 配置缺失。
		return fmt.Errorf("redis pull runner is not initialized")
	}
	// 复用 drain 的配置校验，确保间隔、退避和依赖在进入循环前都有效。
	if err := r.drain.validate(); err != nil {
		// 校验失败时不进入循环，让 App 启动日志暴露真实原因。
		return err
	}
	// 标记 Redis 后台任务处于运行中，status 接口可以看到 pull/process 任一 runner 已启动。
	r.drain.setRunning(true)
	// Pull runner 退出时清除运行状态；如果 Process runner 仍在跑，下一轮状态会由它重新写入。
	defer r.drain.setRunning(false)
	// 进入只拉取 Redis 队列的串行循环，不处理 inbox、不写 usage_events。
	r.drain.runRedisInboxPullLoop(ctx)
	// 循环只会在 ctx 取消或 sleep 中断后返回；正常退出不视为错误。
	return nil
}

// Run 是 App 后台任务接口入口；实际语义交给 RunProcessLoop，避免调用方把它误解成完整 Redis 同步。
func (r *RedisProcessRunner) Run(ctx context.Context) error {
	// 入口只转发到带业务名的方法，保证 App 统一 Runner 接口和 Redis Process 语义同时清晰。
	return r.RunProcessLoop(ctx)
}

// RunProcessLoop 启动独立 Redis Process runner，只负责本地 inbox 到 usage_events 及增量统计。
func (r *RedisProcessRunner) RunProcessLoop(ctx context.Context) error {
	// runner 自身为空或 drain 未注入时直接失败，避免后台 goroutine 静默退出。
	if r == nil || r.drain == nil {
		// 返回明确的 process runner 初始化错误，方便启动日志定位是哪一个 runner 配置缺失。
		return fmt.Errorf("redis process runner is not initialized")
	}
	// 复用 drain 的配置校验，确保间隔、退避和依赖在进入循环前都有效。
	if err := r.drain.validate(); err != nil {
		// 校验失败时不进入循环，让 App 启动日志暴露真实原因。
		return err
	}
	// 标记 Redis 后台任务处于运行中，status 接口可以看到 pull/process 任一 runner 已启动。
	r.drain.setRunning(true)
	// Process runner 退出时清除运行状态；如果 Pull runner 仍在跑，下一轮状态会由它重新写入。
	defer r.drain.setRunning(false)
	// 进入只处理本地 inbox 的串行循环，不触碰远端 Redis 队列。
	r.drain.runRedisInboxProcessLoop(ctx)
	// 循环只会在 ctx 取消或 sleep 中断后返回；正常退出不视为错误。
	return nil
}

// Run 是旧的组合式 Redis runner 入口；新启动路径优先使用 RedisPullRunner 和 RedisProcessRunner。
func (d *RedisDrain) Run(ctx context.Context) error {
	// 组合式入口仍先校验 drain，避免历史调用方绕过新 runner 时拿到无效配置。
	if err := d.validate(); err != nil {
		// 校验失败时不启动任何 goroutine，直接把配置错误返回给调用方。
		return err
	}
	// 标记 Redis 后台任务整体运行中，兼容旧组合式入口的 status 行为。
	d.setRunning(true)
	// 组合式入口退出时清除整体运行状态。
	defer d.setRunning(false)

	var wg sync.WaitGroup
	// 组合式入口内部仍启动两个 goroutine，但 App.Run 不再使用这个入口。
	wg.Add(2)
	// 启动 Pull goroutine，只负责远端 Redis 队列到本地 inbox。
	go func() {
		// Pull goroutine 退出时通知 WaitGroup。
		defer wg.Done()
		// 运行 Pull 专属循环，不处理本地 inbox。
		d.runRedisInboxPullLoop(ctx)
	}()
	// 启动 Process goroutine，只负责本地 inbox 到 usage_events 和增量统计。
	go func() {
		// Process goroutine 退出时通知 WaitGroup。
		defer wg.Done()
		// 运行 Process 专属循环，不拉取远端 Redis。
		d.runRedisInboxProcessLoop(ctx)
	}()
	// 等待上层取消信号，保持组合式入口阻塞运行。
	<-ctx.Done()
	// 等待两个内部 goroutine 都完成退出，避免 Run 返回后仍有后台逻辑运行。
	wg.Wait()
	// ctx 取消是正常停止路径，不作为错误返回。
	return nil
}

// runRedisInboxPullLoop 只从 CPA Redis 队列 LPOP 数据并写入 redis_usage_inboxes，不解码、不写 usage_events。
func (d *RedisDrain) runRedisInboxPullLoop(ctx context.Context) {
	// 记录 Pull runner 启动和轮询间隔，方便日志里区分 Redis Pull 与 Redis Process。
	logrus.WithField("idle_interval", d.config.IdleInterval.String()).Info("redis inbox pull task started")
	// 使用无限循环让 Pull runner 持续从远端队列搬运消息，退出条件统一由 ctx 控制。
	for {
		// 每轮开始先检查退出信号，避免服务关闭时再发起一次远端 Redis 操作。
		select {
		case <-ctx.Done():
			// 上层取消时立即退出 Pull loop，把 goroutine 生命周期交还给 App。
			return
		default:
		}
		// 单轮 Pull 只做 Redis -> redis_usage_inboxes，内部会做 Pull 自身的重入保护和状态记录。
		_, err := d.pullRedisInboxOnce(ctx)
		// Pull 失败时只影响远端拉取，不阻塞独立的本地 Process runner。
		if err != nil {
			// 可记录的错误写入错误日志；上下文取消这类正常退出错误会被过滤。
			if shouldLogSyncError(err) {
				// 日志消息明确标记 pull failed，避免和本地 inbox 处理失败混在一起。
				logrus.WithError(err).Error("redis drain pull failed")
			}
			// 失败后按 ErrorBackoff 等待，避免远端 Redis 异常时形成紧密重试循环。
			if !d.sleep(ctx, d.config.ErrorBackoff) {
				// sleep 被 ctx 打断表示服务正在停止，直接退出 Pull loop。
				return
			}
			// 退避完成后进入下一轮 Pull，不执行成功路径的 idle sleep。
			continue
		}
		// 成功 Pull 后固定等待 idle interval；即使本轮没有消息，也保持 1s 串行节奏。
		if !d.sleep(ctx, d.config.IdleInterval) {
			// sleep 被 ctx 打断表示服务正在停止，直接退出 Pull loop。
			return
		}
	}
}

// runRedisInboxProcessLoop 串行处理已落库的 inbox 行，失败行保留为可重试状态，坏消息单独标记不阻塞后续行。
func (d *RedisDrain) runRedisInboxProcessLoop(ctx context.Context) {
	// 记录 Process runner 启动和处理间隔，方便日志里区分 Redis Pull 与 Redis Process。
	logrus.WithField("interval", redisInboxProcessInterval.String()).Info("redis inbox process task started")
	// 使用无限循环让 Process runner 持续消化本地 inbox，退出条件统一由 ctx 控制。
	for {
		// 每轮开始先检查退出信号，避免服务关闭时再开启一次 SQLite 写事务。
		select {
		case <-ctx.Done():
			// 上层取消时立即退出 Process loop，把 goroutine 生命周期交还给 App。
			return
		default:
		}
		// 单轮 Process 只做 redis_usage_inboxes -> usage_events -> 增量统计，内部会做 Process 自身的重入保护。
		result, err := d.processRedisInboxOnce(ctx)
		// completed_with_warnings 表示坏消息已被隔离，不应按整批失败输出错误日志。
		if err != nil && !errors.Is(err, ErrSyncCompletedWithWarnings) {
			// 可记录的错误写入带批次字段的错误日志；上下文取消这类正常退出错误会被过滤。
			if shouldLogSyncError(err) {
				// 批次失败日志包含插入数、去重数和状态，便于定位 Process 阶段失败范围。
				d.logBatchFailure(result, err)
			}
		}
		// 每轮 Process 完成后固定等待 1s；如果处理耗时超过 1s，下一轮自然顺延且不会重入。
		if !d.sleep(ctx, redisInboxProcessInterval) {
			// sleep 被 ctx 打断表示服务正在停止，直接退出 Process loop。
			return
		}
	}
}

// logBatchFailure 输出 Process 失败批次的关键计数字段，便于区分解码问题、插入问题和聚合问题。
func (d *RedisDrain) logBatchFailure(result *servicedto.RedisBatchSyncResult, err error) {
	fields := logrus.Fields{
		"status":          "",
		"empty":           false,
		"inserted_events": 0,
		"deduped_events":  0,
	}
	// 如果 service 层返回了批次结果，就把结果字段补进日志上下文。
	if result != nil {
		// status 展示本轮 Process 的最终状态。
		fields["status"] = result.Status
		// empty 展示本轮是否没有可处理的 inbox 行。
		fields["empty"] = result.Empty
		// inserted_events 展示本轮实际写入 usage_events 的数量。
		fields["inserted_events"] = result.InsertedEvents
		// deduped_events 当前应保持 0，保留字段是为了兼容现有日志结构。
		fields["deduped_events"] = result.DedupedEvents
	}
	// 输出带上下文字段的错误日志，明确失败来自 Redis Process 批次。
	logrus.WithError(err).WithFields(fields).Error("redis drain batch failed")
}

// Status 汇总 Redis Pull 和 Redis Process 两个 runner 的共享运行状态。
func (d *RedisDrain) Status() Status {
	// 读取共享状态前加锁，避免看到 Pull/Process 写入中的中间状态。
	d.mu.Lock()
	// 函数返回时释放锁，保证所有字段读取在同一把锁内完成。
	defer d.mu.Unlock()
	// 返回状态快照；SyncRunning 只要 Pull 或 Process 任一单轮正在执行就为 true。
	return Status{
		Running:     d.running,                         // Running 表示 Redis 后台 runner 是否已启动。
		LastRunAt:   d.lastRunAt,                       // LastRunAt 记录最后一次 Pull 或 Process 完成状态写入的时间。
		LastError:   d.combinedLastError(),             // LastError 同时保留 Pull 和 Process 两条独立链路的最新失败。
		LastWarning: d.lastProcessWarning,              // LastWarning 记录 Process completed_with_warnings 这类非阻断问题。
		LastStatus:  d.lastStatus,                      // LastStatus 记录最后一次 Pull 或 Process 的状态字符串。
		SyncRunning: d.pullRunning || d.processRunning, // SyncRunning 表示当前是否有 Pull 或 Process 单轮正在执行。
	}
}

func (d *RedisDrain) combinedLastError() string {
	if d.lastPullError != "" && d.lastProcessError != "" {
		return d.lastPullError + "; " + d.lastProcessError
	}
	if d.lastProcessError != "" {
		return d.lastProcessError
	}
	return d.lastPullError
}

// SyncNow 是手动同步入口：Redis 模式下先 Pull 一次再 Process 一次，保持用户手动触发时立即看到新数据的直觉。
func (d *RedisDrain) SyncNow(ctx context.Context) error {
	// 手动同步也复用同一套校验，确保不会绕过后台 runner 的配置要求。
	if err := d.validate(); err != nil {
		// 校验失败时直接返回，避免执行半套 Pull/Process。
		return err
	}
	// 手动同步第一步先拉远端 Redis，把新消息持久化到 redis_usage_inboxes。
	if _, err := d.pullRedisInboxOnce(ctx); err != nil {
		// Pull 失败时不继续 Process，避免用户误以为刚拉到的数据已经处理。
		return err
	}
	// 手动同步第二步处理本地 inbox，把可处理行写入 usage_events 并触发增量统计。
	_, err := d.processRedisInboxOnce(ctx)
	// 返回 Process 结果；completed_with_warnings 会通过 ErrSyncCompletedWithWarnings 表达。
	return err
}

// pullRedisInboxOnce 只防止 Pull 自身重入，不阻塞 Process；这样 Redis 长轮询或退避不会跳过本地 inbox 处理周期。
func (d *RedisDrain) pullRedisInboxOnce(ctx context.Context) (*servicedto.RedisInboxPullResult, error) {
	// 加锁读取和修改 pullRunning，保证同一个 Pull runner 不会在慢 Redis 调用期间重入。
	d.mu.Lock()
	// 如果上一轮 Pull 尚未结束，直接返回重入错误，避免同一队列被两个 Pull 同时消费。
	if d.pullRunning {
		// 返回前释放互斥锁，避免阻塞 status 查询和 process 状态更新。
		d.mu.Unlock()
		// 用统一的 ErrSyncAlreadyRunning 表示本轮被重入保护跳过。
		return nil, ErrSyncAlreadyRunning
	}
	// 标记 Pull 正在运行，status 的 SyncRunning 会因此变成 true。
	d.pullRunning = true
	// 完成运行标记后立即释放锁，真正的 Redis 操作不持有状态锁。
	d.mu.Unlock()

	// 无论 Pull 成功、失败还是被 ctx 取消，都必须清理 pullRunning。
	defer func() {
		// 清理运行标记前重新加锁，保证和 status 查询互斥。
		d.mu.Lock()
		// 清除 Pull 运行标记，让下一轮 Pull 可以继续执行。
		d.pullRunning = false
		// 释放状态锁，结束本轮 Pull 的重入保护范围。
		d.mu.Unlock()
	}()

	// 调用 service 层 PullRedisUsageInbox，把远端 Redis 消息原样持久化到本地 inbox。
	result, err := d.syncer.PullRedisUsageInbox(ctx)
	// 将本轮 Pull 的状态、错误和时间写入共享状态，供 /status 展示。
	d.recordRedisPullResult(result, err)
	// 返回 service 层结果，让 loop 决定下一步是 idle sleep 还是 error backoff。
	return result, err
}

// processRedisInboxOnce 只防止 Process 自身重入，不阻塞 Pull；Process 的输入必须来自已持久化的 redis_usage_inboxes。
func (d *RedisDrain) processRedisInboxOnce(ctx context.Context) (*servicedto.RedisBatchSyncResult, error) {
	// 加锁读取和修改 processRunning，保证同一个 Process runner 不会在慢 SQLite 写入期间重入。
	d.mu.Lock()
	// 如果上一轮 Process 尚未结束，直接返回重入错误，避免两个 SQLite 写事务同时处理 inbox。
	if d.processRunning {
		// 返回前释放互斥锁，避免阻塞 status 查询和 pull 状态更新。
		d.mu.Unlock()
		// 用统一的 ErrSyncAlreadyRunning 表示本轮被重入保护跳过。
		return nil, ErrSyncAlreadyRunning
	}
	// 标记 Process 正在运行，status 的 SyncRunning 会因此变成 true。
	d.processRunning = true
	// 完成运行标记后立即释放锁，真正的 SQLite 处理不持有状态锁。
	d.mu.Unlock()

	// 无论 Process 成功、失败还是被 ctx 取消，都必须清理 processRunning。
	defer func() {
		// 清理运行标记前重新加锁，保证和 status 查询互斥。
		d.mu.Lock()
		// 清除 Process 运行标记，让下一轮 Process 可以继续执行。
		d.processRunning = false
		// 释放状态锁，结束本轮 Process 的重入保护范围。
		d.mu.Unlock()
	}()

	// 调用 service 层 ProcessRedisUsageInbox，把本地 inbox 转成 usage_events 并触发后续增量聚合。
	result, err := d.syncer.ProcessRedisUsageInbox(ctx)
	// 默认把 service 层错误原样返回给上层 loop。
	returnErr := err
	// 如果 service 已经把坏消息隔离成非 failed 状态，外层只需要按 warning 记录，不应中断 Process runner。
	if err != nil && result != nil && result.Status != "" && result.Status != "failed" {
		// 包装成 ErrSyncCompletedWithWarnings，让 loop 可以区分“有坏消息”与“整批失败”。
		returnErr = fmt.Errorf("%w: %v", ErrSyncCompletedWithWarnings, err)
	}
	// 将本轮 Process 的状态、错误和时间写入共享状态，供 /status 展示。
	d.recordRedisProcessResult(result, err)
	// 返回原始结果和语义化错误，让 loop 决定是否输出失败日志。
	return result, returnErr
}

// recordRedisPullResult 把单轮 Pull 的可观测状态写入 RedisDrain，供 /status 汇总展示。
func (d *RedisDrain) recordRedisPullResult(result *servicedto.RedisInboxPullResult, err error) {
	// 状态字段由 Pull 和 Process 两个 runner 共享，写入前必须加锁。
	d.mu.Lock()
	// 函数返回时释放锁，保证下面的状态更新是一个原子片段。
	defer d.mu.Unlock()
	// lastRunAt 使用项目存储时间，避免 status 和数据库时间语义不一致。
	d.lastRunAt = timeutil.NormalizeStorageTime(d.now())
	status := ""
	// 如果 service 层返回了状态，优先使用 service 层状态。
	if result != nil {
		// Pull 结果状态通常是 empty 或 completed，用于反映远端队列是否有数据。
		status = result.Status
	}
	// service 没给状态且没有错误时，兜底记录为 completed。
	if status == "" && err == nil {
		// completed 表示本轮 Pull 正常完成，但不代表一定拉到了消息。
		status = "completed"
	}
	// 写入最新状态，让 /status 能看到最后一次 Redis 后台动作的结果。
	d.lastStatus = status
	// Pull 只维护 Pull 自己的错误状态，不能清理 Process 留下的处理失败。
	d.lastPullError = ""
	if err != nil {
		// lastPullError 只保存文本，避免把 error 对象跨 goroutine 持久保存。
		d.lastPullError = err.Error()
	}
}

// recordRedisProcessResult 把单轮 Process 的可观测状态写入 RedisDrain，供 /status 汇总展示。
func (d *RedisDrain) recordRedisProcessResult(result *servicedto.RedisBatchSyncResult, err error) {
	// 状态字段由 Pull 和 Process 两个 runner 共享，写入前必须加锁。
	d.mu.Lock()
	// 函数返回时释放锁，保证下面的状态更新是一个原子片段。
	defer d.mu.Unlock()
	// lastRunAt 使用项目存储时间，避免 status 和数据库时间语义不一致。
	d.lastRunAt = timeutil.NormalizeStorageTime(d.now())
	status := ""
	// 如果 service 层返回了状态，优先使用 service 层状态。
	if result != nil {
		// Process 结果状态会区分 empty、completed、failed 和 completed_with_warnings。
		status = result.Status
	}
	// service 没给状态且没有错误时，兜底记录为 completed。
	if status == "" && err == nil {
		// completed 表示本轮 Process 正常完成，但不代表一定处理了 inbox 行。
		status = "completed"
	}
	// 写入最新状态，让 /status 能看到最后一次 Redis 后台动作的结果。
	d.lastStatus = status
	// Process 只维护 Process 自己的错误和 warning，不能清理 Pull 留下的远端拉取失败。
	d.lastProcessError = ""
	d.lastProcessWarning = ""
	// 如果 Process 返回错误，需要根据批次状态区分 warning 和 error。
	if err != nil {
		// 非 failed 状态说明坏消息已隔离，整体处理仍可继续，因此记录为 warning。
		if result != nil && result.Status != "" && result.Status != "failed" {
			// warning 展示给 status，但不会让 runner 当作整批失败反复报错。
			d.lastProcessWarning = err.Error()
		} else {
			// failed 或无结果说明本轮 Process 失败，记录为 lastProcessError。
			d.lastProcessError = err.Error()
		}
	}
}

// setRunning 用引用计数维护 Redis 后台任务整体运行状态，避免 split runner 互相清掉 Running。
func (d *RedisDrain) setRunning(running bool) {
	// runningRefs 是 Pull/Process runner 共享计数，写入前必须加锁。
	d.mu.Lock()
	// 函数退出时释放锁，避免调用方忘记解锁。
	defer d.mu.Unlock()
	if running {
		// 每个独立 runner 启动都增加一个引用，只要引用数大于 0，status 就保持 Running=true。
		d.runningRefs++
	} else if d.runningRefs > 0 {
		// runner 退出只释放自己的引用，不影响另一个仍在运行的 runner。
		d.runningRefs--
	}
	// 派生字段保留给 status 读取，兼容旧组合式 runner 和新 split runner。
	d.running = d.runningRefs > 0
}

// validate 检查 RedisDrain 的依赖和调度配置，并补齐测试可替换的默认函数。
func (d *RedisDrain) validate() error {
	// nil drain 说明 runner 构造链路有缺失，必须在进入后台循环前失败。
	if d == nil {
		// 返回明确错误，避免后续访问字段时 panic。
		return fmt.Errorf("redis drain is nil")
	}
	// syncer 是 Pull/Process 调用 service 层的唯一依赖，缺失时不能启动。
	if d.syncer == nil {
		// 返回明确错误，定位 NewRedisDrain 的调用方没有注入 sync service。
		return fmt.Errorf("redis drain syncer is nil")
	}
	// Pull loop 的 idle interval 必须大于 0，否则成功路径会形成紧密循环。
	if d.config.IdleInterval <= 0 {
		// 返回配置错误，要求调用方提供正数间隔。
		return fmt.Errorf("redis drain idle interval must be greater than zero")
	}
	// ErrorBackoff 必须大于 0，否则失败路径会形成紧密重试循环。
	if d.config.ErrorBackoff <= 0 {
		// 返回配置错误，要求调用方提供正数退避时间。
		return fmt.Errorf("redis drain error backoff must be greater than zero")
	}
	// now 允许测试替换；生产路径没注入时使用 time.Now。
	if d.now == nil {
		// 补齐默认时间函数，后续状态记录统一通过 d.now。
		d.now = time.Now
	}
	// sleep 允许测试替换；生产路径没注入时使用支持 ctx 取消的 sleepContext。
	if d.sleep == nil {
		// 补齐默认 sleep 函数，后续 loop 等待统一可被 ctx 打断。
		d.sleep = sleepContext
	}
	// 所有依赖和配置都有效，可以进入 Pull 或 Process loop。
	return nil
}

// sleepContext 等待指定时长或 ctx 取消，返回值告诉 loop 是否应该继续下一轮。
func sleepContext(ctx context.Context, d time.Duration) bool {
	// 每次等待创建独立 timer，避免 time.After 在长生命周期 loop 中产生不可控资源持有。
	timer := time.NewTimer(d)
	// 函数退出时停止 timer，避免未触发的 timer 继续占用资源。
	defer timer.Stop()
	// 同时等待 ctx 取消和 timer 到期，让后台任务可以快速停机。
	select {
	case <-ctx.Done():
		// ctx 取消表示 App 正在停机，调用方应退出 loop。
		return false
	case <-timer.C:
		// timer 到期表示间隔正常结束，调用方可以继续下一轮。
		return true
	}
}
