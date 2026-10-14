package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"
)

// ErrWriterClosed is returned when a message job is submitted after the write
// queue has been closed (shutdown). The message did not persist.
var ErrWriterClosed = errors.New("database: writer queue closed")

// writeQueueCapacity bounds the queued message jobs; senders block when the
// queue is full, which is the backpressure the single writer connection needs
// (Rust's own write queue is bounded the same way).
const writeQueueCapacity = 1024

// afterBatchLimit bounds one after-phase transaction: at most this many jobs'
// after-commit pairs (search row + unread bump) share a commit. It caps the
// statements and WAL frames a single after transaction writes and the time
// the writer holds the lane for one chunk, while keeping a burst's commit
// count near two: a full queue-capacity batch (writeQueueCapacity jobs) needs
// four after transactions, and any batch at or below this bound needs one.
// Larger batches split into several after transactions.
const afterBatchLimit = 256

// writerBatchObserver, when non-nil, is captured by newMessageWriter and
// called with each primary batch's size and its after-phase transaction
// count. It is a test hook, set before Open (like fastWriteRecord), for the
// batch-size histogram and the commits-per-message contract.
var writerBatchObserver func(primary, afterCommits int)

// messageWriter serializes message creation onto the one write connection.
//
// The lane is adaptive (ENGINE-45): a job submitted while the queue is empty
// and no batch is in flight runs in-line on the caller's goroutine, in its
// own transaction — no channel hop, no extra scheduling latency, one commit
// (the same shape as the direct CAMPFIRE_WRITE_QUEUE=off path). Any job that
// arrives while a write is in flight is queued instead, and the loop commits
// every job it drains in a single transaction: group commit. The busy mark
// is set and cleared in the same critical section that takes a job (submit
// for in-line, the loop for batches), and in-line claims additionally
// require an empty queue, so a later write can never overtake an earlier
// one: ids ascend with the order the jobs are taken.
//
// On the queued path each job runs inside its own savepoint, so a job that
// fails rolls back exactly its own statements while the rest of the batch
// still commits; the commit itself is one per batch. After the shared commit
// the writer runs a second phase of its own: the jobs' after-commit work
// (search index, unread bump) in bounded transactions, each job inside its
// own savepoint when the transaction holds more than one, so one failed
// after-job rolls back only its own statements while the rest commit. A batch
// of B messages therefore costs the primary commit plus the after phase's
// transactions — two when the batch fits one after transaction — instead of
// the B+1 commits the per-caller shape paid. The in-line path folds the
// after work into the job's own transaction, so a lone post commits
// atomically; its after-commit-failure behaviour is the direct path's (the
// write rolls back with the failing statement), not the queued path's.
//
// A writer is not safe for direct use; all jobs go through submit, and Close
// must be called before the underlying connection closes.
type messageWriter struct {
	conn laneConn
	jobs chan *messageJob
	wake chan struct{}
	stop chan struct{}
	done chan struct{}
	once sync.Once
	// observe, when non-nil (tests), receives each primary batch's size and
	// the number of after-phase transactions it needed. It is captured at
	// construction from writerBatchObserver, so a test sets that before Open.
	observe func(primary, afterCommits int)
	// mu guards closed, inflight and busy. closed rejects submissions after
	// Close; inflight counts submissions that passed the closed check and
	// are still between their mutex unlock and the channel send, so Close's
	// drain knows when no send can land any more; busy marks the writer as
	// executing — a batch on the loop or an in-line job on a caller — and is
	// written under mu together with the job that made the writer busy.
	mu       sync.Mutex
	closed   bool
	inflight int
	busy     bool
}

type messageJob struct {
	ctx   context.Context
	run   func(jobTx) (Message, error)
	after func(context.Context, jobTx, Message) error
	done  chan messageResult
}

type messageResult struct {
	message Message
	err     error
	// afterErr is the queued path's after-commit failure: the primary commit
	// landed and the message is durable, but its search row / unread bump did
	// not. The caller reports it the way Rails raises from the save that
	// committed — the message stays. It is always nil on the direct and
	// in-line paths, whose after work commits with the row.
	afterErr error
}

func newMessageWriter(conn laneConn) *messageWriter {
	w := &messageWriter{
		conn:    conn,
		jobs:    make(chan *messageJob, writeQueueCapacity),
		wake:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		observe: writerBatchObserver,
	}
	go w.loop()
	return w
}

func (w *messageWriter) submit(job *messageJob) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrWriterClosed
	}
	w.inflight++
	if !w.busy && len(w.jobs) == 0 {
		// In-line: the lane is idle, so run the job here instead of waking
		// the loop. The busy mark keeps writers that arrive while this one
		// runs on the queue, where the group commit can batch them; the
		// single connection serializes the transaction against any batch
		// the loop takes in between.
		w.busy = true
		w.mu.Unlock()
		w.runInline(job)
		w.mu.Lock()
		w.inflight--
		w.busy = false
		w.mu.Unlock()
		return nil
	}
	select {
	case w.jobs <- job:
		w.mu.Unlock()
	default:
		// The queue is full: drop the mutex before the blocking send so
		// Close and in-line claims are never held up by backpressure.
		w.mu.Unlock()
		w.jobs <- job
	}
	// Kick the loop; it re-probes the channel rather than consuming the
	// wake, so a job is never taken by the signal itself. The buffered
	// channel coalesces kicks while the loop is already awake.
	select {
	case w.wake <- struct{}{}:
	default:
	}
	w.mu.Lock()
	w.inflight--
	w.mu.Unlock()
	return nil
}

// Close stops the loop after it has drained the queue: every job submitted
// before Close ran to completion (submitted after returns ErrWriterClosed).
// Jobs running in-line on their callers are part of the inflight count, so
// Close waits for them too. Close blocks until the queue is empty and the
// loop has exited. It is idempotent.
func (w *messageWriter) Close() {
	w.once.Do(func() {
		w.mu.Lock()
		w.closed = true
		w.mu.Unlock()
		close(w.stop)
		<-w.done
	})
}

func (w *messageWriter) loop() {
	defer close(w.done)
	for {
		job, stop := w.nextJob()
		if stop {
			w.drain()
			return
		}
		w.runBatch(job)
		w.mu.Lock()
		w.busy = false
		w.mu.Unlock()
	}
}

// nextJob takes the next queued job, marking the writer busy in the same
// critical section that takes it, or reports the stop signal. Taking the job
// under the mutex closes the window in which an in-line claim could observe
// an idle writer with a job already claimed here (a later write overtaking
// an earlier one). When the queue is empty the loop sleeps on the wake
// channel; the wake is only a kick — the channel itself never carries jobs,
// so no job is consumed by the signal.
func (w *messageWriter) nextJob() (job *messageJob, stop bool) {
	for {
		select {
		case <-w.stop:
			return nil, true
		default:
		}
		w.mu.Lock()
		select {
		case job := <-w.jobs:
			w.busy = true
			w.mu.Unlock()
			return job, false
		default:
			w.mu.Unlock()
		}
		// Sleep without the mutex until a submit kicks us (or Close stops
		// us); the probe above is authoritative about what to take.
		select {
		case <-w.stop:
			return nil, true
		case <-w.wake:
		}
	}
}

// LaneBlocker holds the write lane busy until the returned release runs:
// message jobs submitted while it is held take the queued batch path instead
// of running in-line, which tests use to pin the queued path's behaviour
// deterministically. The blocker's own transaction commits nothing.
func (d *DB) LaneBlocker() func() {
	gate := make(chan struct{})
	var once sync.Once
	job := &messageJob{
		ctx:  context.Background(),
		run:  func(tx jobTx) (Message, error) { <-gate; return Message{}, nil },
		done: make(chan messageResult, 1),
	}
	go func() {
		if err := d.writer.submit(job); err != nil {
			panic(err)
		}
	}()
	return func() {
		once.Do(func() {
			close(gate)
			<-job.done
		})
	}
}

// runInline executes one job on the caller's goroutine: its own transaction,
// the after-commit statements inside the same transaction, one commit — the
// direct path's shape, without the CAMPFIRE_WRITE_QUEUE=off flag. The result
// is delivered only after the commit, so a caller's response can never
// precede its message's persistence. Cancellation behaves like the direct
// path: a cancelled context fails the transaction before anything runs, and
// the job's statements carry their own context. A panicking job fails this
// job's write (its transaction rolls back) instead of taking the caller
// down, like the batch path.
func (w *messageWriter) runInline(job *messageJob) {
	var result messageResult
	func() {
		defer func() {
			if p := recover(); p != nil {
				slog.Error("message write panicked", "error", p)
				result = messageResult{err: fmt.Errorf("database: message write panic: %v", p)}
			}
		}()
		tx, err := w.conn.begin(job.ctx)
		if err != nil {
			result = messageResult{err: err}
			return
		}
		defer tx.rollback()
		m, err := job.run(tx)
		if err == nil && job.after != nil {
			err = job.after(job.ctx, tx, m)
		}
		if err != nil {
			result = messageResult{err: err}
			return
		}
		if err := tx.commit(); err != nil {
			result = messageResult{err: err}
			return
		}
		result = messageResult{message: m}
	}()
	job.done <- result
}

// drain processes every job still queued; it is the shutdown path and waits
// for submissions that passed the closed check but had not sent yet (their
// count is inflight, and their send lands in the buffered channel). In-line
// jobs running on their callers also keep the count above zero until they
// finish.
func (w *messageWriter) drain() {
	for {
		select {
		case job := <-w.jobs:
			w.runBatch(job)
		default:
			w.mu.Lock()
			n := w.inflight
			w.mu.Unlock()
			if n == 0 {
				return
			}
			runtime.Gosched()
		}
	}
}

// runBatch commits the head job and everything queued behind it in one
// transaction, then runs the batch's after-commit work in the writer-owned
// second phase. Results are delivered only after both phases, so a caller's
// response can never precede its message's persistence or its after-commit
// work. A single-job batch runs directly in the transaction (a failure rolls
// back exactly its own statements, which is all there is); larger batches run
// each job inside its own savepoint, so one failing job rolls back only its
// own statements while the rest of the batch still commits.
func (w *messageWriter) runBatch(head *messageJob) {
	batch := []*messageJob{head}
	for {
		select {
		case job := <-w.jobs:
			batch = append(batch, job)
		default:
			goto run
		}
	}
run:
	results := make([]messageResult, len(batch))
	var commitErr error
	var tx laneTx
	func() {
		// A panicking job must not take the writer down with it; its batch is
		// rolled back and every job in it fails, like the legacy path.
		defer func() {
			if p := recover(); p != nil {
				slog.Error("message write batch panicked", "error", p)
				commitErr = fmt.Errorf("database: message write panic: %v", p)
				if tx != nil {
					tx.rollback()
				}
			}
		}()
		var err error
		tx, err = w.conn.begin(context.Background())
		if err != nil {
			commitErr = err
			return
		}
		if len(batch) == 1 {
			job := batch[0]
			if err := job.ctx.Err(); err != nil {
				// Cancelled before execution: nothing ran, nothing persists,
				// exactly like the legacy BeginTx failing on a cancelled ctx.
				tx.rollback()
				results[0] = messageResult{err: err}
			} else if m, err := job.run(tx); err != nil {
				tx.rollback()
				results[0] = messageResult{err: err}
			} else if err := tx.commit(); err != nil {
				results[0] = messageResult{err: err}
			} else {
				results[0] = messageResult{message: m}
			}
			return
		}
		for i, job := range batch {
			if err := job.ctx.Err(); err != nil {
				// Cancelled before execution: nothing ran, nothing persists,
				// exactly like the legacy BeginTx failing on a cancelled ctx.
				results[i] = messageResult{err: err}
				continue
			}
			if err := tx.savepoint(); err != nil {
				results[i] = messageResult{err: err}
				continue
			}
			m, err := job.run(tx)
			if err != nil {
				tx.rollbackTo()
				tx.releaseSavepoint()
				results[i] = messageResult{err: err}
				continue
			}
			if err := tx.releaseSavepoint(); err != nil {
				results[i] = messageResult{err: err}
				continue
			}
			results[i] = messageResult{message: m}
		}
		if err := tx.commit(); err != nil {
			commitErr = err
		}
	}()
	// The second phase runs only when the primary commit landed: a failed
	// commit persisted no row, so there is no committed message whose index
	// row should exist, and every successful job in the batch fails with the
	// commit error (the shape the caller's own after transaction never had the
	// chance to run).
	afterCommits := 0
	if commitErr == nil {
		afterCommits = w.runAfter(batch, results)
	}
	// Report the batch before delivering results, so an observer's record is
	// complete by the time every caller has returned (the histogram test reads
	// it after wg.Wait).
	if w.observe != nil {
		w.observe(len(batch), afterCommits)
	}
	for i, job := range batch {
		if result := results[i]; result.err == nil && commitErr != nil {
			// The commit failed: no statement of the batch persisted, so the
			// job's own savepoint work is gone with it.
			results[i] = messageResult{err: commitErr}
		}
		job.done <- results[i]
	}
}

// runAfter is the writer-owned second phase (ENGINE-45): the after-commit work
// of every job whose primary work committed, run in bounded transactions
// after the primary commit. It returns the number of after-phase transactions
// it committed (zero when the batch had no after work), which the batch
// observer reports. The work runs on the writer's goroutine and lane, so a
// later batch cannot overtake an earlier batch's after work; the after
// statements are exactly the job.after surface the caller used to run, so the
// two lanes and the in-line path keep the same SQL in the same order.
func (w *messageWriter) runAfter(batch []*messageJob, results []messageResult) int {
	idxs := make([]int, 0, len(batch))
	for i, job := range batch {
		if job.after != nil && results[i].err == nil {
			idxs = append(idxs, i)
		}
	}
	if len(idxs) == 0 {
		return 0
	}
	commits := 0
	for start := 0; start < len(idxs); start += afterBatchLimit {
		end := start + afterBatchLimit
		if end > len(idxs) {
			end = len(idxs)
		}
		w.runAfterChunk(batch, results, idxs[start:end])
		commits++
	}
	return commits
}

// runAfterChunk commits one bounded group of after-commit jobs. A single job
// runs directly in the transaction (its failure rolls back exactly its own
// statements); a larger group wraps each job in its own savepoint, so one
// failed after-job rolls back only its own statements while the rest of the
// group still commits, and a failed commit fails every job in the group that
// had not already failed. The statements run on a context derived from
// Background, not the request: the Rust after_commit hooks and Rails'
// after_commit callbacks are not request-cancellable, and the primary commit
// already landed, so a disconnecting client must not abort the index/unread
// work. The begin keeps Background (no deadline), matching the shape the
// caller used before the second phase was writer-owned.
func (w *messageWriter) runAfterChunk(batch []*messageJob, results []messageResult, idxs []int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var phaseErr error
	var commitErr error
	var tx laneTx
	func() {
		// A panicking after-job must not take the writer down with it: its
		// chunk is rolled back and every job in it reports the panic.
		defer func() {
			if p := recover(); p != nil {
				slog.Error("message after-commit panicked", "error", p)
				phaseErr = fmt.Errorf("database: message after-commit panic: %v", p)
				if tx != nil {
					tx.rollback()
				}
			}
		}()
		var err error
		tx, err = w.conn.begin(context.Background())
		if err != nil {
			phaseErr = err
			return
		}
		if len(idxs) == 1 {
			i := idxs[0]
			if err := batch[i].after(ctx, tx, results[i].message); err != nil {
				tx.rollback()
				results[i].afterErr = err
				return
			}
			if err := tx.commit(); err != nil {
				results[i].afterErr = err
			}
			return
		}
		for _, i := range idxs {
			if err := tx.savepoint(); err != nil {
				results[i].afterErr = err
				continue
			}
			if err := batch[i].after(ctx, tx, results[i].message); err != nil {
				tx.rollbackTo()
				tx.releaseSavepoint()
				results[i].afterErr = err
				continue
			}
			if err := tx.releaseSavepoint(); err != nil {
				results[i].afterErr = err
				continue
			}
		}
		if err := tx.commit(); err != nil {
			commitErr = err
		}
	}()
	if phaseErr == nil && commitErr == nil {
		return
	}
	for _, i := range idxs {
		if results[i].afterErr != nil {
			continue
		}
		if phaseErr != nil {
			results[i].afterErr = phaseErr
		} else {
			results[i].afterErr = commitErr
		}
	}
}

// checkpointer runs PASSIVE WAL checkpoints on its own connection, off the
// writer. The writer connection has PRAGMA wal_autocheckpoint=0, so no commit
// ever pays the checkpoint's WAL and database fsyncs; the checkpointer pays
// them on a schedule instead, exactly the division of labour the Rust port's
// checkpointer thread implements.
type checkpointer struct {
	conn     *sql.DB
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
}

func startCheckpointer(driver, dsn string, interval time.Duration) (*checkpointer, error) {
	conn, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	if err := conn.Ping(); err != nil {
		conn.Close()
		return nil, err
	}
	c := &checkpointer{conn: conn, interval: interval, stop: make(chan struct{}), done: make(chan struct{})}
	go c.loop()
	return c, nil
}

func (c *checkpointer) loop() {
	defer close(c.done)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.passive()
		}
	}
}

func (c *checkpointer) passive() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// PASSIVE never blocks: frames a reader is using are skipped and picked up
	// on the next tick. The row reports (busy, log pages, checkpointed pages).
	var busy, log, checked int
	if err := c.conn.QueryRowContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)").Scan(&busy, &log, &checked); err != nil {
		slog.Warn("WAL checkpoint failed", "error", err)
	}
}

func (c *checkpointer) Close() {
	c.once.Do(func() {
		close(c.stop)
		<-c.done
		c.passive() // one final checkpoint after the writer drained
		c.conn.Close()
	})
}
