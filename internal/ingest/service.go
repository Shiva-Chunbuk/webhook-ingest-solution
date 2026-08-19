// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger

	workerCtx  context.Context
	stopWorker context.CancelFunc
	workMu     sync.Mutex
	workClosed bool
	workDone   chan struct{}
	workWG     sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	workerCtx, stopWorker := context.WithCancel(context.Background())
	return &Service{
		store: s, cache: c, rdb: rdb, log: log,
		workerCtx: workerCtx, stopWorker: stopWorker,
	}
}

// Start restores cached totals and resumes durable recording work.
func (s *Service) Start(ctx context.Context) error {
	totals, err := s.store.AllAccountStats(ctx)
	if err != nil {
		return err
	}
	for accountID, total := range totals {
		s.cache.Set(accountID, stats.AccountStats{
			CallCount:        total.CallCount,
			TotalDurationSec: total.TotalDurationSec,
		})
	}

	callIDs, err := s.store.PendingRecordingCallIDs(ctx)
	if err != nil {
		return err
	}
	for _, callID := range callIDs {
		s.queueRecording(callID)
	}
	return nil
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}
	inserted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		s.queueRecording(rec.CallID)
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, callID string) error {
	timer := time.NewTimer(recordingWork)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	return s.store.MarkRecordingProcessed(ctx, callID)
}

func (s *Service) queueRecording(callID string) {
	s.workMu.Lock()
	defer s.workMu.Unlock()
	if s.workClosed {
		return
	}

	s.workWG.Add(1)
	go func() {
		defer s.workWG.Done()
		if err := s.processRecording(s.workerCtx, callID); err != nil &&
			!errors.Is(err, context.Canceled) {
			s.log.Error("recording processing failed", "call_id", callID, "err", err)
		}
	}()
}

// Shutdown waits for accepted recording work, cancelling it at the deadline.
// Unfinished rows remain pending in Postgres and are resumed by Start.
func (s *Service) Shutdown(ctx context.Context) error {
	s.workMu.Lock()
	if !s.workClosed {
		s.workClosed = true
		s.workDone = make(chan struct{})
		go func() {
			s.workWG.Wait()
			close(s.workDone)
		}()
	}
	done := s.workDone
	s.workMu.Unlock()

	select {
	case <-done:
		s.stopWorker()
		return nil
	case <-ctx.Done():
		s.stopWorker()
		return ctx.Err()
	}
}
