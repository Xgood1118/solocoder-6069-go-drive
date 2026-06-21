package job

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"go-drive/common"
	"go-drive/common/registry"
	"go-drive/common/task"
	"go-drive/common/types"
	"go-drive/storage"
	"log"
	"os"
	"path/filepath"
	"time"
)

const (
	retentionDays     = 90
	archiveDirName    = "job_archives"
	defaultTruncateLen = 200
)

type JobHistoryService struct {
	ch              *registry.ComponentsHolder
	config          common.Config
	runner          task.Runner
	jobHistoryDAO   *storage.JobHistoryDAO
	jobRetryConfigDAO *storage.JobRetryConfigDAO
	jobExecutor     *JobExecutor
	archiveDir      string
}

func NewJobHistoryService(
	ch *registry.ComponentsHolder,
	config common.Config,
	runner task.Runner,
	jobHistoryDAO *storage.JobHistoryDAO,
	jobRetryConfigDAO *storage.JobRetryConfigDAO,
	jobExecutor *JobExecutor,
) (*JobHistoryService, error) {
	archiveDir, e := config.GetDir(archiveDirName, true)
	if e != nil {
		return nil, fmt.Errorf("failed to create archive directory: %w", e)
	}

	s := &JobHistoryService{
		ch:                ch,
		config:            config,
		runner:            runner,
		jobHistoryDAO:     jobHistoryDAO,
		jobRetryConfigDAO: jobRetryConfigDAO,
		jobExecutor:       jobExecutor,
		archiveDir:        archiveDir,
	}

	ch.Add(registry.KeyJobHistoryService, s)

	go s.startBackgroundTasks()

	return s, nil
}

func (s *JobHistoryService) startBackgroundTasks() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		go s.processRetention()
		go s.processPendingRetries()
	}
}

func (s *JobHistoryService) processRetention() {
	cutoffDate := time.Now().AddDate(0, 0, -retentionDays).UnixMilli()

	histories, _, e := s.jobHistoryDAO.GetHistoriesByDateRange(0, 0, cutoffDate, 1, 1000)
	if e != nil {
		log.Printf("[JobHistoryService] failed to get old histories: %v", e)
		return
	}

	if len(histories) == 0 {
		return
	}

	dates := make(map[string][]types.JobHistory)
	for _, h := range histories {
		date := time.UnixMilli(h.StartedAt).Format("2006-01-02")
		dates[date] = append(dates[date], h)
	}

	for date, dateHistories := range dates {
		if e := s.archiveDateHistories(date, dateHistories); e != nil {
			log.Printf("[JobHistoryService] failed to archive date %s: %v", date, e)
			continue
		}
	}

	affected, e := s.jobHistoryDAO.ArchiveOldHistories(cutoffDate)
	if e != nil {
		log.Printf("[JobHistoryService] failed to mark histories as archived: %v", e)
		return
	}
	log.Printf("[JobHistoryService] archived %d histories", affected)
}

func (s *JobHistoryService) archiveDateHistories(date string, histories []types.JobHistory) error {
	filename := filepath.Join(s.archiveDir, fmt.Sprintf("job_histories_%s.json.gz", date))

	if _, e := os.Stat(filename); e == nil {
		return nil
	}

	tempFile := filename + ".tmp"
	f, e := os.Create(tempFile)
	if e != nil {
		return e
	}
	defer f.Close()

	gzWriter := gzip.NewWriter(f)
	defer gzWriter.Close()

	encoder := json.NewEncoder(gzWriter)
	for _, h := range histories {
		logs, e := s.jobHistoryDAO.GetEventLogs(h.ID)
		if e != nil {
			log.Printf("[JobHistoryService] failed to get event logs for history %d: %v", h.ID, e)
		}

		record := struct {
			History   types.JobHistory   `json:"history"`
			EventLogs []types.JobEventLog `json:"eventLogs"`
		}{
			History:   h,
			EventLogs: logs,
		}

		if e := encoder.Encode(record); e != nil {
			return e
		}
	}

	if e := gzWriter.Close(); e != nil {
		return e
	}
	if e := f.Close(); e != nil {
		return e
	}

	return os.Rename(tempFile, filename)
}

func (s *JobHistoryService) processPendingRetries() {
	now := time.Now().UnixMilli()

	pendingRetries, e := s.jobRetryConfigDAO.GetPendingRetries(now)
	if e != nil {
		log.Printf("[JobHistoryService] failed to get pending retries: %v", e)
		return
	}

	for _, retryConfig := range pendingRetries {
		go s.executeRetry(retryConfig)
	}
}

func (s *JobHistoryService) executeRetry(config types.JobRetryConfig) {
	job, e := s.jobExecutor.jobDAO.GetJob(config.JobId)
	if e != nil {
		log.Printf("[JobHistoryService] failed to get job %d for retry: %v", config.JobId, e)
		return
	}

	history := &types.JobHistory{
		JobId:          job.ID,
		JobDescription: job.Description,
		TriggerSource:  types.TriggerSourceRetry,
		Status:         types.JobHistoryStatusRunning,
		StartedAt:      types.NowMillis(),
		RetryCount:     config.RetryCount + 1,
	}

	if e := s.jobHistoryDAO.CreateHistory(history); e != nil {
		log.Printf("[JobHistoryService] failed to create retry history for job %d: %v", config.JobId, e)
		return
	}

	nextRetryAt := time.Now().Add(time.Duration(config.RetryIntervalMinutes) * time.Minute).UnixMilli()
	if e := s.jobRetryConfigDAO.UpdateNextRetryAt(config.JobId, nextRetryAt); e != nil {
		log.Printf("[JobHistoryService] failed to update next retry time for job %d: %v", config.JobId, e)
	}

	logFn := func(message string) {
		eventLog := &types.JobEventLog{
			HistoryId: history.ID,
			Timestamp: types.NowMillis(),
			Level:     "info",
			Message:   message,
		}
		if e := s.jobHistoryDAO.AddEventLog(eventLog); e != nil {
			log.Printf("[JobHistoryService] failed to add event log: %v", e)
		}
	}

	_, e = s.runner.Execute(func(ctx types.TaskCtx) (any, error) {
		e := s.jobExecutor.ExecuteJobSync(ctx, job, TriggerEvent{}, logFn)
		return nil, e
	}, task.WithNameGroup(job.Description, "job/retry"))

	completedAt := types.NowMillis()
	durationMs := completedAt - history.StartedAt

	if e != nil {
		errorSummary := e.Error()
		if len(errorSummary) > 512 {
			errorSummary = errorSummary[:512]
		}

		newRetryCount := config.RetryCount + 1
		if e := s.jobRetryConfigDAO.UpdateRetryCount(config.JobId, newRetryCount); e != nil {
			log.Printf("[JobHistoryService] failed to update retry count for job %d: %v", config.JobId, e)
		}

		if newRetryCount >= config.MaxRetries {
			if e := s.jobHistoryDAO.MarkAsDeadLetter(history.ID, e.Error()); e != nil {
				log.Printf("[JobHistoryService] failed to mark history as dead letter: %v", e)
			}
			if e := s.jobRetryConfigDAO.DeleteByJobId(config.JobId); e != nil {
				log.Printf("[JobHistoryService] failed to delete retry config for job %d: %v", config.JobId, e)
			}
		}

		if e := s.jobHistoryDAO.UpdateHistoryStatus(
			history.ID,
			types.JobHistoryStatusFailed,
			completedAt,
			durationMs,
			errorSummary,
			e.Error(),
		); e != nil {
			log.Printf("[JobHistoryService] failed to update history status: %v", e)
		}
	} else {
		if e := s.jobHistoryDAO.UpdateHistoryStatus(
			history.ID,
			types.JobHistoryStatusSuccess,
			completedAt,
			durationMs,
			"",
			"",
		); e != nil {
			log.Printf("[JobHistoryService] failed to update history status: %v", e)
		}

		if e := s.jobRetryConfigDAO.DeleteByJobId(config.JobId); e != nil {
			log.Printf("[JobHistoryService] failed to delete retry config for job %d: %v", config.JobId, e)
		}
	}
}

func (s *JobHistoryService) ManualRetry(historyId uint) (*types.JobHistory, error) {
	oldHistory, e := s.jobHistoryDAO.GetHistoryById(historyId)
	if e != nil {
		return nil, e
	}

	job, e := s.jobExecutor.jobDAO.GetJob(oldHistory.JobId)
	if e != nil {
		return nil, e
	}

	newHistory := &types.JobHistory{
		JobId:          job.ID,
		JobDescription: job.Description,
		ExecutionId:    oldHistory.ExecutionId,
		TriggerSource:  types.TriggerSourceManual,
		TriggerData:    oldHistory.TriggerData,
		Status:         types.JobHistoryStatusRunning,
		StartedAt:      types.NowMillis(),
	}

	createdHistory, e := s.jobHistoryDAO.RetryJob(historyId, newHistory)
	if e != nil {
		return nil, e
	}

	go func() {
		logFn := func(message string) {
			eventLog := &types.JobEventLog{
				HistoryId: createdHistory.ID,
				Timestamp: types.NowMillis(),
				Level:     "info",
				Message:   message,
			}
			if e := s.jobHistoryDAO.AddEventLog(eventLog); e != nil {
				log.Printf("[JobHistoryService] failed to add event log: %v", e)
			}
		}

		_, execErr := s.runner.Execute(func(ctx types.TaskCtx) (any, error) {
			execErr := s.jobExecutor.ExecuteJobSync(ctx, job, TriggerEvent{}, logFn)
			return nil, execErr
		}, task.WithNameGroup(job.Description, "job/manual-retry"))

		completedAt := types.NowMillis()
		durationMs := completedAt - createdHistory.StartedAt

		if execErr != nil {
			errorSummary := execErr.Error()
			if len(errorSummary) > 512 {
				errorSummary = errorSummary[:512]
			}
			_ = s.jobHistoryDAO.UpdateHistoryStatus(
				createdHistory.ID,
				types.JobHistoryStatusFailed,
				completedAt,
				durationMs,
				errorSummary,
				execErr.Error(),
			)
		} else {
			_ = s.jobHistoryDAO.UpdateHistoryStatus(
				createdHistory.ID,
				types.JobHistoryStatusSuccess,
				completedAt,
				durationMs,
				"",
				"",
			)
		}
	}()

	return createdHistory, nil
}

func (s *JobHistoryService) GetEventLogs(historyId uint, truncateLen int) ([]types.JobEventLog, error) {
	logs, e := s.jobHistoryDAO.GetEventLogs(historyId)
	if e != nil {
		return nil, e
	}

	if truncateLen <= 0 {
		truncateLen = defaultTruncateLen
	}

	for i := range logs {
		if truncateLen < len(logs[i].Message) {
			logs[i].MessageFull = logs[i].Message
			logs[i].Message = logs[i].Message[:truncateLen] + "..."
			logs[i].IsExpanded = false
		} else {
			logs[i].IsExpanded = true
		}
	}

	return logs, nil
}

func (s *JobHistoryService) ExpandEventLog(logId uint, historyId uint) (*types.JobEventLog, error) {
	logs, e := s.jobHistoryDAO.GetEventLogs(historyId)
	if e != nil {
		return nil, e
	}

	for i := range logs {
		if logs[i].ID == logId {
			logs[i].ExpandMessage()
			return &logs[i], nil
		}
	}

	return nil, fmt.Errorf("event log not found: %d", logId)
}

func (s *JobHistoryService) GetHistoryById(id uint) (types.JobHistory, error) {
	return s.jobHistoryDAO.GetHistoryById(id)
}

func (s *JobHistoryService) GetHistory(jobId uint, page, pageSize int) ([]types.JobHistory, int64, error) {
	return s.jobHistoryDAO.GetHistoryByJob(jobId, page, pageSize)
}

func (s *JobHistoryService) GetDeadLetterJobs(page, pageSize int) ([]types.JobHistory, int64, error) {
	return s.jobHistoryDAO.GetDeadLetterJobs(page, pageSize)
}

func (s *JobHistoryService) CreateHistory(history *types.JobHistory) error {
	return s.jobHistoryDAO.CreateHistory(history)
}

func (s *JobHistoryService) UpdateHistoryStatus(id uint, status string, completedAt int64, durationMs int64, errorSummary string, errorDetail string) error {
	return s.jobHistoryDAO.UpdateHistoryStatus(id, status, completedAt, durationMs, errorSummary, errorDetail)
}

func (s *JobHistoryService) AddEventLog(log *types.JobEventLog) error {
	return s.jobHistoryDAO.AddEventLog(log)
}

func (s *JobHistoryService) GetRetryConfig(jobId uint) (types.JobRetryConfig, error) {
	return s.jobRetryConfigDAO.GetByJobId(jobId)
}

func (s *JobHistoryService) SaveRetryConfig(config types.JobRetryConfig) (types.JobRetryConfig, error) {
	if config.RetryIntervalMinutes <= 0 {
		config.RetryIntervalMinutes = 5
	}
	if config.MaxRetries <= 0 {
		config.MaxRetries = 3
	}
	return s.jobRetryConfigDAO.Save(config)
}

func (s *JobHistoryService) DeleteRetryConfig(jobId uint) error {
	return s.jobRetryConfigDAO.DeleteByJobId(jobId)
}

func (s *JobHistoryService) Dispose() error {
	return nil
}
