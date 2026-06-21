package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	err "go-drive/common/errors"
	"go-drive/common/registry"
	"go-drive/common/task"
	"go-drive/common/types"
	"go-drive/storage"
	"log"
	"strings"
	"sync"
	"time"
)

type JobExecutor struct {
	ch                *registry.ComponentsHolder
	runner            task.Runner
	jobDAO            *storage.JobDAO
	jobHistoryDAO     *storage.JobHistoryDAO
	jobRetryConfigDAO *storage.JobRetryConfigDAO

	triggers   map[JobTriggerType]IJobTriggerInstance
	executions map[uint]*jobExecutionItem

	mu sync.Mutex
}

func NewJobExecutor(jobDAO *storage.JobDAO, jobHistoryDAO *storage.JobHistoryDAO, jobRetryConfigDAO *storage.JobRetryConfigDAO, ch *registry.ComponentsHolder) (*JobExecutor, error) {
	runner := ch.Get(registry.KeyTaskRunner).(task.Runner)

	executor := &JobExecutor{
		ch:                ch,
		runner:            runner,
		jobDAO:            jobDAO,
		jobHistoryDAO:     jobHistoryDAO,
		jobRetryConfigDAO: jobRetryConfigDAO,
		executions:        make(map[uint]*jobExecutionItem),
		triggers:          make(map[JobTriggerType]IJobTriggerInstance),
	}

	for _, triggerDef := range GetTriggerDefs() {
		executor.triggers[JobTriggerType(triggerDef.Name)] = triggerDef.Factory(executor, ch)
	}

	e := executor.ReloadJobs()
	if e != nil {
		return nil, e
	}

	_ = jobDAO.UpdateAllRunningJobExecutionsToFailed()

	ch.Add(registry.KeyJobExecutor, executor)
	return executor, nil
}

func (je *JobExecutor) ReloadJobs() error {
	je.mu.Lock()
	defer je.mu.Unlock()

	jobs, e := je.jobDAO.GetJobs(false)
	if e != nil {
		return e
	}

	for _, trigger := range je.triggers {
		trigger.Clear()
	}

	// Parse triggers and register them
	for _, job := range jobs {
		triggers, e := je.parseTriggers(job)
		if e != nil {
			log.Printf("error parsing triggers for job %d: %v", job.ID, e)
			continue
		}

		for _, trigger := range triggers {
			triggerInstance := je.triggers[trigger.Type]
			if triggerInstance == nil {
				continue
			}
			if e := triggerInstance.Register(job.ID, trigger.Config); e != nil {
				log.Printf("error registering trigger %s for job %d: %v", string(trigger.Type), job.ID, e)
				continue
			}
		}
	}

	return nil
}

func (je *JobExecutor) parseTriggers(job types.Job) ([]ParsedJobTrigger, error) {
	if job.Triggers == "" {
		return nil, fmt.Errorf("no triggers found for job %d", job.ID)
	}
	var triggers []ParsedJobTrigger
	if e := json.Unmarshal([]byte(job.Triggers), &triggers); e != nil {
		return nil, fmt.Errorf("failed to parse triggers for job %d: %w", job.ID, e)
	}
	return triggers, nil
}

// TriggerExecutionWithEvent runs the job using task.Runner with event information and returns the task
func (je *JobExecutor) TriggerExecution(jobID uint, event TriggerEvent) (task.Task, error) {
	job, e := je.jobDAO.GetJob(jobID)
	if e != nil {
		return task.Task{}, e
	}

	return je.runner.Execute(func(ctx types.TaskCtx) (any, error) {
		return nil, je.ExecuteJobSync(ctx, job, event, nil)
	}, task.WithNameGroup(job.Description, "job/execution"))
}

func (je *JobExecutor) ExecuteJobSync(ctx context.Context, job types.Job, event TriggerEvent, onLog func(string)) error {
	triggerSource := je.getTriggerSource(&event)
	return je.executeJobWithHistory(ctx, job, event, triggerSource, onLog, 0, 0)
}

func (je *JobExecutor) ExecuteJobWithSource(ctx context.Context, job types.Job, event TriggerEvent, triggerSource string, onLog func(string)) error {
	return je.executeJobWithHistory(ctx, job, event, triggerSource, onLog, 0, 0)
}

func (je *JobExecutor) executeJobWithHistory(ctx context.Context, job types.Job, event TriggerEvent,
	triggerSource string, onLog func(string), retryOf uint, retryCount int) error {

	history, e := je.newJobHistory(job, triggerSource, &event, retryOf, retryCount)
	if e != nil {
		log.Printf("[JobExecutor] failed to create job history: %v", e)
		return e
	}

	jobExecution, e := je.newJobExecution(job, history)
	if e != nil {
		je.updateHistoryStatus(history, types.JobHistoryStatusFailed, 0, e.Error(), e.Error())
		return e
	}

	logger := newJobExecutionLogger(jobExecution.ID, onLog)
	je.addEventLog(history.ID, "info", fmt.Sprintf("Job started: %s", job.Description))

	return je.executeJob(ctx, job, jobExecution, history, logger, &event)
}

func (je *JobExecutor) executeJob(ctx context.Context, job types.Job,
	jobExecution *types.JobExecution, history *types.JobHistory, logger *jobExecutionLogger, event *TriggerEvent) (e error) {
	executionCtx, cancel := context.WithCancel(ctx)
	item := &jobExecutionItem{JobExecution: jobExecution, jobHistory: history, cancel: cancel, logger: logger}
	je.addJobExecution(item)

	defer func() {
		je.updateJobExecutionResult(item, e)
	}()

	actionDef := GetActionDef(job.Action)
	if actionDef == nil {
		e = errors.New("job action not found: " + job.Action)
		je.addEventLog(history.ID, "error", e.Error())
		return
	}

	params := make(types.SM, 0)
	e = json.Unmarshal([]byte(job.ActionParams), &params)
	if e != nil {
		e = fmt.Errorf("failed to parse params: %s", e.Error())
		je.addEventLog(history.ID, "error", e.Error())
		return
	}

	// Merge event information into params if provided
	if event != nil {
		eventBytes, e := json.Marshal(event)
		if e == nil {
			params["$event"] = string(eventBytes)
		}
	}

	je.addEventLog(history.ID, "info", fmt.Sprintf("Executing action: %s", job.Action))

	e = actionDef.Do(executionCtx, params, je.ch, item.logger.Log)
	if e != nil {
		je.addEventLog(history.ID, "error", fmt.Sprintf("Job failed: %v", e))
	} else {
		je.addEventLog(history.ID, "info", "Job completed successfully")
	}
	return
}

func (je *JobExecutor) getTriggerSource(event *TriggerEvent) string {
	if event == nil {
		return types.TriggerSourceManual
	}
	switch event.Type {
	case JobTriggerTypeCron:
		return types.TriggerSourceCron
	case JobTriggerTypeEntry:
		return types.TriggerSourceEvent
	default:
		return types.TriggerSourceManual
	}
}

func (je *JobExecutor) newJobHistory(job types.Job, triggerSource string, event *TriggerEvent, retryOf uint, retryCount int) (*types.JobHistory, error) {
	now := types.NowMillis()
	triggerData := ""
	if event != nil && event.Data != nil {
		eventBytes, e := json.Marshal(event.Data)
		if e == nil {
			triggerData = string(eventBytes)
		}
	}

	history := &types.JobHistory{
		JobId:          job.ID,
		JobDescription: job.Description,
		TriggerSource:  triggerSource,
		TriggerData:    triggerData,
		Status:         types.JobHistoryStatusRunning,
		StartedAt:      now,
		RetryOf:        retryOf,
		RetryCount:     retryCount,
	}
	e := je.jobHistoryDAO.CreateHistory(history)
	return history, e
}

func (je *JobExecutor) addEventLog(historyId uint, level string, message string) {
	eventLog := &types.JobEventLog{
		HistoryId: historyId,
		Timestamp: types.NowMillis(),
		Level:     level,
		Message:   message,
	}
	if e := je.jobHistoryDAO.AddEventLog(eventLog); e != nil {
		log.Printf("[JobExecutor] failed to add event log: %v", e)
	}
}

func (je *JobExecutor) newJobExecution(job types.Job, history *types.JobHistory) (*types.JobExecution, error) {
	jobExecution := &types.JobExecution{
		JobId:     job.ID,
		StartedAt: uint64(time.Now().UnixMilli()),
		Status:    types.JobExecutionRunning,
	}
	e := je.jobDAO.AddJobExecution(jobExecution)
	if e != nil {
		return nil, e
	}

	if history != nil {
		history.ExecutionId = jobExecution.ID
	}

	return jobExecution, e
}

func (je *JobExecutor) updateHistoryStatus(history *types.JobHistory, status string, durationMs int64, errorSummary string, errorDetail string) {
	now := types.NowMillis()
	completedAt := int64(0)
	if status != types.JobHistoryStatusRunning {
		completedAt = now
	}
	if e := je.jobHistoryDAO.UpdateHistoryStatus(history.ID, status, completedAt, durationMs, errorSummary, errorDetail); e != nil {
		log.Printf("[JobExecutor] failed to update job history status: %v", e)
	}
}

func (je *JobExecutor) scheduleRetry(job types.Job, history *types.JobHistory, event TriggerEvent, lastError error) bool {
	retryConfig, e := je.jobRetryConfigDAO.GetByJobId(job.ID)
	if e != nil {
		return false
	}

	if !retryConfig.RetryEnabled {
		return false
	}

	if retryConfig.RetryCount >= retryConfig.MaxRetries {
		je.addEventLog(history.ID, "warn", fmt.Sprintf("Retry count exhausted (%d/%d), marking as dead letter", retryConfig.RetryCount, retryConfig.MaxRetries))
		if e := je.jobHistoryDAO.MarkAsDeadLetter(history.ID, lastError.Error()); e != nil {
			log.Printf("[JobExecutor] failed to mark as dead letter: %v", e)
		}
		return false
	}

	nextRetryAt := types.NowMillis() + int64(retryConfig.RetryIntervalMinutes)*60*1000
	nextRetryCount := retryConfig.RetryCount + 1

	if e := je.jobRetryConfigDAO.UpdateRetryCount(job.ID, nextRetryCount); e != nil {
		log.Printf("[JobExecutor] failed to update retry count: %v", e)
		return false
	}
	if e := je.jobRetryConfigDAO.UpdateNextRetryAt(job.ID, nextRetryAt); e != nil {
		log.Printf("[JobExecutor] failed to update next retry at: %v", e)
		return false
	}

	je.addEventLog(history.ID, "info", fmt.Sprintf("Scheduled retry %d/%d at %v", nextRetryCount, retryConfig.MaxRetries, time.UnixMilli(nextRetryAt)))

	go func() {
		waitDuration := time.Until(time.UnixMilli(nextRetryAt))
		if waitDuration > 0 {
			time.Sleep(waitDuration)
		}

		_, e := je.runner.Execute(func(ctx types.TaskCtx) (any, error) {
			return nil, je.executeJobWithHistory(ctx, job, event, types.TriggerSourceRetry, nil, history.ID, nextRetryCount)
		}, task.WithNameGroup(job.Description, "job/retry"))

		if e != nil {
			log.Printf("[JobExecutor] failed to execute retry: %v", e)
		}
	}()

	return true
}

func (je *JobExecutor) updateJobExecutionResult(item *jobExecutionItem, e error) {
	item.CompletedAt = uint64(time.Now().UnixMilli())
	durationMs := int64(item.CompletedAt) - int64(item.StartedAt)

	if e != nil {
		item.Status = types.JobExecutionFailed
		item.ErrorMsg = e.Error()
	} else {
		item.Status = types.JobExecutionSuccess
	}

	item.JobExecution.Logs = item.logger.String()
	if updateErr := je.jobDAO.UpdateJobExecution(item.JobExecution); updateErr != nil {
		log.Printf("[JobExecutor] failed to update job execution: %v", updateErr)
	}

	if item.jobHistory != nil {
		errorSummary := ""
		errorDetail := ""
		historyStatus := types.JobHistoryStatusSuccess

		if e != nil {
			errorSummary = e.Error()
			if len(errorSummary) > 512 {
				errorSummary = errorSummary[:512]
			}
			errorDetail = e.Error()
			historyStatus = types.JobHistoryStatusFailed
		}

		je.updateHistoryStatus(item.jobHistory, historyStatus, durationMs, errorSummary, errorDetail)

		if e != nil {
			job, getJobErr := je.jobDAO.GetJob(item.JobId)
			if getJobErr == nil {
				event := TriggerEvent{}
				if item.jobHistory.TriggerData != "" {
					_ = json.Unmarshal([]byte(item.jobHistory.TriggerData), &event.Data)
				}
				je.scheduleRetry(job, item.jobHistory, event, e)
			}
		}
	}

	item.cancel()
	je.removeJobExecution(item.ID)
}

// ValidateTriggers validates all triggers in a job
func (je *JobExecutor) ValidateTriggers(triggersJSON string) error {
	if triggersJSON == "" {
		return err.NewBadRequestError("triggers are required")
	}

	var triggers []ParsedJobTrigger
	if e := json.Unmarshal([]byte(triggersJSON), &triggers); e != nil {
		return err.NewBadRequestError("invalid triggers format: " + e.Error())
	}

	if len(triggers) == 0 {
		return err.NewBadRequestError("at least one trigger is required")
	}

	for _, trigger := range triggers {
		triggerDef := GetTriggerDef(trigger.Type)
		if triggerDef == nil {
			return err.NewBadRequestError("unknown trigger type: " + string(trigger.Type))
		}
		if e := triggerDef.Validate(trigger.Config); e != nil {
			return e
		}
	}

	return nil
}

func (je *JobExecutor) GetJobTriggersInfo(jobID uint) (map[JobTriggerType][]types.SM, error) {
	statsMap := make(map[JobTriggerType][]types.SM, len(je.triggers))
	for triggerType, trigger := range je.triggers {
		stats, e := trigger.GetInfo(jobID)
		if e != nil {
			return nil, e
		}
		if len(stats) == 0 {
			continue
		}
		statsMap[triggerType] = stats
	}
	return statsMap, nil
}

func (je *JobExecutor) CancelJobExecution(id uint) error {
	item := je.executions[id]
	if item != nil {
		item.cancel()
	}
	return nil
}

func (je *JobExecutor) IsJobExecutionRunning(id uint) bool {
	item := je.executions[id]
	if item == nil {
		return false
	}
	return item.Status == types.JobExecutionRunning
}

func (je *JobExecutor) addJobExecution(exec *jobExecutionItem) {
	je.mu.Lock()
	defer je.mu.Unlock()
	je.executions[exec.ID] = exec
}

func (je *JobExecutor) removeJobExecution(id uint) {
	je.mu.Lock()
	defer je.mu.Unlock()
	delete(je.executions, id)
}

func (je *JobExecutor) Dispose() error {
	je.mu.Lock()
	defer je.mu.Unlock()

	for _, trigger := range je.triggers {
		trigger.Dispose()
	}

	for _, exec := range je.executions {
		exec.cancel()
		je.updateJobExecutionResult(exec, errors.New("aborted"))
	}
	return nil
}

func (je *JobExecutor) TriggerExecutionWithSource(jobID uint, event TriggerEvent, triggerSource string) (task.Task, error) {
	job, e := je.jobDAO.GetJob(jobID)
	if e != nil {
		return task.Task{}, e
	}

	return je.runner.Execute(func(ctx types.TaskCtx) (any, error) {
		return nil, je.executeJobWithHistory(ctx, job, event, triggerSource, nil, 0, 0)
	}, task.WithNameGroup(job.Description, "job/execution"))
}

type jobExecutionItem struct {
	*types.JobExecution
	jobHistory *types.JobHistory
	cancel     func()
	logger     *jobExecutionLogger
}

func newJobExecutionLogger(jid uint, onLog func(string)) *jobExecutionLogger {
	return &jobExecutionLogger{jid: jid, onLog: onLog}
}

type jobExecutionLogger struct {
	jid   uint
	onLog func(string)
	logs  strings.Builder
	mu    sync.RWMutex
}

func (jel *jobExecutionLogger) Log(s string) {
	log.Printf("[JobExecutor] [%d] %s\n", jel.jid, s)
	if jel.onLog != nil {
		jel.onLog(s)
	}
	jel.mu.Lock()
	defer jel.mu.Unlock()
	jel.logs.WriteString(s)
	jel.logs.WriteRune('\n')
}

func (jel *jobExecutionLogger) String() string {
	jel.mu.RLock()
	defer jel.mu.RUnlock()
	return jel.logs.String()
}
