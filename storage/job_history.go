package storage

import (
	"errors"
	err "go-drive/common/errors"
	"go-drive/common/registry"
	"go-drive/common/types"

	"gorm.io/gorm"
)

type JobHistoryDAO struct {
	db *DB
}

func NewJobHistoryDAO(db *DB, ch *registry.ComponentsHolder) *JobHistoryDAO {
	dao := &JobHistoryDAO{db: db}
	ch.Add(registry.KeyJobHistoryDAO, dao)
	return dao
}

func (h *JobHistoryDAO) CreateHistory(history *types.JobHistory) error {
	return h.db.C().Create(history).Error
}

func (h *JobHistoryDAO) UpdateHistoryStatus(id uint, status string, completedAt int64, durationMs int64, errorSummary string, errorDetail string) error {
	updates := map[string]any{
		"status": status,
	}
	if completedAt > 0 {
		updates["completed_at"] = completedAt
	}
	if durationMs > 0 {
		updates["duration_ms"] = durationMs
	}
	if errorSummary != "" {
		updates["error_summary"] = errorSummary
	}
	if errorDetail != "" {
		updates["error_detail"] = errorDetail
	}
	return h.db.C().Model(&types.JobHistory{}).Where("`id` = ?", id).Updates(updates).Error
}

func (h *JobHistoryDAO) GetHistoryByJob(jobId uint, page, pageSize int) ([]types.JobHistory, int64, error) {
	histories := make([]types.JobHistory, 0)
	var total int64

	query := h.db.C().Model(&types.JobHistory{}).Where("`job_id` = ?", jobId)

	if e := query.Count(&total).Error; e != nil {
		return histories, 0, e
	}

	offset := (page - 1) * pageSize
	e := query.Order("`started_at` DESC").Offset(offset).Limit(pageSize).Find(&histories).Error
	return histories, total, e
}

func (h *JobHistoryDAO) GetDeadLetterJobs(page, pageSize int) ([]types.JobHistory, int64, error) {
	histories := make([]types.JobHistory, 0)
	var total int64

	query := h.db.C().Model(&types.JobHistory{}).Where("`status` = ?", types.JobHistoryStatusDeadLetter)

	if e := query.Count(&total).Error; e != nil {
		return histories, 0, e
	}

	offset := (page - 1) * pageSize
	e := query.Order("`started_at` DESC").Offset(offset).Limit(pageSize).Find(&histories).Error
	return histories, total, e
}

func (h *JobHistoryDAO) GetHistoryById(id uint) (types.JobHistory, error) {
	history := types.JobHistory{}
	e := h.db.C().Where("`id` = ?", id).First(&history).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return history, err.NewNotFoundError()
	}
	return history, e
}

func (h *JobHistoryDAO) GetHistoriesByDateRange(jobId uint, startDate, endDate int64, page, pageSize int) ([]types.JobHistory, int64, error) {
	histories := make([]types.JobHistory, 0)
	var total int64

	query := h.db.C().Model(&types.JobHistory{}).
		Where("`started_at` >= ? AND `started_at` <= ?", startDate, endDate)
	if jobId > 0 {
		query = query.Where("`job_id` = ?", jobId)
	}

	if e := query.Count(&total).Error; e != nil {
		return histories, 0, e
	}

	offset := (page - 1) * pageSize
	e := query.Order("`started_at` DESC").Offset(offset).Limit(pageSize).Find(&histories).Error
	return histories, total, e
}

func (h *JobHistoryDAO) ArchiveOldHistories(beforeDate int64) (int64, error) {
	result := h.db.C().Model(&types.JobHistory{}).
		Where("`started_at` < ? AND `is_archived` = ?", beforeDate, false).
		Update("is_archived", true)
	return result.RowsAffected, result.Error
}

func (h *JobHistoryDAO) AddEventLog(log *types.JobEventLog) error {
	log.TruncateMessage()
	return h.db.C().Create(log).Error
}

func (h *JobHistoryDAO) GetEventLogs(historyId uint) ([]types.JobEventLog, error) {
	logs := make([]types.JobEventLog, 0)
	e := h.db.C().Where("`history_id` = ?", historyId).Order("`timestamp` ASC, `id` ASC").Find(&logs).Error
	if e != nil {
		return logs, e
	}
	for i := range logs {
		logs[i].ExpandMessage()
	}
	return logs, nil
}

func (h *JobHistoryDAO) MarkAsDeadLetter(id uint, errorDetail string) error {
	history := types.JobHistory{}
	e := h.db.C().Where("`id` = ?", id).First(&history).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return err.NewNotFoundError()
	}
	if e != nil {
		return e
	}

	updates := map[string]any{
		"status": types.JobHistoryStatusDeadLetter,
	}
	if errorDetail != "" {
		updates["error_detail"] = errorDetail
	}

	return h.db.C().Model(&history).Updates(updates).Error
}

func (h *JobHistoryDAO) RetryJob(historyId uint, newHistory *types.JobHistory) (*types.JobHistory, error) {
	oldHistory := types.JobHistory{}
	e := h.db.C().Where("`id` = ?", historyId).First(&oldHistory).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return nil, err.NewNotFoundError()
	}
	if e != nil {
		return nil, e
	}

	e = h.db.C().Transaction(func(tx *gorm.DB) error {
		newHistory.RetryOf = oldHistory.ID
		newHistory.RetryCount = oldHistory.RetryCount + 1
		if newHistory.TriggerSource == "" {
			newHistory.TriggerSource = types.TriggerSourceRetry
		}

		if e := tx.Create(newHistory).Error; e != nil {
			return e
		}

		return tx.Model(&oldHistory).Update("status", types.JobHistoryStatusRetrying).Error
	})

	return newHistory, e
}
