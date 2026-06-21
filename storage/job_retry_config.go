package storage

import (
	"errors"
	err "go-drive/common/errors"
	"go-drive/common/registry"
	"go-drive/common/types"

	"gorm.io/gorm"
)

type JobRetryConfigDAO struct {
	db *DB
}

func NewJobRetryConfigDAO(db *DB, ch *registry.ComponentsHolder) *JobRetryConfigDAO {
	dao := &JobRetryConfigDAO{db: db}
	ch.Add(registry.KeyJobRetryConfigDAO, dao)
	return dao
}

func (s *JobRetryConfigDAO) GetByJobId(jobId uint) (types.JobRetryConfig, error) {
	config := types.JobRetryConfig{}
	e := s.db.C().Where("`job_id` = ?", jobId).First(&config).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return config, err.NewNotFoundError()
	}
	return config, e
}

func (s *JobRetryConfigDAO) Save(config types.JobRetryConfig) (types.JobRetryConfig, error) {
	return config, s.db.C().Save(&config).Error
}

func (s *JobRetryConfigDAO) UpdateRetryCount(jobId uint, retryCount int) error {
	return s.db.C().Model(&types.JobRetryConfig{}).
		Where("`job_id` = ?", jobId).
		Update("retry_count", retryCount).Error
}

func (s *JobRetryConfigDAO) UpdateNextRetryAt(jobId uint, nextRetryAt int64) error {
	return s.db.C().Model(&types.JobRetryConfig{}).
		Where("`job_id` = ?", jobId).
		Update("next_retry_at", nextRetryAt).Error
}

func (s *JobRetryConfigDAO) DeleteByJobId(jobId uint) error {
	return s.db.C().Delete(&types.JobRetryConfig{}, "`job_id` = ?", jobId).Error
}

func (s *JobRetryConfigDAO) GetPendingRetries(now int64) ([]types.JobRetryConfig, error) {
	configs := make([]types.JobRetryConfig, 0)
	e := s.db.C().
		Where("`retry_enabled` = ? AND `retry_count` < `max_retries` AND `next_retry_at` <= ?", true, now).
		Find(&configs).Error
	return configs, e
}
