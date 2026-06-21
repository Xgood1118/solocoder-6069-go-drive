package storage

import (
	"errors"
	"go-drive/common/registry"
	"go-drive/common/types"

	"gorm.io/gorm"
)

type IndexJobStateDAO struct {
	db *DB
}

func NewIndexJobStateDAO(db *DB, ch *registry.ComponentsHolder) *IndexJobStateDAO {
	dao := &IndexJobStateDAO{db: db}
	ch.Add(registry.KeyIndexJobStateDAO, dao)
	return dao
}

func (d *IndexJobStateDAO) GetByDrive(drive string) (*types.IndexJobState, error) {
	var state types.IndexJobState
	e := d.db.C().Where("`drive` = ?", drive).Take(&state).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	return &state, nil
}

func (d *IndexJobStateDAO) CreateOrUpdate(state *types.IndexJobState) error {
	var existing types.IndexJobState
	e := d.db.C().Where("`drive` = ?", state.Drive).Take(&existing).Error
	if e == nil {
		state.ID = existing.ID
		return d.db.C().Save(state).Error
	}
	if !errors.Is(e, gorm.ErrRecordNotFound) {
		return e
	}
	return d.db.C().Create(state).Error
}

func (d *IndexJobStateDAO) UpdateProgress(drive string, currentPath string, totalFiles int64, scannedFiles int64, indexedFiles int64, failedFiles int64) error {
	return d.db.C().Model(&types.IndexJobState{}).
		Where("`drive` = ?", drive).
		Updates(map[string]interface{}{
			"current_path":   currentPath,
			"total_files":    totalFiles,
			"scanned_files":  scannedFiles,
			"indexed_files":  indexedFiles,
			"failed_files":   failedFiles,
			"last_updated_at": types.NowMillis(),
		}).Error
}

func (d *IndexJobStateDAO) UpdateStatus(drive string, status string, errorMsg ...string) error {
	updates := map[string]interface{}{
		"status":          status,
		"last_updated_at": types.NowMillis(),
	}
	if len(errorMsg) > 0 {
		updates["error_msg"] = errorMsg[0]
	}
	if status == types.IndexStatusRunning {
		updates["started_at"] = types.NowMillis()
	}
	return d.db.C().Model(&types.IndexJobState{}).
		Where("`drive` = ?", drive).
		Updates(updates).Error
}

func (d *IndexJobStateDAO) DeleteByDrive(drive string) error {
	return d.db.C().Delete(&types.IndexJobState{}, "`drive` = ?", drive).Error
}

func (d *IndexJobStateDAO) ListAll() ([]types.IndexJobState, error) {
	var states []types.IndexJobState
	e := d.db.C().Find(&states).Error
	return states, e
}
