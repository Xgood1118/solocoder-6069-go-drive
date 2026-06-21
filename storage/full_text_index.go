package storage

import (
	"errors"
	err "go-drive/common/errors"
	"go-drive/common/registry"
	"go-drive/common/types"

	"gorm.io/gorm"
)

type FullTextIndexStats struct {
	Total     int64 `json:"total"`
	Indexed   int64 `json:"indexed"`
	Pending   int64 `json:"pending"`
	Failed    int64 `json:"failed"`
	TotalSize int64 `json:"totalSize"`
}

type FullTextIndexDAO struct {
	db *DB
}

func NewFullTextIndexDAO(db *DB, ch *registry.ComponentsHolder) *FullTextIndexDAO {
	dao := &FullTextIndexDAO{db: db}
	ch.Add(registry.KeyFullTextIndexDAO, dao)
	return dao
}

func (d *FullTextIndexDAO) GetByPathHash(pathHash string) (*types.FullTextIndex, error) {
	var index types.FullTextIndex
	e := d.db.C().Where("`path_hash` = ?", pathHash).Take(&index).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return nil, err.NewNotFoundError()
	}
	if e != nil {
		return nil, e
	}
	return &index, nil
}

func (d *FullTextIndexDAO) Save(index *types.FullTextIndex) error {
	return d.db.C().Create(index).Error
}

func (d *FullTextIndexDAO) UpdateContent(pathHash string, content string, contentHash string) error {
	return d.db.C().Model(&types.FullTextIndex{}).
		Where("`path_hash` = ?", pathHash).
		Updates(map[string]interface{}{
			"content":        content,
			"content_hash":   contentHash,
			"indexed":        true,
			"last_indexed_at": types.NowMillis(),
			"error_msg":      "",
		}).Error
}

func (d *FullTextIndexDAO) DeleteByPath(drive string, path string) error {
	pathHash := types.HashPath(drive, path)
	return d.db.C().Delete(&types.FullTextIndex{}, "`path_hash` = ?", pathHash).Error
}

func (d *FullTextIndexDAO) DeleteByDrive(drive string) error {
	return d.db.C().Delete(&types.FullTextIndex{}, "`drive` = ?", drive).Error
}

func (d *FullTextIndexDAO) ListPending(limit int) ([]types.FullTextIndex, error) {
	var indexes []types.FullTextIndex
	e := d.db.C().
		Where("`indexed` = ?", false).
		Order("`mod_time` DESC").
		Limit(limit).
		Find(&indexes).Error
	return indexes, e
}

func (d *FullTextIndexDAO) SearchByName(drive string, name string, offset int, limit int) ([]types.FullTextIndex, int64, error) {
	var indexes []types.FullTextIndex
	var total int64

	tx := d.db.C().Model(&types.FullTextIndex{}).Where("`indexed` = ?", true)
	if drive != "" {
		tx = tx.Where("`drive` = ?", drive)
	}
	if name != "" {
		tx = tx.Where("`name` LIKE ?", "%"+name+"%")
	}

	if e := tx.Count(&total).Error; e != nil {
		return nil, 0, e
	}

	if e := tx.Order("`mod_time` DESC").
		Offset(offset).
		Limit(limit).
		Find(&indexes).Error; e != nil {
		return nil, 0, e
	}

	return indexes, total, nil
}

func (d *FullTextIndexDAO) SearchByContent(drive string, keyword string, offset int, limit int) ([]types.FullTextIndex, int64, error) {
	var indexes []types.FullTextIndex
	var total int64

	tx := d.db.C().Model(&types.FullTextIndex{}).Where("`indexed` = ?", true)
	if drive != "" {
		tx = tx.Where("`drive` = ?", drive)
	}
	if keyword != "" {
		tx = tx.Where("`content` LIKE ?", "%"+keyword+"%")
	}

	if e := tx.Count(&total).Error; e != nil {
		return nil, 0, e
	}

	if e := tx.Order("`mod_time` DESC").
		Offset(offset).
		Limit(limit).
		Find(&indexes).Error; e != nil {
		return nil, 0, e
	}

	return indexes, total, nil
}

func (d *FullTextIndexDAO) GetStats() (*FullTextIndexStats, error) {
	var stats FullTextIndexStats

	if e := d.db.C().Model(&types.FullTextIndex{}).Count(&stats.Total).Error; e != nil {
		return nil, e
	}

	if e := d.db.C().Model(&types.FullTextIndex{}).
		Where("`indexed` = ?", true).
		Count(&stats.Indexed).Error; e != nil {
		return nil, e
	}

	if e := d.db.C().Model(&types.FullTextIndex{}).
		Where("`indexed` = ?", false).
		Count(&stats.Pending).Error; e != nil {
		return nil, e
	}

	if e := d.db.C().Model(&types.FullTextIndex{}).
		Where("`error_msg` != ''").
		Count(&stats.Failed).Error; e != nil {
		return nil, e
	}

	if e := d.db.C().Model(&types.FullTextIndex{}).
		Select("COALESCE(SUM(`size`), 0)").
		Scan(&stats.TotalSize).Error; e != nil {
		return nil, e
	}

	return &stats, nil
}

func (d *FullTextIndexDAO) CleanupOldIndexes(drive string, olderThan int64) (int64, error) {
	tx := d.db.C().Model(&types.FullTextIndex{}).
		Where("`last_indexed_at` < ?", olderThan)
	if drive != "" {
		tx = tx.Where("`drive` = ?", drive)
	}

	result := tx.Delete(&types.FullTextIndex{})
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

func (d *FullTextIndexDAO) SearchByNameAndContent(drive string, name string, content string, offset int, limit int) ([]types.FullTextIndex, int64, error) {
	var indexes []types.FullTextIndex
	var total int64

	tx := d.db.C().Model(&types.FullTextIndex{}).Where("`indexed` = ?", true)
	if drive != "" {
		tx = tx.Where("`drive` = ?", drive)
	}
	if name != "" {
		tx = tx.Where("`name` LIKE ?", "%"+name+"%")
	}
	if content != "" {
		tx = tx.Where("`content` LIKE ?", "%"+content+"%")
	}

	if e := tx.Count(&total).Error; e != nil {
		return nil, 0, e
	}

	if e := tx.Order("`mod_time` DESC").
		Offset(offset).
		Limit(limit).
		Find(&indexes).Error; e != nil {
		return nil, 0, e
	}

	return indexes, total, nil
}

func (d *FullTextIndexDAO) SearchByNameOrContent(drive string, name string, content string, offset int, limit int) ([]types.FullTextIndex, int64, error) {
	var indexes []types.FullTextIndex
	var total int64

	tx := d.db.C().Model(&types.FullTextIndex{}).Where("`indexed` = ?", true)
	if drive != "" {
		tx = tx.Where("`drive` = ?", drive)
	}
	if name != "" || content != "" {
		nameCond := d.db.C().Where("`name` LIKE ?", "%"+name+"%")
		contentCond := d.db.C().Where("`content` LIKE ?", "%"+content+"%")
		tx = tx.Where(d.db.C().Where(nameCond).Or(contentCond))
	}

	if e := tx.Count(&total).Error; e != nil {
		return nil, 0, e
	}

	if e := tx.Order("`mod_time` DESC").
		Offset(offset).
		Limit(limit).
		Find(&indexes).Error; e != nil {
		return nil, 0, e
	}

	return indexes, total, nil
}

func (d *FullTextIndexDAO) ListAll(drive string, offset int, limit int) ([]types.FullTextIndex, int64, error) {
	var indexes []types.FullTextIndex
	var total int64

	tx := d.db.C().Model(&types.FullTextIndex{}).Where("`indexed` = ?", true)
	if drive != "" {
		tx = tx.Where("`drive` = ?", drive)
	}

	if e := tx.Count(&total).Error; e != nil {
		return nil, 0, e
	}

	if e := tx.Order("`mod_time` DESC").
		Offset(offset).
		Limit(limit).
		Find(&indexes).Error; e != nil {
		return nil, 0, e
	}

	return indexes, total, nil
}
