package storage

import (
	"errors"
	err "go-drive/common/errors"
	"go-drive/common/registry"
	"go-drive/common/types"
	"time"

	"gorm.io/gorm"
)

type DriveSessionDAO struct {
	db *DB
}

func NewDriveSessionDAO(db *DB, ch *registry.ComponentsHolder) *DriveSessionDAO {
	dao := &DriveSessionDAO{db: db}
	ch.Add(registry.KeyDriveSessionDAO, dao)
	return dao
}

func (d *DriveSessionDAO) CreateSession(drive, token, username string, ttl time.Duration) (types.DriveSession, error) {
	session := types.DriveSession{
		Drive:     drive,
		Token:     token,
		Username:  username,
		CreatedAt: types.NowMillis(),
		ExpiresAt: types.NowMillis() + ttl.Milliseconds(),
	}
	e := d.db.C().Create(&session).Error
	return session, e
}

func (d *DriveSessionDAO) GetSession(token string) (types.DriveSession, error) {
	var session types.DriveSession
	e := d.db.C().Where("`token` = ?", token).Take(&session).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return session, err.NewNotFoundError()
	}
	if e != nil {
		return session, e
	}
	if session.ExpiresAt < types.NowMillis() {
		_ = d.db.C().Delete(&session, "`token` = ?", token).Error
		return session, err.NewNotFoundError()
	}
	return session, nil
}

func (d *DriveSessionDAO) DeleteSession(token string) error {
	return d.db.C().Delete(&types.DriveSession{}, "`token` = ?", token).Error
}

func (d *DriveSessionDAO) DeleteExpiredSessions() (int64, error) {
	result := d.db.C().Delete(&types.DriveSession{}, "`expires_at` < ?", types.NowMillis())
	return result.RowsAffected, result.Error
}

func (d *DriveSessionDAO) DeleteByUser(username string) (int64, error) {
	result := d.db.C().Delete(&types.DriveSession{}, "`username` = ?", username)
	return result.RowsAffected, result.Error
}

func (d *DriveSessionDAO) DeleteByDrive(drive string) (int64, error) {
	result := d.db.C().Delete(&types.DriveSession{}, "`drive` = ?", drive)
	return result.RowsAffected, result.Error
}
