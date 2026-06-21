package types

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

type Option struct {
	Key   string `gorm:"column:key;primaryKey;type:string;size:64"`
	Value string `gorm:"column:value;type:string;size:4096"`
}

type User struct {
	Username string  `gorm:"column:username;primaryKey;not null;type:string;size:32" json:"username" binding:"required"`
	Password string  `gorm:"column:password;not null;type:string;size:64" json:"password,omitempty"`
	RootPath string  `gorm:"column:root_path;type:string;size:512" json:"rootPath,omitempty"`
	Groups   []Group `gorm:"many2many:user_groups;joinForeignKey:username;foreignKey:username" json:"groups"`
}

type Group struct {
	Name string `gorm:"column:name;primaryKey;not null;type:string;size:32" json:"name" binding:"required"`
}

type UserGroup struct {
	Username  string `gorm:"column:username;primaryKey;not null;type:string;size:32" binding:"required"`
	GroupName string `gorm:"column:group_name;primaryKey;not null;type:string;size:32" binding:"required"`
}

type Drive struct {
	Name    string `gorm:"column:name;primaryKey;not null;type:string;size:255" json:"name" binding:"required"`
	Enabled bool   `gorm:"column:enabled;not null;type:bool" json:"enabled"`
	Type    string `gorm:"column:type;not null;type:string;size:32" json:"type" binding:"required"`
	Config  string `gorm:"column:config;not null;type:string;size:4096" json:"config"`
}

type PathMount struct {
	ID      uint    `gorm:"column:id;primaryKey;autoIncrement"`
	Path    *string `gorm:"column:path;not null;type:string;size:512" json:"path"`
	Name    string  `gorm:"column:name;not null;type:string;size:255" json:"name"`
	MountAt string  `gorm:"column:mount_at;not null;type:string;size:512" json:"mountAt"`
}

func (PathMount) TableName() string {
	return "path_mount"
}

type DriveData struct {
	Drive string `gorm:"column:drive;primaryKey;not null;type:string;size:255"`
	Key   string `gorm:"column:data_key;primaryKey;not null;type:string;size:255"`
	Value string `gorm:"column:data_value;not null;type:string;size:4096"`
}

func (DriveData) TableName() string {
	return "drive_data"
}

const (
	CacheEntry    uint8 = 1
	CacheChildren uint8 = 2
)

type DriveCache struct {
	Drive     string `gorm:"column:drive;primaryKey;not null;type:string;size:255"`
	Path      string `gorm:"column:path;primaryKey;not null;type:string;size:255"`
	Depth     *uint8 `gorm:"column:depth;primaryKey;not null"`
	Type      uint8  `gorm:"column:type;primaryKey;not null"`
	Value     string `gorm:"column:cache_value;not null;type:text"`
	ExpiresAt int64  `gorm:"column:expires_at;not null"`
}

func (DriveCache) TableName() string {
	return "drive_cache"
}

type Permission uint8

func (p Permission) Readable() bool {
	return p&PermissionRead == PermissionRead
}

func (p Permission) Writable() bool {
	return p&PermissionWrite == PermissionWrite
}

const (
	PermissionEmpty     Permission = 0
	PermissionRead      Permission = 1 << 0
	PermissionWrite     Permission = 1 << 1
	PermissionReadWrite            = PermissionRead | PermissionWrite
)

const (
	PolicyReject uint8 = 0
	PolicyAccept uint8 = 1
)

type PathPermission struct {
	ID      uint    `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	Path    *string `gorm:"column:path;not null;type:string;size:512" json:"path"`
	Subject string  `gorm:"column:subject;not null;type:string;size:34" json:"subject"`
	// Permission bits for the path which subject accessed: 1: read, 2: write
	Permission Permission `gorm:"column:permission;not null" json:"permission"`
	// Policy to apply to the permission when subject access this path: 0: REJECT, 1: ACCEPT
	Policy uint8 `gorm:"column:policy;not null" json:"policy"`
}

type PathMeta struct {
	Path          *string `gorm:"column:path;primaryKey;not null;type:string;size:512" json:"path"`
	Password      string  `gorm:"column:password;type:string;size:64" json:"password"`
	DefaultSort   string  `gorm:"column:default_sort;type:string;size:32" json:"defaultSort"`
	DefaultMode   string  `gorm:"column:default_mode;type:string;size:32" json:"defaultMode"`
	HiddenPattern string  `gorm:"column:hidden_pattern;type:string;size:512" json:"hiddenPattern"`

	// Recursive Password|DefaultSort|DefaultMode|HiddenPattern
	Recursive uint32 `gorm:"column:recursive;not null" json:"recursive"`
}

type FileBucket struct {
	Name        string `gorm:"column:name;primaryKey;not null;type:string;size:255" json:"name" binding:"required"`
	TargetPath  string `gorm:"column:target_path;not null;type:string;size:512" json:"targetPath" binding:"required"`
	KeyTemplate string `gorm:"column:key_template;type:string;size:512" json:"keyTemplate"`
	CustomKey   bool   `gorm:"column:custom_key;not null;type:bool" json:"customKey"`
	// SecretToken is the auto-generated upload token for this bucket
	SecretToken string `gorm:"column:secret_token;type:string;size:32" json:"secretToken" binding:"required"`
	URLTemplate string `gorm:"column:url_template;not null;type:string;size:512" json:"urlTemplate"`
	// AllowedTypes is a comma separated list of allowed mime types or file extensions, e.g. "image/png,image/jpeg,.png,.jpg"
	AllowedTypes string `gorm:"column:allowed_types;type:string;size:512" json:"allowedTypes"`
	// MaxSize is the maximum allowed size with unit, 0 for unlimited
	MaxSize string `gorm:"column:max_size;not null;type:string" json:"maxSize"`

	// AllowedReferrers is a comma separated list of allowed referrer hosts
	AllowedReferrers string `gorm:"column:allowed_referrers;type:string;size:512" json:"allowedReferrers"`
	// CacheMaxAge is the maximum age in the Cache-Control header with unit
	CacheMaxAge string `gorm:"column:cache_max_age;type:string" json:"cacheMaxAge"`
}

type Job struct {
	ID          uint   `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	Description string `gorm:"column:description;not null;type:text" json:"description"`

	Triggers     string `gorm:"column:triggers;not null;type:text;default:_need_migration_" json:"triggers"`
	Action       string `gorm:"column:action;not null;type:string;size:64;default:_need_migration_" json:"action"`
	ActionParams string `gorm:"column:action_params;not null;type:text;size:512;default:_need_migration_" json:"actionParams"`

	Enabled bool `gorm:"column:enabled;not null;type:bool" json:"enabled"`

	// Schedule is kept for backward compatibility, but deprecated
	DeprecatedSchedule string `gorm:"column:schedule;type:string;size:64" json:"-"`
	DeprecatedJob      string `gorm:"column:job;not null;type:string;size:64" json:"-"`
	DeprecatedParams   string `gorm:"column:params;type:text" json:"-"`
}

const (
	JobExecutionRunning = "running"
	JobExecutionSuccess = "success"
	JobExecutionFailed  = "failed"
)

type JobExecution struct {
	ID          uint   `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	JobId       uint   `gorm:"column:job_id;not null;type:uint" json:"jobId"`
	StartedAt   uint64 `gorm:"column:started_at;type:uint" json:"startedAt"`
	CompletedAt uint64 `gorm:"column:completed_at;type:uint" json:"completedAt"`
	Status      string `gorm:"column:status;not null;type:string" json:"status"`
	Logs        string `gorm:"column:logs;type:string" json:"logs"`
	ErrorMsg    string `gorm:"column:error_msg;type:text" json:"errorMsg"`
}

func UserSubject(username string) string {
	return "u:" + username
}

func GroupSubject(name string) string {
	return "g:" + name
}

const AnySubject = "ANY"

func (p PathPermission) IsForAnonymous() bool {
	return p.Subject == AnySubject
}

func (p PathPermission) IsForUser() bool {
	return strings.HasPrefix(p.Subject, "u:")
}

func (p PathPermission) IsForGroup() bool {
	return strings.HasPrefix(p.Subject, "g:")
}

func (p PathPermission) IsAccept() bool {
	return p.Policy == PolicyAccept
}

func (p PathPermission) IsReject() bool {
	return p.Policy == PolicyReject
}

// ==================== Full-text Index Models ====================

const (
	IndexStatusPending   = "pending"
	IndexStatusRunning   = "running"
	IndexStatusPaused    = "paused"
	IndexStatusCompleted = "completed"
	IndexStatusFailed    = "failed"
)

type FullTextIndex struct {
	ID            uint      `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	Drive         string    `gorm:"column:drive;not null;type:string;size:255;index" json:"drive"`
	PathHash      string    `gorm:"column:path_hash;not null;type:string;size:64;uniqueIndex" json:"pathHash"`
	Path          string    `gorm:"column:path;not null;type:string;size:4096" json:"path"`
	Name          string    `gorm:"column:name;not null;type:string;size:255;index" json:"name"`
	Ext           string    `gorm:"column:ext;type:string;size:64" json:"ext"`
	MimeType      string    `gorm:"column:mime_type;type:string;size:128;index" json:"mimeType"`
	Size          int64     `gorm:"column:size;not null;type:bigint" json:"size"`
	ModTime       int64     `gorm:"column:mod_time;not null;type:bigint;index" json:"modTime"`
	Content       string    `gorm:"column:content;type:text" json:"-"`
	ContentHash   string    `gorm:"column:content_hash;not null;type:string;size:64;index" json:"contentHash"`
	LastIndexedAt int64     `gorm:"column:last_indexed_at;not null;type:bigint;index" json:"lastIndexedAt"`
	Indexed       bool      `gorm:"column:indexed;not null;type:bool;default:false;index" json:"indexed"`
	ErrorMsg      string    `gorm:"column:error_msg;type:text" json:"errorMsg,omitempty"`
}

func (FullTextIndex) TableName() string {
	return "full_text_index"
}

func HashPath(drive, path string) string {
	h := sha256.Sum256([]byte(drive + ":" + path))
	return hex.EncodeToString(h[:])
}

func HashContent(content string) string {
	h := sha256.Sum256([]byte(content))
	return hex.EncodeToString(h[:])
}

type IndexJobState struct {
	ID             uint   `gorm:"column:id;primaryKey;autoIncrement"`
	Drive          string `gorm:"column:drive;not null;type:string;size:255;uniqueIndex"`
	Status         string `gorm:"column:status;not null;type:string;size:32;index"`
	CurrentPath    string `gorm:"column:current_path;type:string;size:4096"`
	TotalFiles     int64  `gorm:"column:total_files;not null;type:bigint;default:0"`
	ScannedFiles   int64  `gorm:"column:scanned_files;not null;type:bigint;default:0"`
	IndexedFiles   int64  `gorm:"column:indexed_files;not null;type:bigint;default:0"`
	FailedFiles    int64  `gorm:"column:failed_files;not null;type:bigint;default:0"`
	StartedAt      int64  `gorm:"column:started_at;type:bigint"`
	LastUpdatedAt  int64  `gorm:"column:last_updated_at;type:bigint"`
	ErrorMsg       string `gorm:"column:error_msg;type:text"`
}

func (IndexJobState) TableName() string {
	return "index_job_state"
}

// ==================== Mount Point Permission Models ====================

type PathMountRule struct {
	ID         uint       `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	Drive      string     `gorm:"column:drive;not null;type:string;size:255;index" json:"drive"`
	Path       string     `gorm:"column:path;not null;type:string;size:512;index" json:"path"`
	Subject    string     `gorm:"column:subject;not null;type:string;size:34" json:"subject"`
	Permission Permission `gorm:"column:permission;not null" json:"permission"`
	Policy     uint8      `gorm:"column:policy;not null" json:"policy"`
	Inherits   bool       `gorm:"column:inherits;not null;type:bool;default:true" json:"inherits"`
	CreatedAt  int64      `gorm:"column:created_at;not null;type:bigint" json:"createdAt"`
	UpdatedAt  int64      `gorm:"column:updated_at;not null;type:bigint" json:"updatedAt"`
}

func (PathMountRule) TableName() string {
	return "path_mount_rule"
}

type MountPermissionNode struct {
	Path        string                `json:"path"`
	Drive       string                `json:"drive"`
	IsMountRoot bool                  `json:"isMountRoot"`
	Permissions []PathMountRule       `json:"permissions"`
	Children    []*MountPermissionNode `json:"children,omitempty"`
}

// ==================== Job History & Retry Models ====================

const (
	JobHistoryStatusRunning   = "running"
	JobHistoryStatusSuccess   = "success"
	JobHistoryStatusFailed    = "failed"
	JobHistoryStatusDeadLetter = "dead_letter"
	JobHistoryStatusRetrying  = "retrying"
)

const (
	TriggerSourceCron    = "cron"
	TriggerSourceManual  = "manual"
	TriggerSourceEvent   = "event"
	TriggerSourceRetry   = "retry"
	TriggerSourceAPI     = "api"
)

type JobRetryConfig struct {
	ID                   uint   `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	JobId                uint   `gorm:"column:job_id;not null;type:uint;uniqueIndex" json:"jobId"`
	RetryEnabled         bool   `gorm:"column:retry_enabled;not null;type:bool;default:false" json:"retryEnabled"`
	RetryIntervalMinutes int    `gorm:"column:retry_interval_minutes;not null;type:int;default:5" json:"retryIntervalMinutes"`
	MaxRetries           int    `gorm:"column:max_retries;not null;type:int;default:3" json:"maxRetries"`
	RetryCount           int    `gorm:"column:retry_count;not null;type:int;default:0" json:"retryCount"`
	NextRetryAt          int64  `gorm:"column:next_retry_at;type:bigint" json:"nextRetryAt,omitempty"`
}

func (JobRetryConfig) TableName() string {
	return "job_retry_config"
}

type JobHistory struct {
	ID             uint   `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	JobId          uint   `gorm:"column:job_id;not null;type:uint;index" json:"jobId"`
	JobDescription string `gorm:"column:job_description;type:text" json:"jobDescription"`
	ExecutionId    uint   `gorm:"column:execution_id;type:uint" json:"executionId,omitempty"`
	TriggerSource  string `gorm:"column:trigger_source;not null;type:string;size:32;index" json:"triggerSource"`
	TriggerData    string `gorm:"column:trigger_data;type:text" json:"triggerData,omitempty"`
	Status         string `gorm:"column:status;not null;type:string;size:32;index" json:"status"`
	StartedAt      int64  `gorm:"column:started_at;not null;type:bigint;index" json:"startedAt"`
	CompletedAt    int64  `gorm:"column:completed_at;type:bigint" json:"completedAt,omitempty"`
	DurationMs     int64  `gorm:"column:duration_ms;type:bigint" json:"durationMs,omitempty"`
	ErrorSummary   string `gorm:"column:error_summary;type:string;size:512" json:"errorSummary,omitempty"`
	ErrorDetail    string `gorm:"column:error_detail;type:text" json:"errorDetail,omitempty"`
	IsArchived     bool   `gorm:"column:is_archived;not null;type:bool;default:false;index" json:"isArchived"`
	RetryOf        uint   `gorm:"column:retry_of;type:uint;index" json:"retryOf,omitempty"`
	RetryCount     int    `gorm:"column:retry_count;not null;type:int;default:0" json:"retryCount"`
}

func (JobHistory) TableName() string {
	return "job_history"
}

type JobEventLog struct {
	ID          uint   `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	HistoryId   uint   `gorm:"column:history_id;not null;type:uint;index" json:"historyId"`
	Timestamp   int64  `gorm:"column:timestamp;not null;type:bigint;index" json:"timestamp"`
	Level       string `gorm:"column:level;not null;type:string;size:16;default:info" json:"level"`
	Message     string `gorm:"column:message;not null;type:text" json:"message"`
	MessageFull string `gorm:"column:message_full;type:text" json:"messageFull,omitempty"`
	IsExpanded  bool   `gorm:"-" json:"isExpanded,omitempty"`
}

func (JobEventLog) TableName() string {
	return "job_event_log"
}

func (l *JobEventLog) TruncateMessage() {
	if len(l.Message) > 200 {
		l.MessageFull = l.Message
		l.Message = l.Message[:200] + "..."
	}
}

func (l *JobEventLog) ExpandMessage() {
	if l.MessageFull != "" {
		l.Message = l.MessageFull
		l.IsExpanded = true
	}
}

// ==================== Drive Context & Token Models ====================

type DriveSession struct {
	Drive     string    `gorm:"column:drive;primaryKey;not null;type:string;size:255"`
	Token     string    `gorm:"column:token;not null;type:string;size:64;index"`
	Username  string    `gorm:"column:username;not null;type:string;size:32;index"`
	CreatedAt int64     `gorm:"column:created_at;not null;type:bigint"`
	ExpiresAt int64     `gorm:"column:expires_at;not null;type:bigint;index"`
}

func (DriveSession) TableName() string {
	return "drive_session"
}

// Now returns current Unix timestamp in milliseconds
func NowMillis() int64 {
	return time.Now().UnixMilli()
}
