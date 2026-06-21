package server

import (
	"go-drive/common"
	err "go-drive/common/errors"
	"go-drive/common/event"
	"go-drive/common/registry"
	"go-drive/common/task"
	"go-drive/common/types"
	"go-drive/common/utils"
	"go-drive/drive"
	"go-drive/server/job"
	mp "go-drive/server/mount_permission"
	"go-drive/server/search"
	"go-drive/storage"
	"strings"

	"github.com/gin-gonic/gin"
)

func InitAdminRoutes(
	r gin.IRouter,
	ch *registry.ComponentsHolder,
	config common.Config,
	bus event.Bus,
	runner task.Runner,
	jobExecutor *job.JobExecutor,
	jobHistoryService *job.JobHistoryService,
	access *drive.Access,
	rootDrive *drive.RootDrive,
	searchSvc *search.Service,
	fullTextService *search.FullTextService,
	mountPermService *mp.MountPermissionService,
	tokenStore types.TokenStore,
	optionsDAO *storage.OptionsDAO,
	userDAO *storage.UserDAO,
	groupDAO *storage.GroupDAO,
	driveDAO *storage.DriveDAO,
	driveDataDAO *storage.DriveDataDAO,
	permissionDAO *storage.PathPermissionDAO,
	pathMountDAO *storage.PathMountDAO,
	pathMountRuleDAO *storage.PathMountRuleDAO,
	pathMetaDAO *storage.PathMetaDAO,
	jobDAO *storage.JobDAO,
	jobHistoryDAO *storage.JobHistoryDAO,
	jobRetryConfigDAO *storage.JobRetryConfigDAO,
	ftIndexDAO *storage.FullTextIndexDAO,
	indexJobStateDAO *storage.IndexJobStateDAO,
	driveSessionDAO *storage.DriveSessionDAO,
	fileBucketDAO *storage.FileBucketDAO) error {

	r = r.Group("/admin", TokenAuth(tokenStore), AdminGroupRequired())

	ur := &usersRoute{userDAO}
	// list users
	r.GET("/users", ur.listUsers)
	// get user by username
	r.GET("/user/:username", ur.getUser)
	// create user
	r.POST("/user", ur.createUser)
	// update user
	r.PUT("/user/:username", ur.updateUser)
	// delete user
	r.DELETE("/user/:username", ur.deleteUser)

	gr := &groupsRoute{userDAO, groupDAO}
	// list groups
	r.GET("/groups", gr.listGroups)
	// get group and it's users
	r.GET("/group/:name", gr.getGroup)
	// create group
	r.POST("/group", gr.createGroup)
	// update group
	r.PUT("/group/:name", gr.updateGroup)
	// delete group
	r.DELETE("/group/:name", gr.deleteGroup)

	dr := &drivesRoute{config, driveDAO, driveDataDAO, rootDrive}
	// get drive factories
	r.GET("/drive-factories", dr.getDriveFactories)
	// get drives
	r.GET("/drives", dr.getDrives)
	// add drive
	r.POST("/drive", dr.createDrive)
	// update drive
	r.PUT("/drive/:name", dr.updateDrive)
	// delete drive
	r.DELETE("/drive/:name", dr.deleteDrive)
	// get drive initialization information
	r.POST("/drive/:name/init-config", dr.getDriveInitConfig)
	// init drive
	r.POST("/drive/:name/init", dr.doDriveInit)
	// reload drives
	r.POST("/drives/reload", dr.reloadDrives)

	cr := &configRoute{access, permissionDAO, pathMetaDAO, pathMountDAO, optionsDAO, rootDrive, bus}
	// get by path
	r.GET("/path-permissions/*path", cr.getPathPermissions)
	// save path permissions
	r.PUT("/path-permissions/*path", cr.savePathPermissions)

	// get all path meta
	r.GET("/path-meta", cr.getAllPathMeta)
	// create or add path meta
	r.POST("/path-meta/*path", cr.savePathMeta)
	// delete path meta by path
	r.DELETE("/path-meta/*path", cr.deletePathMeta)

	// save options
	r.PUT("/options", cr.saveOptions)
	// get option
	r.GET("/options/:keys", cr.getOptions)

	// save mounts
	r.POST("/mount/*to", cr.savePathMounts)

	mr := &miscRoute{access, permissionDAO, pathMountDAO, rootDrive, searchSvc, ch}

	// Full-text index management
	ftr := &fullTextAdminRoute{fullTextService, ftIndexDAO, indexJobStateDAO, rootDrive, runner}
	// trigger full-text index build
	r.POST("/fulltext/index/:drive", ftr.triggerIndex)
	// pause index
	r.POST("/fulltext/index/:drive/pause", ftr.pauseIndex)
	// resume index
	r.POST("/fulltext/index/:drive/resume", ftr.resumeIndex)
	// get index state
	r.GET("/fulltext/index/:drive/state", ftr.getIndexState)
	// get index stats
	r.GET("/fulltext/stats", ftr.getStats)
	// clear index
	r.DELETE("/fulltext/index/:drive", ftr.clearIndex)

	// Mount point permissions management
	mpr := &mountPermRoute{mountPermService, pathMountRuleDAO, rootDrive}
	// get permission tree
	r.GET("/mount-permissions/tree/:drive", mpr.getPermissionTree)
	// get effective permissions — routed via getPermissions when path starts with /effective
	// (separate catch-all route conflicts with /*path in the same group)
	// get permissions by path
	r.GET("/mount-permissions/:drive/*path", mpr.getPermissions)
	// save permissions
	r.PUT("/mount-permissions/:drive/*path", mpr.savePermissions)
	// delete permissions
	r.DELETE("/mount-permissions/:drive/*path", mpr.deletePermissions)
	// Job history management routes
	jhr := &jobHistoryRoute{jobHistoryService, jobHistoryDAO, jobRetryConfigDAO, jobExecutor, runner}
	// get job history
	r.GET("/jobs/:id/history", jhr.getJobHistory)
	// get history detail
	r.GET("/jobs/history/:id", jhr.getHistoryDetail)
	// get event logs for history
	r.GET("/jobs/history/:id/logs", jhr.getEventLogs)
	// expand event log
	r.POST("/jobs/history/:id/logs/:logId/expand", jhr.expandEventLog)
	// manual retry
	r.POST("/jobs/history/:id/retry", jhr.manualRetry)
	// get dead letter jobs
	r.GET("/jobs/dead-letter", jhr.getDeadLetterJobs)
	// get retry config
	r.GET("/jobs/:id/retry-config", jhr.getRetryConfig)
	// save retry config
	r.PUT("/jobs/:id/retry-config", jhr.saveRetryConfig)
	// delete retry config
	r.DELETE("/jobs/:id/retry-config", jhr.deleteRetryConfig)
	// archive old histories
	r.POST("/jobs/history/archive", jhr.archiveOldHistories)
	// index files
	r.POST("/search/index/*path", mr.updateSearcherIndexes)
	// clean all PathPermission and PathMount that is point to invalid path
	r.POST("/clean-permissions-mounts", mr.cleanupInvalidPathPermissionsAndMounts)
	// get service stats
	r.GET("/stats", mr.getSystemStats)
	// clean drive cache
	r.DELETE("/drive-cache/:name", mr.clearDriveCache)

	// region script drives

	scriptDriveRoutesGroup := r.Group("/scripts")
	sdr := &scriptDrivesRoute{config: config}
	// get available drives from repository
	scriptDriveRoutesGroup.GET("/available", sdr.getAvailableDrives)
	// get installed drives
	scriptDriveRoutesGroup.GET("/installed", sdr.getInstalledDrives)
	// install drive
	scriptDriveRoutesGroup.POST("/install/:name", sdr.installDrive)
	// uninstall drive
	scriptDriveRoutesGroup.DELETE("/uninstall/:name", sdr.uninstallDrive)
	// get drive script content
	scriptDriveRoutesGroup.GET("/content/:name", sdr.getDriveScriptContent)
	// update drive script content
	scriptDriveRoutesGroup.PUT("/content/:name", sdr.saveDriveScriptContent)

	jobsRoutesGroup := r.Group("/jobs")
	jr := &jobsRoute{ch, runner, jobExecutor, jobDAO}
	// get all job definitions
	jobsRoutesGroup.GET("/definitions", jr.getJobsDefinitions)
	// get all created jobs
	jobsRoutesGroup.GET("", jr.getJobs)
	// create job
	jobsRoutesGroup.POST("", jr.createJob)
	// update job
	jobsRoutesGroup.PUT("/:id", jr.updateJob)
	// delete job
	jobsRoutesGroup.DELETE("/:id", jr.deleteJob)
	// get all executions
	jobsRoutesGroup.GET("/executions", jr.getAllExecutions)
	// execute a job
	jobsRoutesGroup.POST("/execution", jr.executeJob)
	// cancel job execution
	jobsRoutesGroup.PUT("/execution/:id/cancel", jr.cancelJobExecution)
	// delete job execution
	jobsRoutesGroup.DELETE("/execution/:id", jr.deleteJobExecution)
	// delete job executions by jobId
	jobsRoutesGroup.DELETE("/execution", jr.deleteJobExecutionsByJobId)
	// execute job script code
	jobsRoutesGroup.POST("/script-eval", jr.scriptEval)

	fbr := &fileBucketConfigRoute{fileBucketDAO}
	// get all file buckets
	r.GET("/file-buckets", fbr.getAllBuckets)
	// create file bucket
	r.POST("/file-bucket", fbr.createBucket)
	// update file bucket
	r.PUT("/file-bucket/:name", fbr.updateBucket)
	// delete file bucket
	r.DELETE("/file-bucket/:name", fbr.deleteBucket)

	return nil
}

// ==================== Full-text Index Admin Routes ====================

type fullTextAdminRoute struct {
	ftService       *search.FullTextService
	ftIndexDAO      *storage.FullTextIndexDAO
	jobStateDAO     *storage.IndexJobStateDAO
	rootDrive       *drive.RootDrive
	runner          task.Runner
}

func (r *fullTextAdminRoute) triggerIndex(c *gin.Context) {
	driveName := c.Param("drive")
	force := utils.ToBool(c.Query("force"))

	t, err := r.ftService.TriggerBuildIndex(driveName, force)
	if err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, t)
}

func (r *fullTextAdminRoute) pauseIndex(c *gin.Context) {
	driveName := c.Param("drive")
	if err := r.ftService.PauseIndex(driveName); err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, types.M{"status": "paused"})
}

func (r *fullTextAdminRoute) resumeIndex(c *gin.Context) {
	driveName := c.Param("drive")
	t, err := r.ftService.ResumeIndex(driveName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, t)
}

func (r *fullTextAdminRoute) getIndexState(c *gin.Context) {
	driveName := c.Param("drive")
	state, err := r.ftService.GetIndexState(driveName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, state)
}

func (r *fullTextAdminRoute) getStats(c *gin.Context) {
	stats, err := r.ftService.GetStats()
	if err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, stats)
}

func (r *fullTextAdminRoute) clearIndex(c *gin.Context) {
	driveName := c.Param("drive")
	if err := r.ftService.ClearIndex(driveName); err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, types.M{"status": "cleared"})
}

// ==================== Mount Permission Admin Routes ====================

type mountPermRoute struct {
	mpService       *mp.MountPermissionService
	ruleDAO         *storage.PathMountRuleDAO
	rootDrive       *drive.RootDrive
}

func (r *mountPermRoute) getPermissionTree(c *gin.Context) {
	driveName := c.Param("drive")
	tree, err := r.mpService.GetTree(driveName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, tree)
}

func (r *mountPermRoute) getPermissions(c *gin.Context) {
	driveName := c.Param("drive")
	path := utils.CleanPath(c.Param("path"))
	// Delegate to getEffectivePermissions when path starts with "effective/"
	// (Gin tree cannot have both :drive/effective/*path and :drive/*path registered)
	// Note: utils.CleanPath strips leading "/", so "effective" not "/effective"
	if path == "effective" || strings.HasPrefix(path, "effective/") {
		r.getEffectivePermissions(c)
		return
	}
	rules, err := r.ruleDAO.GetByPath(driveName, path)
	if err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, rules)
}

func (r *mountPermRoute) savePermissions(c *gin.Context) {
	driveName := c.Param("drive")
	path := utils.CleanPath(c.Param("path"))

	var rules []types.PathMountRule
	if err := c.Bind(&rules); err != nil {
		_ = c.Error(err)
		return
	}

	if err := r.ruleDAO.SaveRules(driveName, path, rules); err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, types.M{"status": "saved"})
}

func (r *mountPermRoute) deletePermissions(c *gin.Context) {
	driveName := c.Param("drive")
	path := utils.CleanPath(c.Param("path"))
	if err := r.ruleDAO.DeleteByPath(driveName, path); err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, types.M{"status": "deleted"})
}

func (r *mountPermRoute) getEffectivePermissions(c *gin.Context) {
	driveName := c.Param("drive")
	path := utils.CleanPath(c.Param("path"))
	// Strip the "effective" prefix from the path
	// Note: utils.CleanPath strips leading "/", so "effective" not "/effective"
	if path == "effective" {
		path = ""
	} else if strings.HasPrefix(path, "effective/") {
		path = strings.TrimPrefix(path, "effective/")
	}
	session := GetSession(c)

	subjects := make([]string, 0)
	subjects = append(subjects, types.AnySubject)
	if !session.IsAnonymous() {
		subjects = append(subjects, types.UserSubject(session.User.Username))
		for _, g := range session.User.Groups {
			subjects = append(subjects, types.GroupSubject(g.Name))
		}
	}

	perm, err := r.mpService.GetEffectivePermissions(driveName, path, subjects)
	if err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, types.M{
		"permission": perm,
		"readable":   perm.Readable(),
		"writable":   perm.Writable(),
	})
}

// ==================== Job History Admin Routes ====================

type jobHistoryRoute struct {
	historyService  *job.JobHistoryService
	historyDAO      *storage.JobHistoryDAO
	retryDAO        *storage.JobRetryConfigDAO
	jobExecutor     *job.JobExecutor
	runner          task.Runner
}

func (r *jobHistoryRoute) getJobHistory(c *gin.Context) {
	jobId := utils.ToUInt(c.Param("id"), 0)
	page := utils.ToInt(c.Query("page"), 1)
	pageSize := utils.ToInt(c.Query("pageSize"), 20)

	if pageSize > 100 {
		pageSize = 100
	}

	histories, total, err := r.historyDAO.GetHistoryByJob(jobId, page, pageSize)
	if err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, types.M{
		"items": histories,
		"total": total,
		"page":  page,
		"pageSize": pageSize,
	})
}

func (r *jobHistoryRoute) getHistoryDetail(c *gin.Context) {
	id := utils.ToUInt(c.Param("id"), 0)
	history, err := r.historyDAO.GetHistoryById(id)
	if err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, history)
}

func (r *jobHistoryRoute) getEventLogs(c *gin.Context) {
	id := utils.ToUInt(c.Param("id"), 0)
	truncate := utils.ToInt(c.Query("truncate"), 200)

	logs, err := r.historyService.GetEventLogs(id, truncate)
	if err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, logs)
}

func (r *jobHistoryRoute) expandEventLog(c *gin.Context) {
	historyId := utils.ToUInt(c.Param("id"), 0)
	logId := utils.ToUInt(c.Param("logId"), 0)

	log, err := r.historyService.ExpandEventLog(logId, historyId)
	if err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, log)
}

func (r *jobHistoryRoute) manualRetry(c *gin.Context) {
	id := utils.ToUInt(c.Param("id"), 0)

	t, err := r.historyService.ManualRetry(id)
	if err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, t)
}

func (r *jobHistoryRoute) getDeadLetterJobs(c *gin.Context) {
	page := utils.ToInt(c.Query("page"), 1)
	pageSize := utils.ToInt(c.Query("pageSize"), 20)

	if pageSize > 100 {
		pageSize = 100
	}

	histories, total, err := r.historyDAO.GetDeadLetterJobs(page, pageSize)
	if err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, types.M{
		"items": histories,
		"total": total,
		"page":  page,
		"pageSize": pageSize,
	})
}

func (r *jobHistoryRoute) getRetryConfig(c *gin.Context) {
	jobId := utils.ToUInt(c.Param("id"), 0)
	config, e := r.retryDAO.GetByJobId(jobId)
	if e != nil {
		if err.IsNotFoundError(e) {
			SetResult(c, types.JobRetryConfig{JobId: jobId})
			return
		}
		_ = c.Error(e)
		return
	}
	SetResult(c, config)
}

func (r *jobHistoryRoute) saveRetryConfig(c *gin.Context) {
	jobId := utils.ToUInt(c.Param("id"), 0)

	var config types.JobRetryConfig
	if err := c.Bind(&config); err != nil {
		_ = c.Error(err)
		return
	}
	config.JobId = jobId

	saved, e := r.retryDAO.Save(config)
	if e != nil {
		_ = c.Error(e)
		return
	}
	SetResult(c, saved)
}

func (r *jobHistoryRoute) deleteRetryConfig(c *gin.Context) {
	jobId := utils.ToUInt(c.Param("id"), 0)
	if err := r.retryDAO.DeleteByJobId(jobId); err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, types.M{"status": "deleted"})
}

func (r *jobHistoryRoute) archiveOldHistories(c *gin.Context) {
	days := utils.ToInt(c.Query("days"), 90)
	before := types.NowMillis() - int64(days)*24*60*60*1000

	count, err := r.historyDAO.ArchiveOldHistories(before)
	if err != nil {
		_ = c.Error(err)
		return
	}
	SetResult(c, types.M{
		"archived": count,
		"beforeMs": before,
		"days":     days,
	})
}
