//go:build wireinject
// +build wireinject

package main

import (
	"context"
	"go-drive/common"
	"go-drive/common/event"
	"go-drive/common/i18n"
	"go-drive/common/registry"
	"go-drive/common/task"
	"go-drive/common/types"
	"go-drive/common/utils"
	"go-drive/drive"
	"go-drive/server"
	"go-drive/server/job"
	mp "go-drive/server/mount_permission"
	"go-drive/server/search"
	"go-drive/server/thumbnail"
	"go-drive/storage"

	"github.com/gin-gonic/gin"
	"github.com/google/wire"
)

func Initialize(ctx context.Context, ch *registry.ComponentsHolder) (*gin.Engine, error) {
	wire.Build(
		common.InitConfig,
		storage.NewDB,
		event.NewBus,
		storage.NewUserDAO,
		storage.NewPathPermissionDAO,
		storage.NewDriveCacheDAO,
		storage.NewGroupDAO,
		storage.NewPathMountDAO,
		storage.NewDriveDAO,
		storage.NewDriveDataDAO,
		storage.NewOptionsDAO,
		storage.NewJobDAO,
		storage.NewPathMetaDAO,
		storage.NewFileBucketDAO,
		storage.NewIndexJobStateDAO,
		storage.NewDriveSessionDAO,
		storage.NewPathMountRuleDAO,
		storage.NewFullTextIndexDAO,
		storage.NewJobHistoryDAO,
		storage.NewJobRetryConfigDAO,
		wire.Bind(new(task.Runner), new(*task.TunnyRunner)),
		task.NewTunnyRunner,
		utils.NewSigner,
		wire.Bind(new(types.TokenStore), new(*server.FileTokenStore)),
		server.NewFileTokenStore,
		server.NewChunkUploader,
		thumbnail.NewMaker,
		drive.NewRootDrive,
		drive.NewAccess,
		search.NewService,
		search.NewFullTextService,
		mp.NewMountPermissionService,
		wire.Bind(new(i18n.MessageSource), new(*i18n.FileMessageSource)),
		i18n.NewFileMessageSource,
		job.NewJobExecutor,
		job.NewJobHistoryService,
		server.InitServer,
	)
	return &gin.Engine{}, nil
}
