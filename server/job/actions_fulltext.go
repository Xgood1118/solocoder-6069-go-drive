package job

import (
	"context"
	"fmt"
	"go-drive/common/i18n"
	"go-drive/common/registry"
	"go-drive/common/task"
	"go-drive/common/types"
	"go-drive/server/search"
	"strconv"
)

func init() {
	t := i18n.TPrefix("jobs.fulltext_index.")

	RegisterActionDef(JobActionDef{
		Name:        "fulltext_index",
		DisplayName: t("name"),
		Description: t("desc"),
		ParamsForm: []types.FormItem{
			{Field: "drive", Label: t("drive"), Description: t("drive_desc"), Type: "text", Required: true},
			{Field: "force", Label: t("force"), Description: t("force_desc"), Type: "checkbox"},
			{Field: "maxSize", Label: t("max_size"), Description: t("max_size_desc"), Type: "text", DefaultValue: "10MB"},
		},
		Do: func(ctx context.Context, params types.SM, ch *registry.ComponentsHolder, log func(string)) error {
			driveName := params["drive"]
			force := params.GetBool("force")
			maxSizeStr := params["maxSize"]

			ftService := ch.Get(registry.KeyFullTextIndexService).(*search.FullTextService)

			var maxSize int64 = 10 * 1024 * 1024
			if maxSizeStr != "" {
				maxSize = types.SV(maxSizeStr).DataSize(10 * 1024 * 1024)
			}
			ftService.SetMaxContentSize(maxSize)

			taskCtx, ok := ctx.(types.TaskCtx)
			if !ok {
				taskCtx = task.NewContextWrapper(ctx)
			}

			log(fmt.Sprintf("Starting full-text index for drive: %s, force: %v, maxSize: %d bytes", driveName, force, maxSize))

			runningTask, err := ftService.TriggerBuildIndex(driveName, force)
			if err != nil {
				return fmt.Errorf("failed to start index job: %w", err)
			}

			log(fmt.Sprintf("Index task started: %s", runningTask.Id))

			for {
				select {
				case <-ctx.Done():
					_ = ftService.PauseIndex(driveName)
					return ctx.Err()
				default:
				}

				state, err := ftService.GetIndexState(driveName)
				if err != nil {
					return fmt.Errorf("failed to get index state: %w", err)
				}
				if state == nil {
					return fmt.Errorf("index state not found")
				}

				log(fmt.Sprintf("Progress: %d/%d files scanned, %d indexed, %d failed",
					state.ScannedFiles, state.TotalFiles, state.IndexedFiles, state.FailedFiles))

				if state.Status == types.IndexStatusCompleted {
					log(fmt.Sprintf("Index completed successfully. Total: %d, Indexed: %d, Failed: %d",
						state.TotalFiles, state.IndexedFiles, state.FailedFiles))
					break
				}
				if state.Status == types.IndexStatusFailed {
					return fmt.Errorf("index failed: %s", state.ErrorMsg)
				}
				if state.Status == types.IndexStatusPaused {
					log("Index paused")
					break
				}

				taskCtx.Total(state.TotalFiles, true)
				taskCtx.Progress(state.ScannedFiles, true)

				select {
				case <-ctx.Done():
					_ = ftService.PauseIndex(driveName)
					return ctx.Err()
				default:
				}
			}

			return nil
		},
	})

	RegisterActionDef(JobActionDef{
		Name:        "fulltext_index_pause",
		DisplayName: t("pause_name"),
		Description: t("pause_desc"),
		ParamsForm: []types.FormItem{
			{Field: "drive", Label: t("drive"), Description: t("drive_desc"), Type: "text", Required: true},
		},
		Do: func(ctx context.Context, params types.SM, ch *registry.ComponentsHolder, log func(string)) error {
			driveName := params["drive"]
			ftService := ch.Get(registry.KeyFullTextIndexService).(*search.FullTextService)

			log(fmt.Sprintf("Pausing full-text index for drive: %s", driveName))
			return ftService.PauseIndex(driveName)
		},
	})

	RegisterActionDef(JobActionDef{
		Name:        "fulltext_index_resume",
		DisplayName: t("resume_name"),
		Description: t("resume_desc"),
		ParamsForm: []types.FormItem{
			{Field: "drive", Label: t("drive"), Description: t("drive_desc"), Type: "text", Required: true},
		},
		Do: func(ctx context.Context, params types.SM, ch *registry.ComponentsHolder, log func(string)) error {
			driveName := params["drive"]
			ftService := ch.Get(registry.KeyFullTextIndexService).(*search.FullTextService)

			log(fmt.Sprintf("Resuming full-text index for drive: %s", driveName))
			_, err := ftService.ResumeIndex(driveName)
			return err
		},
	})

	RegisterActionDef(JobActionDef{
		Name:        "fulltext_index_clear",
		DisplayName: t("clear_name"),
		Description: t("clear_desc"),
		ParamsForm: []types.FormItem{
			{Field: "drive", Label: t("drive"), Description: t("drive_desc"), Type: "text", Required: true},
		},
		Do: func(ctx context.Context, params types.SM, ch *registry.ComponentsHolder, log func(string)) error {
			driveName := params["drive"]
			ftService := ch.Get(registry.KeyFullTextIndexService).(*search.FullTextService)

			log(fmt.Sprintf("Clearing full-text index for drive: %s", driveName))
			return ftService.ClearIndex(driveName)
		},
	})
}
