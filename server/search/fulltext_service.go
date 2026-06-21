package search

import (
	"context"
	"fmt"
	"go-drive/common/registry"
	"go-drive/common/task"
	"go-drive/common/types"
	"go-drive/common/utils"
	"go-drive/drive"
	"go-drive/storage"
	"log"
	"path/filepath"
	"strings"
)

const (
	defaultMaxContentSize    = 10 * 1024 * 1024
	fullTextIndexBatchSize   = 100
)

type FullTextService struct {
	ch              *registry.ComponentsHolder
	rootDrive       *drive.RootDrive
	runner          task.Runner
	ftIndexDAO      *storage.FullTextIndexDAO
	jobStateDAO     *storage.IndexJobStateDAO
	maxContentSize  int64
}

func NewFullTextService(
	ch *registry.ComponentsHolder,
	rootDrive *drive.RootDrive,
	runner task.Runner,
	ftIndexDAO *storage.FullTextIndexDAO,
	jobStateDAO *storage.IndexJobStateDAO,
) *FullTextService {
	s := &FullTextService{
		ch:             ch,
		rootDrive:      rootDrive,
		runner:         runner,
		ftIndexDAO:     ftIndexDAO,
		jobStateDAO:    jobStateDAO,
		maxContentSize: defaultMaxContentSize,
	}
	ch.Add(registry.KeyFullTextIndexService, s)
	return s
}

func (s *FullTextService) SetMaxContentSize(size int64) {
	s.maxContentSize = size
}

func (s *FullTextService) TriggerBuildIndex(driveName string, force bool) (task.Task, error) {
	existingState, err := s.jobStateDAO.GetByDrive(driveName)
	if err != nil {
		return task.Task{}, err
	}
	if existingState != nil && existingState.Status == types.IndexStatusRunning {
		return task.Task{}, fmt.Errorf("index job already running for drive: %s", driveName)
	}

	return s.runner.Execute(func(ctx types.TaskCtx) (any, error) {
		err := s.buildIndex(ctx, driveName, force)
		return nil, err
	}, task.WithNameGroup(driveName, "search/fulltext-index"))
}

func (s *FullTextService) PauseIndex(driveName string) error {
	state, err := s.jobStateDAO.GetByDrive(driveName)
	if err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("no index job found for drive: %s", driveName)
	}
	return s.jobStateDAO.UpdateStatus(driveName, types.IndexStatusPaused)
}

func (s *FullTextService) ResumeIndex(driveName string) (task.Task, error) {
	state, err := s.jobStateDAO.GetByDrive(driveName)
	if err != nil {
		return task.Task{}, err
	}
	if state == nil {
		return task.Task{}, fmt.Errorf("no index job found for drive: %s", driveName)
	}
	if state.Status != types.IndexStatusPaused {
		return task.Task{}, fmt.Errorf("index job is not paused: %s", state.Status)
	}

	return s.runner.Execute(func(ctx types.TaskCtx) (any, error) {
		err := s.buildIndex(ctx, driveName, false)
		return nil, err
	}, task.WithNameGroup(driveName, "search/fulltext-index"))
}

func (s *FullTextService) GetIndexState(driveName string) (*types.IndexJobState, error) {
	return s.jobStateDAO.GetByDrive(driveName)
}

func (s *FullTextService) buildIndex(ctx types.TaskCtx, driveName string, force bool) error {
	now := types.NowMillis()
	state := &types.IndexJobState{
		Drive:         driveName,
		Status:        types.IndexStatusRunning,
		LastUpdatedAt: now,
	}

	existingState, err := s.jobStateDAO.GetByDrive(driveName)
	if err != nil {
		return err
	}

	if existingState != nil && existingState.Status == types.IndexStatusPaused && !force {
		state.TotalFiles = existingState.TotalFiles
		state.ScannedFiles = existingState.ScannedFiles
		state.IndexedFiles = existingState.IndexedFiles
		state.FailedFiles = existingState.FailedFiles
		state.StartedAt = existingState.StartedAt
		state.CurrentPath = existingState.CurrentPath
	} else {
		state.StartedAt = now
	}

	if err := s.jobStateDAO.CreateOrUpdate(state); err != nil {
		return err
	}

	dispatcher := s.rootDrive.Get()

	rootPath := driveName + "/"
	startPath := rootPath
	if state.CurrentPath != "" {
		if !strings.HasPrefix(state.CurrentPath, rootPath) {
			startPath = rootPath + strings.TrimPrefix(state.CurrentPath, "/")
		} else {
			startPath = state.CurrentPath
		}
	}

	ctx.Total(state.TotalFiles, true)
	ctx.Progress(state.ScannedFiles, true)

	walked := make(map[string]bool)
	batch := make([]types.IEntry, 0, fullTextIndexBatchSize)

	processBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		for _, entry := range batch {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if err := s.indexEntry(ctx, driveName, entry, force); err != nil {
				log.Printf("[FullTextService] failed to index %s: %v", entry.Path(), err)
				state.FailedFiles++
			} else {
				state.IndexedFiles++
			}
			state.ScannedFiles++
			state.CurrentPath = entry.Path()

			ctx.Total(state.TotalFiles, true)
			ctx.Progress(state.ScannedFiles, true)

			if err := s.jobStateDAO.UpdateProgress(
				driveName, state.CurrentPath,
				state.TotalFiles, state.ScannedFiles,
				state.IndexedFiles, state.FailedFiles,
			); err != nil {
				log.Printf("[FullTextService] failed to update progress: %v", err)
			}
		}

		batch = batch[:0]
		return nil
	}

	err = s.walkDrive(ctx, dispatcher, startPath, func(entry types.IEntry) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if walked[entry.Path()] {
			return nil
		}
		walked[entry.Path()] = true
		state.TotalFiles++

		if entry.Type() == types.TypeFile {
			batch = append(batch, entry)
			if len(batch) >= fullTextIndexBatchSize {
				if err := processBatch(); err != nil {
					return err
				}
			}
		}
		return nil
	})

	if err != nil {
		if ctx.Err() != nil {
			s.jobStateDAO.UpdateStatus(driveName, types.IndexStatusPaused)
			return ctx.Err()
		}
		s.jobStateDAO.UpdateStatus(driveName, types.IndexStatusFailed, err.Error())
		return err
	}

	if err := processBatch(); err != nil {
		s.jobStateDAO.UpdateStatus(driveName, types.IndexStatusFailed, err.Error())
		return err
	}

	state.Status = types.IndexStatusCompleted
	state.CurrentPath = ""
	state.LastUpdatedAt = types.NowMillis()
	return s.jobStateDAO.CreateOrUpdate(state)
}

func (s *FullTextService) indexEntry(ctx context.Context, driveName string, entry types.IEntry, force bool) error {
	entryPath := entry.Path()
	prefix := driveName + "/"
	if strings.HasPrefix(entryPath, prefix) {
		entryPath = strings.TrimPrefix(entryPath, prefix)
	}

	pathHash := types.HashPath(driveName, entryPath)

	existing, err := s.ftIndexDAO.GetByPathHash(pathHash)
	if err == nil && existing != nil {
		if !force && existing.ModTime == entry.ModTime() {
			existing.LastIndexedAt = types.NowMillis()
			return s.ftIndexDAO.UpdateContent(pathHash, existing.Content, existing.ContentHash)
		}
	}

	mimeType := DetectMIME(entry.Name())
	ftIndex := &types.FullTextIndex{
		Drive:         driveName,
		PathHash:      pathHash,
		Path:          entryPath,
		Name:          filepath.Base(entryPath),
		Ext:           utils.PathExt(entry.Name()),
		MimeType:      mimeType,
		Size:          entry.Size(),
		ModTime:       entry.ModTime(),
		LastIndexedAt: types.NowMillis(),
		Indexed:       false,
	}

	parseResult := ParseEntryContent(ctx, entry, s.maxContentSize)
	if parseResult.Error != nil {
		ftIndex.ErrorMsg = parseResult.Error.Error()
		return s.ftIndexDAO.Save(ftIndex)
	}

	ftIndex.MimeType = parseResult.MimeType
	ftIndex.Content = parseResult.Content
	ftIndex.ContentHash = types.HashContent(parseResult.Content)
	ftIndex.Indexed = true

	if existing != nil {
		return s.ftIndexDAO.UpdateContent(pathHash, ftIndex.Content, ftIndex.ContentHash)
	}
	return s.ftIndexDAO.Save(ftIndex)
}

func (s *FullTextService) walkDrive(ctx context.Context, d types.IDrive, startPath string, visit func(types.IEntry) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	entry, err := d.Get(ctx, startPath)
	if err != nil {
		return err
	}

	if startPath == "" || !strings.HasPrefix(entry.Path(), startPath) {
		if err := visit(entry); err != nil {
			return err
		}
	}

	if entry.Type() == types.TypeDir {
		entries, err := d.List(ctx, entry.Path())
		if err != nil {
			return err
		}
		for _, child := range entries {
			if err := s.walkDrive(ctx, d, child.Path(), visit); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *FullTextService) Search(ctx context.Context, driveName, query string, offset, limit int, perms utils.PermMap) (*FullTextSearchResult, error) {
	parsedQuery := ParseSearchQuery(query)

	var (
		items []types.FullTextIndex
		total int64
		err   error
	)

	if parsedQuery.ContentQuery != "" && parsedQuery.NameQuery != "" {
		if parsedQuery.Operator == OpOR {
			items, total, err = s.ftIndexDAO.SearchByNameOrContent(driveName, parsedQuery.NameQuery, parsedQuery.ContentQuery, offset, limit)
		} else {
			items, total, err = s.ftIndexDAO.SearchByNameAndContent(driveName, parsedQuery.NameQuery, parsedQuery.ContentQuery, offset, limit)
		}
	} else if parsedQuery.ContentQuery != "" {
		items, total, err = s.ftIndexDAO.SearchByContent(driveName, parsedQuery.ContentQuery, offset, limit)
	} else if parsedQuery.NameQuery != "" {
		items, total, err = s.ftIndexDAO.SearchByName(driveName, parsedQuery.NameQuery, offset, limit)
	} else {
		items, total, err = s.ftIndexDAO.ListAll(driveName, offset, limit)
	}

	if err != nil {
		return nil, err
	}

	filteredItems := make([]types.FullTextIndex, 0, len(items))
	for _, item := range items {
		fullPath := item.Drive + ":" + item.Path
		p := perms.ResolvePath(fullPath)
		if p.Readable() {
			filteredItems = append(filteredItems, item)
		}
	}

	return &FullTextSearchResult{
		Items: filteredItems,
		Total: total,
		Query: parsedQuery,
	}, nil
}

func (s *FullTextService) DeleteIndex(driveName, path string) error {
	return s.ftIndexDAO.DeleteByPath(driveName, path)
}

func (s *FullTextService) ClearIndex(driveName string) error {
	if err := s.ftIndexDAO.DeleteByDrive(driveName); err != nil {
		return err
	}
	return s.jobStateDAO.DeleteByDrive(driveName)
}

func (s *FullTextService) GetStats() (*FullTextStats, error) {
	daoStats, err := s.ftIndexDAO.GetStats()
	if err != nil {
		return nil, err
	}
	return &FullTextStats{
		FullTextIndexStats: *daoStats,
	}, nil
}

type FullTextStats struct {
	storage.FullTextIndexStats
}

type FullTextSearchResult struct {
	Items []types.FullTextIndex   `json:"items"`
	Total int64                   `json:"total"`
	Query *ParsedSearchQuery      `json:"query"`
}

type SearchOperator int

const (
	OpAND SearchOperator = iota
	OpOR
	OpNOT
)

type ParsedSearchQuery struct {
	RawQuery     string         `json:"rawQuery"`
	NameQuery    string         `json:"nameQuery,omitempty"`
	ContentQuery string         `json:"contentQuery,omitempty"`
	Scope        string         `json:"scope,omitempty"`
	Operator     SearchOperator `json:"operator"`
	Exclusions   []string       `json:"exclusions,omitempty"`
}

func ParseSearchQuery(query string) *ParsedSearchQuery {
	result := &ParsedSearchQuery{
		RawQuery: query,
		Operator: OpAND,
	}

	tokens := strings.Fields(query)
	var nameParts, contentParts, exclusions []string

	for _, token := range tokens {
		switch {
		case strings.HasPrefix(token, "name:"):
			nameParts = append(nameParts, strings.TrimPrefix(token, "name:"))
		case strings.HasPrefix(token, "content:"):
			contentParts = append(contentParts, strings.TrimPrefix(token, "content:"))
		case strings.HasPrefix(token, "scope:"):
			result.Scope = strings.TrimPrefix(token, "scope:")
		case strings.HasPrefix(token, "NOT "):
			exclusions = append(exclusions, strings.TrimPrefix(token, "NOT "))
		case strings.HasPrefix(token, "OR"):
			result.Operator = OpOR
		case strings.HasPrefix(token, "AND"):
			result.Operator = OpAND
		default:
			if len(contentParts) == 0 && len(nameParts) == 0 {
				nameParts = append(nameParts, token)
				contentParts = append(contentParts, token)
			} else {
				contentParts = append(contentParts, token)
			}
		}
	}

	result.NameQuery = strings.Join(nameParts, " ")
	result.ContentQuery = strings.Join(contentParts, " ")
	result.Exclusions = exclusions

	return result
}

var _ types.ISysConfig = (*FullTextService)(nil)

func (s *FullTextService) SysConfig() (string, types.M, error) {
	stats, _ := s.GetStats()
	return "fulltext", types.M{
		"enabled":         true,
		"maxContentSize":  s.maxContentSize,
		"stats":           stats,
		"supportedQueries": []string{
			"name:xxx",
			"content:xxx",
			"name:xxx AND content:xxx",
			"name:xxx OR content:xxx",
			"scope:driveName",
		},
	}, nil
}
