package search

import (
	"context"
	"fmt"
	"go-drive/common/types"
	"io"
	"mime"
	"path/filepath"
	"strings"
)

type ContentParser interface {
	CanParse(mimeType string) bool
	Parse(ctx context.Context, entry types.IEntry, maxSize int64) (string, error)
	Name() string
}

var parserRegistry = make(map[string]ContentParser)

func RegisterContentParser(p ContentParser) {
	parserRegistry[p.Name()] = p
}

func GetContentParserForMIME(mimeType string) ContentParser {
	for _, p := range parserRegistry {
		if p.CanParse(mimeType) {
			return p
		}
	}
	return nil
}

func DetectMIME(name string) string {
	ext := strings.ToLower(filepath.Ext(name))
	if ext == "" {
		return "application/octet-stream"
	}
	mimeType := mime.TypeByExtension(ext)
	if mimeType == "" {
		switch ext {
		case ".txt", ".md", ".markdown", ".log", ".csv", ".json", ".xml", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".conf":
			mimeType = "text/plain"
		case ".go", ".py", ".js", ".ts", ".tsx", ".jsx", ".java", ".c", ".cpp", ".h", ".hpp", ".cs", ".rb", ".php", ".swift", ".kt", ".rs", ".scala", ".lua", ".sh", ".bash", ".zsh", ".fish", ".ps1", ".bat", ".cmd":
			mimeType = "text/plain"
		case ".html", ".htm", ".css", ".scss", ".sass", ".less":
			mimeType = "text/plain"
		case ".sql":
			mimeType = "text/plain"
		case ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".svg", ".tiff":
			mimeType = "image/*"
		case ".pdf":
			mimeType = "application/pdf"
		case ".doc", ".docx":
			mimeType = "application/msword"
		case ".xls", ".xlsx":
			mimeType = "application/vnd.ms-excel"
		case ".ppt", ".pptx":
			mimeType = "application/vnd.ms-powerpoint"
		default:
			mimeType = "application/octet-stream"
		}
	}
	return mimeType
}

type TextContentParser struct{}

func init() {
	RegisterContentParser(&TextContentParser{})
	RegisterContentParser(&OCRContentParser{})
}

func (p *TextContentParser) Name() string {
	return "text"
}

func (p *TextContentParser) CanParse(mimeType string) bool {
	return strings.HasPrefix(mimeType, "text/") ||
		mimeType == "application/json" ||
		mimeType == "application/xml" ||
		mimeType == "application/x-yaml" ||
		mimeType == "application/yaml" ||
		strings.Contains(mimeType, "javascript") ||
		strings.Contains(mimeType, "typescript")
}

func (p *TextContentParser) Parse(ctx context.Context, entry types.IEntry, maxSize int64) (string, error) {
	if entry.Size() > maxSize && maxSize > 0 {
		return "", fmt.Errorf("file too large: %d bytes (max: %d)", entry.Size(), maxSize)
	}

	reader, err := entry.GetReader(ctx, 0, maxSize)
	if err != nil {
		return "", fmt.Errorf("failed to get reader: %w", err)
	}
	defer reader.Close()

	content, err := io.ReadAll(io.LimitReader(reader, maxSize))
	if err != nil {
		return "", fmt.Errorf("failed to read content: %w", err)
	}

	return strings.TrimSpace(string(content)), nil
}

type OCRContentParser struct{}

func (p *OCRContentParser) Name() string {
	return "ocr"
}

func (p *OCRContentParser) CanParse(mimeType string) bool {
	return strings.HasPrefix(mimeType, "image/")
}

func (p *OCRContentParser) Parse(ctx context.Context, entry types.IEntry, maxSize int64) (string, error) {
	return "[OCR placeholder: image content indexing requires OCR service]", nil
}

type ContentParseResult struct {
	Content    string
	MimeType   string
	ParserUsed string
	Error      error
}

func ParseEntryContent(ctx context.Context, entry types.IEntry, maxSize int64) ContentParseResult {
	mimeType := DetectMIME(entry.Name())
	parser := GetContentParserForMIME(mimeType)

	if parser == nil {
		return ContentParseResult{
			MimeType: mimeType,
			Error:    fmt.Errorf("no parser available for MIME type: %s", mimeType),
		}
	}

	content, err := parser.Parse(ctx, entry, maxSize)
	return ContentParseResult{
		Content:    content,
		MimeType:   mimeType,
		ParserUsed: parser.Name(),
		Error:      err,
	}
}
