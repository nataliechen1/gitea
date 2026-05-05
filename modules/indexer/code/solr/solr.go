// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package solr

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	repo_model "code.gitea.io/gitea/models/repo"
	"code.gitea.io/gitea/modules/analyze"
	"code.gitea.io/gitea/modules/charset"
	"code.gitea.io/gitea/modules/git"
	"code.gitea.io/gitea/modules/git/gitcmd"
	"code.gitea.io/gitea/modules/gitrepo"
	"code.gitea.io/gitea/modules/indexer"
	"code.gitea.io/gitea/modules/indexer/code/internal"
	"code.gitea.io/gitea/modules/indexer/internal/solr"
	"code.gitea.io/gitea/modules/setting"
	"code.gitea.io/gitea/modules/timeutil"
	"code.gitea.io/gitea/modules/typesniffer"
	"code.gitea.io/gitea/modules/util"

	"github.com/go-enry/go-enry/v2"
)

const solrRepoIndexerLatestVersion = 1

var _ internal.Indexer = &Indexer{}

type Indexer struct {
	*solr.Indexer
}

func (b *Indexer) SupportedSearchModes() []indexer.SearchMode {
	return indexer.SearchModesExactWords()
}

func NewIndexer(url, indexerName string) *Indexer {
	return &Indexer{Indexer: solr.NewIndexer(url, indexerName)}
}

func (b *Indexer) addUpdate(ctx context.Context, catFileBatch git.CatFileBatch, sha string, update internal.FileUpdate, repo *repo_model.Repository) ([]solr.BulkOp, error) {
	// Ignore vendored files in code search
	if setting.Indexer.ExcludeVendored && analyze.IsVendor(update.Filename) {
		return nil, nil
	}

	size := update.Size
	var err error
	if !update.Sized {
		var stdout string
		stdout, _, err = gitrepo.RunCmdString(ctx, repo, gitcmd.NewCommand("cat-file", "-s").AddDynamicArguments(update.BlobSha))
		if err != nil {
			return nil, err
		}
		if size, err = strconv.ParseInt(strings.TrimSpace(stdout), 10, 64); err != nil {
			return nil, fmt.Errorf("misformatted git cat-file output: %w", err)
		}
	}

	id := internal.FilenameIndexerID(repo.ID, update.Filename)
	if size > setting.Indexer.MaxIndexerFileSize {
		return []solr.BulkOp{solr.DeleteOp(id)}, nil
	}

	info, batchReader, err := catFileBatch.QueryContent(update.BlobSha)
	if err != nil {
		return nil, err
	}

	fileContents, err := io.ReadAll(io.LimitReader(batchReader, info.Size))
	if err != nil {
		return nil, err
	} else if !typesniffer.DetectContentType(fileContents).IsText() {
		// FIXME: UTF-16 files will probably fail here
		return nil, nil
	}

	if _, err = batchReader.Discard(1); err != nil {
		return nil, err
	}

	doc := map[string]any{
		"repo_id":    repo.ID,
		"filename":   update.Filename,
		"content":    string(charset.ToUTF8DropErrors(fileContents)),
		"commit_id":  sha,
		"language":   analyze.GetCodeLanguage(update.Filename, fileContents),
		"updated_at": timeutil.TimeStampNow(),
	}
	return []solr.BulkOp{solr.IndexOp(id, doc)}, nil
}

func (b *Indexer) addDelete(filename string, repo *repo_model.Repository) solr.BulkOp {
	return solr.DeleteOp(internal.FilenameIndexerID(repo.ID, filename))
}

func (b *Indexer) Index(ctx context.Context, repo *repo_model.Repository, sha string, changes *internal.RepoChanges) error {
	op := make([]solr.BulkOp, 0)
	if len(changes.Updates) > 0 {
		batch, err := gitrepo.NewBatch(ctx, repo)
		if err != nil {
			return err
		}
		defer batch.Close()

		for _, update := range changes.Updates {
			updateOps, err := b.addUpdate(ctx, batch, sha, update, repo)
			if err != nil {
				return err
			}
			if len(updateOps) > 0 {
				op = append(op, updateOps...)
			}
		}
	}

	for _, filename := range changes.RemovedFilenames {
		op = append(op, b.addDelete(filename, repo))
	}

	if len(op) > 0 {
		esBatchSize := 50
		for i := 0; i < len(op); i += esBatchSize {
			if err := b.Bulk(ctx, op[i:min(i+esBatchSize, len(op))]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Indexer) Delete(ctx context.Context, repoID int64) error {
	return b.DeleteByQuery(ctx, fmt.Sprintf("repo_id:%d", repoID))
}

func (b *Indexer) Search(ctx context.Context, opts *internal.SearchOptions) (int64, []*internal.SearchResult, []*internal.SearchResultLanguages, error) {
	params := url.Values{}
	params.Set("wt", "json")
	params.Set("defType", "edismax")
	params.Set("q.op", "AND")
	params.Set("fl", "repo_id,filename,commit_id,content,updated_at,language")
	params.Set("hl", "true")
	params.Set("hl.fl", "content,filename")
	params.Set("hl.simple.pre", "<em>")
	params.Set("hl.simple.post", "</em>")
	params.Set("hl.requireFieldMatch", "false")
	params.Set("facet", "true")
	params.Set("facet.field", "language")
	params.Set("facet.limit", "10")
	params.Set("facet.sort", "count")

	if opts.Keyword == "" {
		params.Set("q", "*:*")
	} else {
		params.Set("q", opts.Keyword)
		params.Set("qf", "content filename^10")
		if util.IfZero(opts.SearchMode, b.SupportedSearchModes()[0].ModeValue) == indexer.SearchModeExact {
			params.Set("pf", "content filename^10")
		}
	}

	if len(opts.RepoIDs) > 0 {
		params.Add("fq", buildIntegerFilter("repo_id", opts.RepoIDs))
	}
	if opts.Language != "" {
		params.Add("fq", fmt.Sprintf("language:%s", escapeSolrTerm(opts.Language)))
	}

	start, pageSize := opts.GetSkipTake()
	params.Set("start", strconv.Itoa(start))
	params.Set("rows", strconv.Itoa(pageSize))
	params.Set("sort", "_score desc,updated_at asc")

	var raw struct {
		Response struct {
			NumFound int64 `json:"numFound"`
			Docs     []struct {
				ID        string  `json:"id"`
				RepoID    int64   `json:"repo_id"`
				Filename  string  `json:"filename"`
				CommitID  string  `json:"commit_id"`
				Content   string  `json:"content"`
				UpdatedAt float64 `json:"updated_at"`
				Language  string  `json:"language"`
			} `json:"docs"`
		} `json:"response"`
		Highlighting map[string]map[string][]string `json:"highlighting"`
		FacetCounts struct {
			FacetFields map[string][]any `json:"facet_fields"`
		} `json:"facet_counts"`
	}

	if err := b.Indexer.Search(ctx, params, &raw); err != nil {
		return 0, nil, nil, err
	}

	hits := make([]*internal.SearchResult, 0, len(raw.Response.Docs))
	for _, doc := range raw.Response.Docs {
		startIndex, endIndex := internal.FilenameMatchIndexPos(doc.Content)
		hl, hasHighlight := raw.Highlighting[doc.ID]
		if hasHighlight {
			if c, ok := hl["filename"]; ok && len(c) > 0 {
				startIndex, endIndex = internal.FilenameMatchIndexPos(doc.Content)
			} else if c, ok := hl["content"]; ok && len(c) > 0 {
				if s, e := contentMatchIndexPos(c[0], "<em>", "</em>"); s >= 0 {
					startIndex, endIndex = s, e
				}
			}
		}
		hits = append(hits, &internal.SearchResult{
			RepoID:      doc.RepoID,
			Filename:    doc.Filename,
			CommitID:    doc.CommitID,
			Content:     doc.Content,
			UpdatedUnix: timeutil.TimeStamp(doc.UpdatedAt),
			Language:    doc.Language,
			StartIndex:  startIndex,
			EndIndex:    endIndex,
			Color:       enry.GetColor(doc.Language),
		})
	}

	languageAggs := make([]*internal.SearchResultLanguages, 0, 10)
	if buckets, ok := raw.FacetCounts.FacetFields["language"]; ok {
		for i := 0; i < len(buckets); i += 2 {
			language, ok := buckets[i].(string)
			if !ok {
				continue
			}
			count, err := parseBucketCount(buckets[i+1])
			if err != nil {
				continue
			}
			languageAggs = append(languageAggs, &internal.SearchResultLanguages{
				Language: language,
				Color:    enry.GetColor(language),
				Count:    int(count),
			})
		}
	}

	return raw.Response.NumFound, hits, languageAggs, nil
}

func buildIntegerFilter(field string, values []int64) string {
	parts := make([]string, 0, len(values))
	for _, v := range values {
		parts = append(parts, strconv.FormatInt(v, 10))
	}
	return fmt.Sprintf("%s:(%s)", field, strings.Join(parts, " "))
}

func escapeSolrTerm(value string) string {
	var b strings.Builder
	special := `+\-!(){}[]^"~*?:/`
	for _, r := range value {
		if strings.ContainsRune(special, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	escaped := b.String()
	if strings.ContainsAny(value, " \t\n\r") {
		escaped = `"` + strings.ReplaceAll(escaped, `"`, `\\"`) + `"`
	}
	return escaped
}

func parseBucketCount(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected bucket count type: %T", v)
	}
}

func contentMatchIndexPos(content, start, end string) (int, int) {
	startIdx := strings.Index(content, start)
	if startIdx < 0 {
		return -1, -1
	}
	endIdx := strings.Index(content[startIdx+len(start):], end)
	if endIdx < 0 {
		return -1, -1
	}
	return startIdx, (startIdx + len(start) + endIdx + len(end)) - 9
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
