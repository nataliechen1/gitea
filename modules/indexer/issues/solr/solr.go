// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package solr

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"code.gitea.io/gitea/modules/indexer"
	indexer_internal "code.gitea.io/gitea/modules/indexer/internal"
	"code.gitea.io/gitea/modules/indexer/internal/solr"
	"code.gitea.io/gitea/modules/indexer/issues/internal"
	"code.gitea.io/gitea/modules/optional"
	"code.gitea.io/gitea/modules/util"
)


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

func (b *Indexer) Index(ctx context.Context, issues ...*internal.IndexerData) error {
	if len(issues) == 0 {
		return nil
	}
	if len(issues) == 1 {
		issue := issues[0]
		return b.Indexer.Index(ctx, strconv.FormatInt(issue.ID, 10), issue)
	}
	ops := make([]solr.BulkOp, 0, len(issues))
	for _, issue := range issues {
		ops = append(ops, solr.IndexOp(strconv.FormatInt(issue.ID, 10), issue))
	}
	return b.Bulk(ctx, ops)
}

func (b *Indexer) Delete(ctx context.Context, ids ...int64) error {
	if len(ids) == 0 {
		return nil
	}
	if len(ids) == 1 {
		return b.Indexer.Delete(ctx, strconv.FormatInt(ids[0], 10))
	}
	ops := make([]solr.BulkOp, 0, len(ids))
	for _, id := range ids {
		ops = append(ops, solr.DeleteOp(strconv.FormatInt(id, 10)))
	}
	return b.Bulk(ctx, ops)
}

func (b *Indexer) Search(ctx context.Context, options *internal.SearchOptions) (*internal.SearchResult, error) {
	params := url.Values{}
	params.Set("wt", "json")
	params.Set("defType", "edismax")
	params.Set("qf", "title^3 content comments")
	params.Set("q.op", "AND")

	if options.Keyword == "" {
		params.Set("q", "*:*")
	} else {
		params.Set("q", options.Keyword)
		if util.IfZero(options.SearchMode, b.SupportedSearchModes()[0].ModeValue) == indexer.SearchModeExact {
			params.Set("pf", "title^3 content comments")
		}
	}

	if len(options.RepoIDs) > 0 {
		repoFilter := buildIntegerFilter("repo_id", options.RepoIDs)
		if options.AllPublic {
			params.Add("fq", fmt.Sprintf("(%s OR is_public:true)", repoFilter))
		} else {
			params.Add("fq", repoFilter)
		}
	} else if options.AllPublic {
		params.Add("fq", "is_public:true")
	}

	if options.IsPull.Has() {
		params.Add("fq", fmt.Sprintf("is_pull:%t", options.IsPull.Value()))
	}
	if options.IsClosed.Has() {
		params.Add("fq", fmt.Sprintf("is_closed:%t", options.IsClosed.Value()))
	}
	if options.IsArchived.Has() {
		params.Add("fq", fmt.Sprintf("is_archived:%t", options.IsArchived.Value()))
	}

	if options.NoLabelOnly {
		params.Add("fq", "no_label:true")
	} else {
		for _, labelID := range options.IncludedLabelIDs {
			params.Add("fq", fmt.Sprintf("label_ids:%d", labelID))
		}
		if len(options.IncludedAnyLabelIDs) > 0 {
			params.Add("fq", buildIntegerFilter("label_ids", options.IncludedAnyLabelIDs))
		}
		for _, labelID := range options.ExcludedLabelIDs {
			params.Add("fq", fmt.Sprintf("-label_ids:%d", labelID))
		}
	}

	if len(options.MilestoneIDs) > 0 {
		params.Add("fq", buildIntegerFilter("milestone_id", options.MilestoneIDs))
	}

	if options.NoProjectOnly {
		params.Add("fq", "no_project:true")
	} else if len(options.ProjectIDs) > 0 {
		params.Add("fq", buildIntegerFilter("project_ids", options.ProjectIDs))
	}

	if options.PosterID != "" {
		posterIDInt64, _ := strconv.ParseInt(options.PosterID, 10, 64)
		params.Add("fq", fmt.Sprintf("poster_id:%d", posterIDInt64))
	}

	if options.AssigneeID != "" {
		if options.AssigneeID == "(any)" {
			params.Add("fq", "assignee_id:[1 TO *]")
		} else {
			assigneeIDInt64, _ := strconv.ParseInt(options.AssigneeID, 10, 64)
			params.Add("fq", fmt.Sprintf("assignee_id:%d", assigneeIDInt64))
		}
	}

	if options.MentionID.Has() {
		params.Add("fq", fmt.Sprintf("mention_ids:%d", options.MentionID.Value()))
	}

	if options.ReviewedID.Has() {
		params.Add("fq", fmt.Sprintf("reviewed_ids:%d", options.ReviewedID.Value()))
	}
	if options.ReviewRequestedID.Has() {
		params.Add("fq", fmt.Sprintf("review_requested_ids:%d", options.ReviewRequestedID.Value()))
	}

	if options.SubscriberID.Has() {
		params.Add("fq", fmt.Sprintf("subscriber_ids:%d", options.SubscriberID.Value()))
	}

	if options.UpdatedAfterUnix.Has() || options.UpdatedBeforeUnix.Has() {
		filter := buildRangeFilter("updated_unix", options.UpdatedAfterUnix, options.UpdatedBeforeUnix)
		params.Add("fq", filter)
	}

	if options.SortBy == "" {
		options.SortBy = internal.SortByCreatedAsc
	}
	params.Set("sort", fmt.Sprintf("%s,id desc", parseSortBy(options.SortBy)))

	const maxPageSize = 10000
	skip, limit := indexer_internal.ParsePaginator(options.Paginator, maxPageSize)
	params.Set("start", strconv.Itoa(skip))
	params.Set("rows", strconv.Itoa(limit))

	var raw struct {
		Response struct {
			NumFound int64             `json:"numFound"`
			Docs     []map[string]any `json:"docs"`
		} `json:"response"`
	}

	if err := b.Indexer.Search(ctx, params, &raw); err != nil {
		return nil, err
	}

	hits := make([]internal.Match, 0, len(raw.Response.Docs))
	for _, doc := range raw.Response.Docs {
		idValue, ok := doc["id"]
		if !ok {
			continue
		}
		id, err := parseInt64(idValue)
		if err != nil {
			return nil, err
		}
		hits = append(hits, internal.Match{ID: id})
	}

	return &internal.SearchResult{Total: raw.Response.NumFound, Hits: hits}, nil
}

func parseSortBy(sortBy internal.SortBy) string {
	field, desc := strings.CutPrefix(string(sortBy), "-")
	if desc {
		return field + " desc"
	}
	return field + " asc"
}

func buildIntegerFilter(field string, values []int64) string {
	parts := make([]string, 0, len(values))
	for _, v := range values {
		parts = append(parts, strconv.FormatInt(v, 10))
	}
	return fmt.Sprintf("%s:(%s)", field, strings.Join(parts, " "))
}

func buildRangeFilter(field string, after, before optional.Option[int64]) string {
	low := "*"
	high := "*"
	if after.Has() {
		low = strconv.FormatInt(after.Value(), 10)
	}
	if before.Has() {
		high = strconv.FormatInt(before.Value(), 10)
	}
	return fmt.Sprintf("%s:[%s TO %s]", field, low, high)
}

func parseInt64(value any) (int64, error) {
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
		return 0, fmt.Errorf("unexpected numeric value: %T", v)
	}
}
