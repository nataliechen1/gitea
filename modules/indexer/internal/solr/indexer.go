// Copyright 2026 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package solr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"code.gitea.io/gitea/modules/json"
)

const (
	bulkActionIndex  = "index"
	bulkActionDelete = "delete"
)

// Indexer is a narrow wrapper around a Solr/SolrCloud collection.
// It supports the subset of operations needed by Gitea's issue and code indexers.
// The connection URL should point to the Solr root (for example, http://solr:8983/solr) and the collection
// name is provided separately.
type Indexer struct {
	client     *http.Client
	base       string
	user       string
	pass       string
	collection string
}

// BulkOp is a single update/delete operation for Solr.
type BulkOp struct {
	action string
	id     string
	doc    any
}

// IndexOp creates a bulk index operation.
func IndexOp(id string, doc any) BulkOp {
	return BulkOp{action: bulkActionIndex, id: id, doc: doc}
}

// DeleteOp creates a bulk delete operation.
func DeleteOp(id string) BulkOp {
	return BulkOp{action: bulkActionDelete, id: id}
}

// NewIndexer builds a new solr indexer.
func NewIndexer(rawURL, collection string) *Indexer {
	return &Indexer{base: rawURL, collection: collection}
}

// Init connects to the Solr root and creates the collection if it does not exist.
func (i *Indexer) Init(ctx context.Context) (bool, error) {
	parsed, err := url.Parse(i.base)
	if err != nil {
		return false, fmt.Errorf("parse solr url: %w", err)
	}
	if parsed.User != nil {
		i.user = parsed.User.Username()
		i.pass, _ = parsed.User.Password()
		parsed.User = nil
	}

	base := parsed.String()
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	i.base = base
	i.collection = strings.TrimPrefix(strings.TrimSpace(i.collection), "/")
	if i.collection == "" {
		return false, errors.New("solr collection name is required")
	}

	if strings.HasSuffix(parsed.Path, "/"+i.collection) {
		parsed.Path = strings.TrimSuffix(parsed.Path, "/"+i.collection)
		base = parsed.String()
		if !strings.HasSuffix(base, "/") {
			base += "/"
		}
		i.base = base
	}

	i.client = &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConns:          100,
		},
	}

	exists, err := i.collectionExists(ctx)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}

	if err := i.createCollection(ctx); err != nil {
		return false, err
	}
	return false, nil
}

// Ping checks if Solr is available.
func (i *Indexer) Ping(ctx context.Context) error {
	var body struct {
		Status string `json:"status"`
	}
	if err := i.doJSON(ctx, http.MethodGet, urlPath(i.collection, "admin", "ping")+"?wt=json", nil, &body); err != nil {
		return err
	}
	if strings.ToUpper(body.Status) != "OK" && strings.ToUpper(body.Status) != "NONE" {
		return fmt.Errorf("status of solr cluster is %s", body.Status)
	}
	return nil
}

// Close closes idle HTTP connections.
func (i *Indexer) Close() {
	if i == nil || i.client == nil {
		return
	}
	i.client.CloseIdleConnections()
	i.client = nil
}

// Bulk sends a batch of index/delete operations to Solr.
func (i *Indexer) Bulk(ctx context.Context, ops []BulkOp) error {
	if len(ops) == 0 {
		return nil
	}
	items := make([]any, 0, len(ops))
	for _, op := range ops {
		if op.action == bulkActionDelete {
			items = append(items, map[string]any{"delete": map[string]any{"id": op.id}})
			continue
		}
		doc := op.doc
		if m, ok := doc.(map[string]any); ok {
			cloned := make(map[string]any, len(m)+1)
			for k, v := range m {
				cloned[k] = v
			}
			cloned["id"] = op.id
			doc = cloned
		}
		items = append(items, doc)
	}

	data, err := json.Marshal(items)
	if err != nil {
		return err
	}
	return i.doJSON(ctx, http.MethodPost, urlPath(i.collection, "update")+"?commitWithin=1000&wt=json", bytes.NewReader(data), nil)
}

// Index writes a single document.
func (i *Indexer) Index(ctx context.Context, id string, doc any) error {
	return i.Bulk(ctx, []BulkOp{IndexOp(id, doc)})
}

// Delete removes a single document by ID.
func (i *Indexer) Delete(ctx context.Context, id string) error {
	data, err := json.Marshal(map[string]any{"delete": map[string]any{"id": id}})
	if err != nil {
		return err
	}
	return i.doJSON(ctx, http.MethodPost, urlPath(i.collection, "update")+"?commitWithin=1000&wt=json", bytes.NewReader(data), nil)
}

// DeleteByQuery removes every document matching the given query.
func (i *Indexer) DeleteByQuery(ctx context.Context, query string) error {
	data, err := json.Marshal(map[string]any{"delete": map[string]any{"query": query}})
	if err != nil {
		return err
	}
	return i.doJSON(ctx, http.MethodPost, urlPath(i.collection, "update")+"?commitWithin=1000&wt=json", bytes.NewReader(data), nil)
}

// Refresh commits pending updates to make them visible to searches.
func (i *Indexer) Refresh(ctx context.Context) error {
	data, err := json.Marshal(map[string]any{"commit": map[string]any{}})
	if err != nil {
		return err
	}
	return i.doJSON(ctx, http.MethodPost, urlPath(i.collection, "update")+"?commit=true&wt=json", bytes.NewReader(data), nil)
}

// Search performs a Solr select query and decodes the response.
func (i *Indexer) Search(ctx context.Context, params url.Values, out any) error {
	if params == nil {
		params = url.Values{}
	}
	params.Set("wt", "json")
	res, err := i.do(ctx, http.MethodGet, urlPath(i.collection, "select")+"?"+params.Encode(), "", nil)
	if err != nil {
		return err
	}
	defer drainAndClose(res)
	return json.NewDecoder(res.Body).Decode(out)
}

func (i *Indexer) collectionExists(ctx context.Context) (bool, error) {
	res, err := i.do(ctx, http.MethodGet, urlPath(i.collection, "admin", "ping")+"?wt=json", "", nil, http.StatusNotFound)
	if err != nil {
		return false, err
	}
	if res.StatusCode == http.StatusNotFound {
		drainAndClose(res)
		return false, nil
	}
	defer drainAndClose(res)
	return true, nil
}

func (i *Indexer) createCollection(ctx context.Context) error {
	query := url.Values{}
	query.Set("action", "CREATE")
	query.Set("name", i.collection)
	query.Set("numShards", "1")
	query.Set("replicationFactor", "1")
	query.Set("collection.configName", "_default")
	query.Set("wt", "json")

	res, err := i.do(ctx, http.MethodGet, urlPath("admin", "collections")+"?"+query.Encode(), "", nil)
	if err != nil {
		return err
	}
	defer drainAndClose(res)

	var raw struct {
		ResponseHeader struct {
			Status int `json:"status"`
		} `json:"responseHeader"`
		Error any `json:"error"`
	}
	if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
		return fmt.Errorf("decode create collection response: %w", err)
	}
	if raw.ResponseHeader.Status != 0 {
		return fmt.Errorf("create collection failed: %v", raw.Error)
	}
	return nil
}

func (i *Indexer) do(ctx context.Context, method, path, contentType string, body io.Reader, okStatus ...int) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, i.base+path, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if i.user != "" || i.pass != "" {
		req.SetBasicAuth(i.user, i.pass)
	}
	res, err := i.client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 300 && !containsStatus(okStatus, res.StatusCode) {
		msg := readErrBody(res)
		res.Body.Close()
		return nil, fmt.Errorf("%s %s: %s", method, path, msg)
	}
	return res, nil
}

func (i *Indexer) doJSON(ctx context.Context, method, path string, body io.Reader, out any, okStatus ...int) error {
	contentType := ""
	if body != nil {
		contentType = "application/json"
	}
	res, err := i.do(ctx, method, path, contentType, body, okStatus...)
	if err != nil {
		return err
	}
	defer drainAndClose(res)
	if out == nil {
		return nil
	}
	return json.NewDecoder(res.Body).Decode(out)
}

func drainAndClose(res *http.Response) {
	_, _ = io.Copy(io.Discard, res.Body)
	res.Body.Close()
}

func readErrBody(res *http.Response) string {
	const limit = 4 << 10
	b, _ := io.ReadAll(io.LimitReader(res.Body, limit))
	_, _ = io.Copy(io.Discard, res.Body)
	return fmt.Sprintf("status %d: %s", res.StatusCode, bytes.TrimSpace(b))
}

func urlPath(segments ...string) string {
	var b strings.Builder
	for idx, s := range segments {
		if idx > 0 {
			b.WriteByte('/')
		}
		b.WriteString(url.PathEscape(s))
	}
	return b.String()
}

func containsStatus(statuses []int, status int) bool {
	for _, s := range statuses {
		if s == status {
			return true
		}
	}
	return false
}
