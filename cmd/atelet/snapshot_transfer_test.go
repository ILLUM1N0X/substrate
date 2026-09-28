// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/ategcs"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/snapshotplugin/objectstoreplugin"
)

// newPluginHerder returns an AteomHerder whose snapshots go through the
// object-store plugin backed by store.
func newPluginHerder(t *testing.T, store ategcs.ObjectStorage) *AteomHerder {
	t.Helper()
	// Test directories come from t.TempDir, so admit any absolute path.
	plugin, err := objectstoreplugin.NewNodePlugin(store, "/")
	if err != nil {
		t.Fatal(err)
	}
	return &AteomHerder{
		snapshots:          objectstoreplugin.NodeClient(plugin),
		snapshotScratchDir: t.TempDir(),
	}
}

// orderedObjectStorage records the order objects are stored in.
type orderedObjectStorage struct {
	mu   sync.Mutex
	puts []string
	data map[string][]byte
}

func (o *orderedObjectStorage) PutObject(_ context.Context, bucket, object string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.puts = append(o.puts, object)
	if o.data == nil {
		o.data = map[string][]byte{}
	}
	o.data[bucket+"/"+object] = b
	return nil
}

func (o *orderedObjectStorage) GetObject(_ context.Context, bucket, object string) (io.ReadCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.data[bucket+"/"+object]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ategcs.ErrObjectNotFound, bucket, object)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func TestUploadSnapshotManifestLast(t *testing.T) {
	store := &orderedObjectStorage{}
	s := newPluginHerder(t, store)
	uri, err := resources.ParseSnapshotURI(pausedSnapshotURI)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	rec := sandboxAssetsRecord{SandboxClass: "gvisor", PauseImage: testPauseImage, SnapshotFiles: []string{"a", "b", "c"}}
	for _, f := range rec.SnapshotFiles {
		if err := os.WriteFile(dir+"/"+f, []byte(f), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.uploadSnapshot(context.Background(), uri, dir, &rec, "ate-demo", "counter"); err != nil {
		t.Fatalf("uploadSnapshot: %v", err)
	}
	if len(store.puts) != 4 {
		t.Fatalf("puts = %v, want 3 files then the manifest", store.puts)
	}
	for i, p := range store.puts {
		isManifest := strings.HasSuffix(p, "/"+sandboxManifestName)
		if isManifest != (i == len(store.puts)-1) {
			t.Fatalf("puts = %v, want the manifest last and only last", store.puts)
		}
	}

	// The manifest round-trips, and scratch space is cleaned up.
	got, err := s.fetchManifest(context.Background(), uri.String())
	if err != nil {
		t.Fatalf("fetchManifest: %v", err)
	}
	back, err := unmarshalSandboxRecord(got)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(back.SnapshotFiles, rec.SnapshotFiles) {
		t.Errorf("manifest files = %v, want %v", back.SnapshotFiles, rec.SnapshotFiles)
	}
	if entries, _ := os.ReadDir(s.snapshotScratchDir); len(entries) != 0 {
		t.Errorf("scratch dir not cleaned up: %v", entries)
	}
}

func TestSnapshotTransferSkipsEmptyFileLists(t *testing.T) {
	store := &orderedObjectStorage{}
	s := newPluginHerder(t, store)
	if err := s.downloadExternalCheckpoint(context.Background(), pausedSnapshotURI, t.TempDir(), nil); err != nil {
		t.Errorf("downloadExternalCheckpoint(no files) = %v, want nil", err)
	}
	if err := s.uploadSnapshotFiles(context.Background(), pausedSnapshotURI, t.TempDir(), nil); err != nil {
		t.Errorf("uploadSnapshotFiles(no files) = %v, want nil", err)
	}
	if len(store.puts) != 0 {
		t.Errorf("objects stored = %v, want none", store.puts)
	}
}
