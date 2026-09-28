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

// Package objectstoreplugin implements the snapshot plugin API on GCS or S3
// object storage, using the plugin's own credentials.
//
// Snapshot files are stored as zstd-compressed objects named <file>.zstd
// under the snapshot URI, except the manifest, which is stored as is. This is
// the layout atelet has always written, so snapshots taken before the plugin
// existed stay readable.
package objectstoreplugin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/agent-substrate/substrate/internal/ategcs"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/snapshotpluginpb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ManifestFile is the snapshot file stored uncompressed. It is small, and
// its presence is what marks a snapshot as complete.
const ManifestFile = "manifest.json"

// NodePlugin serves NodeProviderPlugin on an object storage client.
type NodePlugin struct {
	snapshotpluginpb.UnimplementedNodeProviderPluginServer

	client ategcs.ObjectStorage
	// root confines every local path a caller names.
	root string
}

// NewNodePlugin returns a NodePlugin that reads and writes local files only
// below root.
func NewNodePlugin(client ategcs.ObjectStorage, root string) (*NodePlugin, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("root %q is not an absolute path", root)
	}
	return &NodePlugin{client: client, root: filepath.Clean(root)}, nil
}

// FetchSnapshot downloads the requested snapshot files into write_path.
func (p *NodePlugin) FetchSnapshot(ctx context.Context, req *snapshotpluginpb.FetchSnapshotRequest) (*snapshotpluginpb.FetchSnapshotResponse, error) {
	uri, dir, err := p.validate(req.GetSnapshotUri(), req.GetWritePath(), req.GetFiles())
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, toStatus(fmt.Errorf("while opening restore directory: %w", err))
	}
	defer root.Close()

	g, gCtx := errgroup.WithContext(ctx)
	for _, name := range req.GetFiles() {
		g.Go(func() error {
			return p.fetchFile(gCtx, uri, root, name)
		})
	}
	if err := g.Wait(); err != nil {
		return nil, toStatus(err)
	}
	return &snapshotpluginpb.FetchSnapshotResponse{}, nil
}

// UploadSnapshot uploads the requested files from local_path into the
// snapshot.
func (p *NodePlugin) UploadSnapshot(ctx context.Context, req *snapshotpluginpb.UploadSnapshotRequest) (*snapshotpluginpb.UploadSnapshotResponse, error) {
	uri, dir, err := p.validate(req.GetSnapshotUri(), req.GetLocalPath(), req.GetFiles())
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, toStatus(fmt.Errorf("while opening snapshot directory: %w", err))
	}
	defer root.Close()

	g, gCtx := errgroup.WithContext(ctx)
	for _, name := range req.GetFiles() {
		g.Go(func() error {
			return p.uploadFile(gCtx, uri, root, name)
		})
	}
	if err := g.Wait(); err != nil {
		return nil, toStatus(err)
	}
	return &snapshotpluginpb.UploadSnapshotResponse{}, nil
}

// fetchFile downloads one snapshot file into root. root confines the write,
// so a symlink planted in the directory cannot redirect it elsewhere.
func (p *NodePlugin) fetchFile(ctx context.Context, uri resources.SnapshotURI, root *os.Root, name string) error {
	objectURI, err := uri.ObjectURI(objectName(name))
	if err != nil {
		return fmt.Errorf("while addressing %s: %w", name, err)
	}
	if name == ManifestFile {
		content, err := ategcs.FetchFromGCS(ctx, p.client, objectURI)
		if err != nil {
			return fmt.Errorf("while downloading %s: %w", name, err)
		}
		if err := root.WriteFile(name, content, 0o600); err != nil {
			return fmt.Errorf("while writing %s in restore directory: %w", name, err)
		}
		return nil
	}
	local, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("while opening %s in restore directory: %w", name, err)
	}
	fetchErr := ategcs.FetchFileFromGCSWithZstd(ctx, p.client, objectURI, local)
	closeErr := local.Close()
	if err := errors.Join(fetchErr, closeErr); err != nil {
		return fmt.Errorf("while downloading %s: %w", name, err)
	}
	return nil
}

// uploadFile uploads one regular file from root. root confines the read, so a
// symlink planted in the directory cannot expose a file outside it.
func (p *NodePlugin) uploadFile(ctx context.Context, uri resources.SnapshotURI, root *os.Root, name string) error {
	objectURI, err := uri.ObjectURI(objectName(name))
	if err != nil {
		return fmt.Errorf("while addressing %s: %w", name, err)
	}
	local, err := root.Open(name)
	if err != nil {
		return fmt.Errorf("while opening %s in snapshot directory: %w", name, err)
	}
	defer local.Close()
	info, err := local.Stat()
	if err != nil {
		return fmt.Errorf("while inspecting %s in snapshot directory: %w", name, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("snapshot file %s is not a regular file", name)
	}
	if name == ManifestFile {
		content, err := io.ReadAll(local)
		if err != nil {
			return fmt.Errorf("while reading %s in snapshot directory: %w", name, err)
		}
		if err := ategcs.SendBytesToGCS(ctx, p.client, objectURI, content); err != nil {
			return fmt.Errorf("while uploading %s: %w", name, err)
		}
		return nil
	}
	if err := ategcs.SendFileToGCSWithZstd(ctx, p.client, objectURI, local); err != nil {
		return fmt.Errorf("while uploading %s: %w", name, err)
	}
	return nil
}

// objectName is the name a snapshot file is stored under.
func objectName(file string) string {
	if file == ManifestFile {
		return file
	}
	return file + ".zstd"
}

// validate checks a request's snapshot URI, local directory and file names,
// returning the parsed URI and the cleaned directory.
func (p *NodePlugin) validate(snapshotURI, dir string, files []string) (resources.SnapshotURI, string, error) {
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return resources.SnapshotURI{}, "", status.Error(codes.InvalidArgument, err.Error())
	}
	if !filepath.IsAbs(dir) {
		return resources.SnapshotURI{}, "", status.Errorf(codes.InvalidArgument, "local path %q is not absolute", dir)
	}
	dir = filepath.Clean(dir)
	if rel, err := filepath.Rel(p.root, dir); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return resources.SnapshotURI{}, "", status.Errorf(codes.InvalidArgument, "local path %q is outside %q", dir, p.root)
	}
	if len(files) == 0 {
		return resources.SnapshotURI{}, "", status.Error(codes.InvalidArgument, "no files requested")
	}
	for _, f := range files {
		if !isBaseName(f) {
			return resources.SnapshotURI{}, "", status.Errorf(codes.InvalidArgument, "file %q is not a base name", f)
		}
	}
	return uri, dir, nil
}

func isBaseName(f string) bool {
	return f != "" && f != "." && f != ".." && !strings.ContainsAny(f, `/\`)
}

// toStatus maps a transfer error to a gRPC status, keeping NotFound
// distinguishable so callers can probe for a snapshot.
func toStatus(err error) error {
	switch {
	case errors.Is(err, ategcs.ErrObjectNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
