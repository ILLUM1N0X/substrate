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

package objectstoreplugin

import (
	"context"

	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/pkg/proto/snapshotpluginpb"
	"google.golang.org/grpc"
)

// NodeClient returns a client that calls p directly, without a socket. Tests
// use it to drive callers through the real plugin.
func NodeClient(p *NodePlugin) snapshotpluginpb.NodeProviderPluginClient {
	return nodeClient{p}
}

type nodeClient struct{ p *NodePlugin }

func (c nodeClient) FetchSnapshot(ctx context.Context, in *snapshotpluginpb.FetchSnapshotRequest, _ ...grpc.CallOption) (*snapshotpluginpb.FetchSnapshotResponse, error) {
	return c.p.FetchSnapshot(ctx, in)
}

func (c nodeClient) UploadSnapshot(ctx context.Context, in *snapshotpluginpb.UploadSnapshotRequest, _ ...grpc.CallOption) (*snapshotpluginpb.UploadSnapshotResponse, error) {
	return c.p.UploadSnapshot(ctx, in)
}

// ServerClient returns a client that calls a ServerPlugin on store directly,
// without a socket.
func ServerClient(store objectstore.Store) snapshotpluginpb.ServerProviderPluginClient {
	return serverClient{NewServerPlugin(store)}
}

type serverClient struct{ p *ServerPlugin }

func (c serverClient) CleanupSnapshot(ctx context.Context, in *snapshotpluginpb.CleanupSnapshotRequest, _ ...grpc.CallOption) (*snapshotpluginpb.CleanupSnapshotResponse, error) {
	return c.p.CleanupSnapshot(ctx, in)
}

func (c serverClient) CopySnapshot(ctx context.Context, in *snapshotpluginpb.CopySnapshotRequest, _ ...grpc.CallOption) (*snapshotpluginpb.CopySnapshotResponse, error) {
	return c.p.CopySnapshot(ctx, in)
}
