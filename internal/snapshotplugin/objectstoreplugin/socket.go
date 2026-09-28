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
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Listen removes any stale socket at path and listens on a fresh one that
// only the socket's owner can connect to.
func Listen(path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("while creating socket directory: %w", err)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("while removing stale socket %s: %w", path, err)
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("while listening on %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		lis.Close()
		return nil, fmt.Errorf("while restricting socket %s: %w", path, err)
	}
	return lis, nil
}

// Dial returns a long-lived client connection to a plugin socket. Calls wait
// for the plugin to come up rather than failing while it starts.
//
// The connection is unauthenticated: the socket is reachable only from
// inside the pod, and only by its owner.
func Dial(path string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient("unix://"+path,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, fmt.Errorf("while dialing snapshot plugin at %s: %w", path, err)
	}
	return conn, nil
}
