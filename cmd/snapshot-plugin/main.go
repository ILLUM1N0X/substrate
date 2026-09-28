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

// Command snapshot-plugin serves the snapshot plugin API on GCS or S3 over a
// Unix socket. It runs as a sidecar: "node" next to atelet, "server" next to
// ate-api-server. The backend is chosen by ATE_STORAGE_BACKEND ("s3", or GCS
// by default) with the ambient credentials, as atelet and ate-api-server do.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/storage"
	"github.com/agent-substrate/substrate/internal/ategcs"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/snapshotplugin/objectstoreplugin"
	"github.com/agent-substrate/substrate/pkg/proto/snapshotpluginpb"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const usage = `usage: snapshot-plugin <node|server> [flags]`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	mode := os.Args[1]
	flags := pflag.NewFlagSet(mode, pflag.ExitOnError)
	socket := flags.String("socket", "", "Unix socket to serve on")
	root := flags.String("root", nodepath.BasePath, "node mode: the only directory tree local snapshot files may be read from or written to")
	_ = flags.Parse(os.Args[2:])

	ctx := context.Background()
	serverboot.InitLogger()
	if *socket == "" {
		serverboot.Fatal(ctx, "Missing --socket", fmt.Errorf("--socket is required"))
	}

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: "snapshot-plugin-" + mode,
		Sampling:    serverboot.ResolveTraceSampling(ctx, serverboot.ParentRatioSampling(serverboot.ControlPlaneTraceRatio)),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	srv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	switch mode {
	case "node":
		client, err := newObjectStorage(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to set up the object storage backend", err)
		}
		plugin, err := objectstoreplugin.NewNodePlugin(client, *root)
		if err != nil {
			serverboot.Fatal(ctx, "Invalid --root", err)
		}
		snapshotpluginpb.RegisterNodeProviderPluginServer(srv, plugin)
	case "server":
		store, err := newObjectStore(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to set up the object storage backend", err)
		}
		snapshotpluginpb.RegisterServerProviderPluginServer(srv, objectstoreplugin.NewServerPlugin(store))
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	healthpb.RegisterHealthServer(srv, health.NewServer())

	lis, err := objectstoreplugin.Listen(*socket)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to listen", err)
	}

	// Finish in-flight transfers on SIGTERM. The pod's grace period bounds
	// how long that may take.
	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() {
		<-sigCtx.Done()
		slog.InfoContext(ctx, "Draining snapshot plugin")
		srv.GracefulStop()
	}()

	slog.InfoContext(ctx, "Serving snapshot plugin", slog.String("mode", mode), slog.String("socket", *socket))
	if err := srv.Serve(lis); err != nil {
		serverboot.Fatal(ctx, "Serve failed", err)
	}
}

// newObjectStorage builds the client the node plugin transfers snapshot files
// with, selected the same way atelet selects its own.
func newObjectStorage(ctx context.Context) (ategcs.ObjectStorage, error) {
	if os.Getenv("ATE_STORAGE_BACKEND") == "s3" {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("loading S3 config: %w", err)
		}
		return ategcs.NewS3Client(s3.NewFromConfig(cfg, s3PathStyle)), nil
	}
	return ategcs.NewGCSClient(ctx)
}

// newObjectStore builds the client the server plugin manages whole snapshots
// with, selected the same way ate-api-server selects its own.
func newObjectStore(ctx context.Context) (objectstore.Store, error) {
	if os.Getenv("ATE_STORAGE_BACKEND") == "s3" {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("loading S3 config: %w", err)
		}
		return objectstore.NewS3(s3.NewFromConfig(cfg, s3PathStyle)), nil
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}
	return objectstore.NewGCS(client), nil
}

func s3PathStyle(o *s3.Options) {
	if os.Getenv("AWS_S3_USE_PATH_STYLE") == "true" {
		o.UsePathStyle = true
	}
}
