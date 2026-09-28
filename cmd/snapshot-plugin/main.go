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
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/snapshotplugin/objectstoreplugin"
	"github.com/agent-substrate/substrate/pkg/objectstorage"
	snapshotpluginv1 "github.com/agent-substrate/substrate/pkg/proto/snapshotplugin/v1"
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
		plugin, err := objectstoreplugin.NewNodePlugin(newObjectStorage(ctx), *root)
		if err != nil {
			serverboot.Fatal(ctx, "Invalid --root", err)
		}
		snapshotpluginv1.RegisterNodeProviderPluginServer(srv, plugin)
	case "server":
		store, err := newObjectStore(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to set up the object storage backend", err)
		}
		snapshotpluginv1.RegisterServerProviderPluginServer(srv, objectstoreplugin.NewServerPlugin(store))
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
// with. It is atelet's backend selection, moved here unchanged.
func newObjectStorage(ctx context.Context) objectstorage.ObjectStorage {
	var wrappedGCS objectstorage.ObjectStorage
	var err error
	storageBackend := os.Getenv("ATE_STORAGE_BACKEND")
	switch storageBackend {
	case "s3":
		slog.InfoContext(ctx, "Using S3 storage backend")
		// depend on standard AWS environment variables to configure the client
		// these will need to be set on the atelet pods
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to load S3 config", err)
		}
		wrappedGCS = objectstorage.NewS3Client(s3.NewFromConfig(cfg, func(o *s3.Options) {
			if usePathStyle := os.Getenv("AWS_S3_USE_PATH_STYLE"); usePathStyle == "true" {
				o.UsePathStyle = true
			}
		}))
	// GCS is currently the default, TODO: we assume workload identity / ADC
	default:
		wrappedGCS, err = objectstorage.NewGCSClient(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to create GCS client", err)
		}
	}
	return wrappedGCS
}

// newObjectStore builds the client ate-api manages external snapshots with.
// The backend is selected the same way atelet selects the one it reads and
// writes snapshots through, so both ends of a snapshot's life agree on where
// it lives.
func newObjectStore(ctx context.Context) (objectstore.Store, error) {
	switch backend := os.Getenv("ATE_STORAGE_BACKEND"); backend {
	case "s3":
		slog.InfoContext(ctx, "Using S3 storage backend")
		// Depends on the standard AWS environment variables, which have to be
		// set on the ate-api pod.
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("loading S3 config: %w", err)
		}
		return objectstore.NewS3(s3.NewFromConfig(cfg, func(o *s3.Options) {
			if os.Getenv("AWS_S3_USE_PATH_STYLE") == "true" {
				o.UsePathStyle = true
			}
		})), nil
	// GCS is currently the default, TODO: we assume workload identity / ADC
	default:
		client, err := storage.NewClient(ctx)
		if err != nil {
			return nil, fmt.Errorf("creating GCS client: %w", err)
		}
		return objectstore.NewGCS(client), nil
	}
}
