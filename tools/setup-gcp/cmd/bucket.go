// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"cloud.google.com/go/iam"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/storage"
	"github.com/spf13/cobra"
	"google.golang.org/genproto/googleapis/type/expr"
)

func createSnapshotBucket(ctx context.Context, cfg *Config) error {
	if err := resolveProject(ctx, cfg); err != nil {
		return err
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(cfg.BucketName)
	slog.Info("Checking if Bucket exists", slog.String("bucket", cfg.BucketName))
	attrs, err := bucket.Attrs(ctx)
	if err != nil {
		if !errors.Is(err, storage.ErrBucketNotExist) {
			return fmt.Errorf("getting bucket: %w", err)
		}

		slog.Info("Bucket does not exist. Creating...", slog.String("bucket", cfg.BucketName))
		err = bucket.Create(ctx, cfg.ProjectID, &storage.BucketAttrs{
			Location: cfg.Region,
			UniformBucketLevelAccess: storage.UniformBucketLevelAccess{
				Enabled: true,
			},
		})
		if err != nil {
			return fmt.Errorf("create snapshot bucket: %w", err)
		}
		return nil
	}

	slog.Info("Bucket exists. Checking attributes...", slog.String("bucket", cfg.BucketName))

	// Ensure the bucket belongs to the correct project.
	// GCS bucket names are globally unique, so it's possible this bucket belongs to a different project.
	projectNum := strconv.FormatUint(attrs.ProjectNumber, 10)
	if projectNum != cfg.ProjectNumber {
		return fmt.Errorf("bucket %s belongs to project number %s, but expected %s (it may be owned by another GCP project)", cfg.BucketName, projectNum, cfg.ProjectNumber)
	}

	// Ensure the bucket is in the correct region.
	if !strings.EqualFold(attrs.Location, cfg.Region) {
		return fmt.Errorf("bucket %s is in location %s, but expected %s", cfg.BucketName, attrs.Location, cfg.Region)
	}

	slog.Info("Bucket is in the correct project and region. Checking uniform-bucket-level-access setting...", slog.String("bucket", cfg.BucketName))
	if !attrs.UniformBucketLevelAccess.Enabled {
		slog.Info("Updating uniform-bucket-level-access", slog.String("bucket", cfg.BucketName))
		_, err = bucket.Update(ctx, storage.BucketAttrsToUpdate{
			UniformBucketLevelAccess: &storage.UniformBucketLevelAccess{
				Enabled: true,
			},
		})
		if err != nil {
			return fmt.Errorf("update bucket ubla: %w", err)
		}
	} else {
		slog.Info("uniform-bucket-level-access is already correct", slog.String("bucket", cfg.BucketName))
	}

	return nil
}

// assetPrefix is where hack/install-microvm-deps.sh stages micro-VM sandbox
// assets in the snapshot bucket. It sits outside every snapshot location.
const assetPrefix = "kata-assets/"

// bucketGrant is a role on the snapshot bucket for an ate-system Kubernetes
// service account, limited to objects under prefix when prefix is set.
type bucketGrant struct {
	subject string
	role    string
	prefix  string
}

// snapshotBucketGrants are the roles setup-gcp grants on the snapshot bucket.
// ate-api-server copies snapshots for tags and deletes the ones nothing refers
// to. atelet's object-store plugin reads only micro-VM sandbox assets with its
// own identity: on GCS every snapshot transfer runs on a token scoped to its
// actor, issued by the credential exchange.
var snapshotBucketGrants = []bucketGrant{
	{subject: "ate-api-server", role: "roles/storage.objectAdmin"},
	{subject: "ate-api-server", role: "roles/storage.bucketViewer"},
	{subject: "atelet", role: "roles/storage.objectViewer", prefix: assetPrefix},
}

// condition limits g to objects under its prefix in bucket, or is nil.
func (g bucketGrant) condition(bucket string) *expr.Expr {
	if g.prefix == "" {
		return nil
	}
	return &expr.Expr{
		Title:      "objects under " + g.prefix,
		Expression: fmt.Sprintf(`resource.name.startsWith("projects/_/buckets/%s/objects/%s")`, bucket, g.prefix),
	}
}

func createIamPolicyBindings(ctx context.Context, cfg *Config) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	// Version 3, since a policy holding a conditional binding cannot be read
	// at version 1.
	handle := client.Bucket(cfg.BucketName).IAM().V3()
	policy, err := handle.Policy(ctx)
	if err != nil {
		return fmt.Errorf("get bucket iam policy: %w", err)
	}
	changed := addBucketGrants(policy, cfg, snapshotBucketGrants)
	if !changed {
		slog.Info("IAM policy is already correct", slog.String("bucket", cfg.BucketName))
		return nil
	}

	slog.Info("Setting IAM policy for bucket", slog.String("bucket", cfg.BucketName))
	if err := handle.SetPolicy(ctx, policy); err != nil {
		return fmt.Errorf("set bucket iam policy: %w", err)
	}
	return nil
}

// addBucketGrants adds each grant the policy lacks and reports whether it
// added any.
func addBucketGrants(policy *iam.Policy3, cfg *Config, grants []bucketGrant) bool {
	changed := false
	for _, g := range grants {
		member := fmt.Sprintf("principal://iam.googleapis.com/projects/%s/locations/global/workloadIdentityPools/%s.svc.id.goog/subject/ns/ate-system/sa/%s", cfg.ProjectNumber, cfg.ProjectID, g.subject)
		cond := g.condition(cfg.BucketName)
		if hasBinding(policy.Bindings, g.role, member, cond) {
			continue
		}
		slog.Info("Adding role to member", slog.String("bucket", cfg.BucketName), slog.String("role", g.role), slog.String("member", member), slog.String("condition", cond.GetExpression()))
		policy.Bindings = append(policy.Bindings, &iampb.Binding{Role: g.role, Members: []string{member}, Condition: cond})
		changed = true
	}
	return changed
}

// hasBinding reports whether bindings already grant role to member under the
// same condition, or without one when cond is nil. A binding under another
// condition does not count: it may not cover the objects Substrate touches.
func hasBinding(bindings []*iampb.Binding, role, member string, cond *expr.Expr) bool {
	for _, b := range bindings {
		if b.Role != role || !slices.Contains(b.Members, member) {
			continue
		}
		if b.Condition.GetExpression() == cond.GetExpression() {
			return true
		}
	}
	return false
}

func validateBucketFlags(ctx context.Context, cfg *Config) error {
	if cfg.BucketName == "" {
		return errors.New("--name is required")
	}
	return resolveProject(ctx, cfg)
}

var bucketCmd = &cobra.Command{
	Use:   "bucket",
	Short: "Create GCS bucket",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := validateBucketFlags(cmd.Context(), &cfg); err != nil {
			return err
		}
		return createSnapshotBucket(cmd.Context(), &cfg)
	},
}

func init() {
	createCmd.AddCommand(bucketCmd)
	bucketCmd.Flags().StringVar(&cfg.BucketName, "name", getEnv("BUCKET_NAME", ""), "Name of the GCS bucket [env: BUCKET_NAME]")
}
