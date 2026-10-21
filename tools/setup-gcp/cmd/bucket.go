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
	"net/http"
	"slices"
	"strconv"
	"strings"

	"cloud.google.com/go/iam"
	"cloud.google.com/go/storage"
	"github.com/spf13/cobra"
	"google.golang.org/api/googleapi"
)

// createBuckets creates the snapshot bucket and the micro-VM asset bucket.
func createBuckets(ctx context.Context, cfg *Config) error {
	if err := createBucket(ctx, cfg, cfg.BucketName); err != nil {
		return err
	}
	return createBucket(ctx, cfg, cfg.assetBucketName())
}

func createBucket(ctx context.Context, cfg *Config, name string) error {
	if err := resolveProject(ctx, cfg); err != nil {
		return err
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(name)
	slog.Info("Checking if Bucket exists", slog.String("bucket", name))
	attrs, err := bucket.Attrs(ctx)
	if err != nil {
		if !errors.Is(err, storage.ErrBucketNotExist) {
			return fmt.Errorf("getting bucket %s: %w", name, err)
		}

		slog.Info("Bucket does not exist. Creating...", slog.String("bucket", name))
		err = bucket.Create(ctx, cfg.ProjectID, &storage.BucketAttrs{
			Location: cfg.Region,
			UniformBucketLevelAccess: storage.UniformBucketLevelAccess{
				Enabled: true,
			},
		})
		if err != nil {
			return fmt.Errorf("create bucket %s: %w", name, err)
		}
		return nil
	}

	slog.Info("Bucket exists. Checking attributes...", slog.String("bucket", name))

	// Ensure the bucket belongs to the correct project.
	// GCS bucket names are globally unique, so it's possible this bucket belongs to a different project.
	projectNum := strconv.FormatUint(attrs.ProjectNumber, 10)
	if projectNum != cfg.ProjectNumber {
		return fmt.Errorf("bucket %s belongs to project number %s, but expected %s (it may be owned by another GCP project)", name, projectNum, cfg.ProjectNumber)
	}

	// Ensure the bucket is in the correct region.
	if !strings.EqualFold(attrs.Location, cfg.Region) {
		return fmt.Errorf("bucket %s is in location %s, but expected %s", name, attrs.Location, cfg.Region)
	}

	slog.Info("Bucket is in the correct project and region. Checking uniform-bucket-level-access setting...", slog.String("bucket", name))
	if !attrs.UniformBucketLevelAccess.Enabled {
		slog.Info("Updating uniform-bucket-level-access", slog.String("bucket", name))
		_, err = bucket.Update(ctx, storage.BucketAttrsToUpdate{
			UniformBucketLevelAccess: &storage.UniformBucketLevelAccess{
				Enabled: true,
			},
		})
		if err != nil {
			return fmt.Errorf("update bucket %s ubla: %w", name, err)
		}
	} else {
		slog.Info("uniform-bucket-level-access is already correct", slog.String("bucket", name))
	}

	return nil
}

// bucketGrant is a role on a bucket for an ate-system Kubernetes service
// account.
type bucketGrant struct {
	bucket  string
	subject string
	role    string
}

// bucketGrants are the roles setup-gcp grants on the two buckets.
// ate-api-server copies snapshots for tags and deletes the ones nothing refers
// to, so it gets objectAdmin, plus bucketViewer to resolve the bucket. atelet
// reads only micro-VM sandbox assets with its own identity: on GCS every
// snapshot transfer runs on a token scoped to its actor, issued by the
// credential exchange.
func bucketGrants(cfg *Config) []bucketGrant {
	return []bucketGrant{
		{bucket: cfg.BucketName, subject: "ate-api-server", role: "roles/storage.objectAdmin"},
		{bucket: cfg.BucketName, subject: "ate-api-server", role: "roles/storage.bucketViewer"},
		{bucket: cfg.assetBucketName(), subject: "atelet", role: "roles/storage.objectViewer"},
	}
}

func createIamPolicyBindings(ctx context.Context, cfg *Config) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	// Every policy is read before any is written, so a missing bucket fails
	// the run before it grants anything.
	names := []string{cfg.BucketName, cfg.assetBucketName()}
	policies := make([]*iam.Policy, len(names))
	for i, name := range names {
		policy, err := client.Bucket(name).IAM().Policy(ctx)
		if gerr := (*googleapi.Error)(nil); errors.As(err, &gerr) && gerr.Code == http.StatusNotFound {
			return fmt.Errorf("bucket %s does not exist, run `create bucket` first: %w", name, err)
		}
		if err != nil {
			return fmt.Errorf("get bucket %s iam policy: %w", name, err)
		}
		policies[i] = policy
	}

	grants := bucketGrants(cfg)
	for i, name := range names {
		if !addGrants(policies[i], cfg, name, grants) {
			slog.Info("IAM policy is already correct", slog.String("bucket", name))
			continue
		}
		slog.Info("Setting IAM policy for bucket", slog.String("bucket", name))
		if err := client.Bucket(name).IAM().SetPolicy(ctx, policies[i]); err != nil {
			return fmt.Errorf("set bucket %s iam policy: %w", name, err)
		}
	}
	return nil
}

// addGrants adds the grants on the named bucket that policy lacks, and
// reports whether it added any.
func addGrants(policy *iam.Policy, cfg *Config, name string, grants []bucketGrant) bool {
	changed := false
	for _, g := range grants {
		if g.bucket != name {
			continue
		}
		member := fmt.Sprintf("principal://iam.googleapis.com/projects/%s/locations/global/workloadIdentityPools/%s.svc.id.goog/subject/ns/ate-system/sa/%s", cfg.ProjectNumber, cfg.ProjectID, g.subject)
		if hasUnconditionalBinding(policy, g.role, member) {
			continue
		}
		slog.Info("Adding role to member", slog.String("bucket", name), slog.String("role", g.role), slog.String("member", member))
		policy.Add(member, iam.RoleName(g.role))
		changed = true
	}
	return changed
}

// hasUnconditionalBinding reports whether the policy already grants role to
// member without a condition. Conditional bindings are ignored: they may not
// apply to the objects Substrate touches.
func hasUnconditionalBinding(policy *iam.Policy, role, member string) bool {
	for _, b := range policy.InternalProto.Bindings {
		if b.Condition == nil && b.Role == role && slices.Contains(b.Members, member) {
			return true
		}
	}
	return false
}

func validateBucketFlags(ctx context.Context, cfg *Config) error {
	if cfg.BucketName == "" {
		return errors.New("--name is required")
	}
	if err := checkAssetBucketName(cfg); err != nil {
		return err
	}
	return resolveProject(ctx, cfg)
}

// maxBucketNameLength is the longest bucket name GCS accepts.
const maxBucketNameLength = 63

// checkAssetBucketName fails when the asset bucket's name, the snapshot
// bucket's plus a suffix, is longer than GCS allows.
func checkAssetBucketName(cfg *Config) error {
	if n := len(cfg.assetBucketName()); n > maxBucketNameLength {
		return fmt.Errorf("bucket name %q is too long: the asset bucket %q would have %d characters, and GCS allows %d", cfg.BucketName, cfg.assetBucketName(), n, maxBucketNameLength)
	}
	return nil
}

var bucketCmd = &cobra.Command{
	Use:   "bucket",
	Short: "Create the GCS snapshot bucket and the micro-VM asset bucket",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := validateBucketFlags(cmd.Context(), &cfg); err != nil {
			return err
		}
		return createBuckets(cmd.Context(), &cfg)
	},
}

func init() {
	createCmd.AddCommand(bucketCmd)
	bucketCmd.Flags().StringVar(&cfg.BucketName, "name", getEnv("BUCKET_NAME", ""), "Name of the GCS snapshot bucket. The asset bucket is named NAME-assets [env: BUCKET_NAME]")
}
