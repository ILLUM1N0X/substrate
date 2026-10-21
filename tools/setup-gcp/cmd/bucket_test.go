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
	"slices"
	"strings"
	"testing"

	"cloud.google.com/go/iam"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/genproto/googleapis/type/expr"
)

func TestValidateBucketFlags(t *testing.T) {
	origGetProject := getProjectFn
	defer func() { getProjectFn = origGetProject }()

	getProjectFn = func(ctx context.Context, name string) (*resourcemanagerpb.Project, error) {
		return &resourcemanagerpb.Project{
			Name:      "projects/123456",
			ProjectId: "test-project",
		}, nil
	}

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "missing bucket name",
			cfg:  Config{ProjectID: "test-project"},
			want: "--name is required",
		},
		{
			name: "missing project id",
			cfg:  Config{BucketName: "test-bucket"},
			want: "--project-id is required",
		},
		{
			name: "missing project id when only project number provided",
			cfg:  Config{BucketName: "test-bucket", ProjectNumber: "123456"},
			want: "--project-id is required",
		},
		{
			name: "all required flags present with both id and number",
			cfg:  Config{ProjectID: "test-project", ProjectNumber: "123456", BucketName: "test-bucket"},
		},
		{
			name: "all required flags present with only project id",
			cfg:  Config{ProjectID: "test-project", BucketName: "test-bucket"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgCopy := tt.cfg
			err := validateBucketFlags(t.Context(), &cfgCopy)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("validateBucketFlags() = %v; want nil", err)
				}
				if cfgCopy.ProjectNumber != "123456" {
					t.Errorf("cfgCopy.ProjectNumber = %q; want %q", cfgCopy.ProjectNumber, "123456")
				}
				return
			}
			if err == nil {
				t.Fatalf("validateBucketFlags() = nil; want %q", tt.want)
			}
			if err.Error() != tt.want {
				t.Errorf("validateBucketFlags() = %q; want %q", err.Error(), tt.want)
			}
		})
	}
}

func TestAddBucketGrants(t *testing.T) {
	cfg := &Config{ProjectID: "proj", ProjectNumber: "123", BucketName: "snaps"}
	member := func(sa string) string {
		return "principal://iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/proj.svc.id.goog/subject/ns/ate-system/sa/" + sa
	}
	assets := &expr.Expr{Expression: `resource.name.startsWith("projects/_/buckets/snaps/objects/kata-assets/")`}

	tests := []struct {
		name     string
		existing []*iampb.Binding
		want     []string // role and member of each binding added, "role member [conditional]"
	}{
		{
			name: "empty policy gets every grant, atelet only on the asset prefix",
			want: []string{
				"roles/storage.objectAdmin " + member("ate-api-server"),
				"roles/storage.bucketViewer " + member("ate-api-server"),
				"roles/storage.objectViewer " + member("atelet") + " conditional",
			},
		},
		{
			name: "grants already in place add nothing",
			existing: []*iampb.Binding{
				{Role: "roles/storage.objectAdmin", Members: []string{member("ate-api-server")}},
				{Role: "roles/storage.bucketViewer", Members: []string{member("ate-api-server")}},
				{Role: "roles/storage.objectViewer", Members: []string{member("atelet")}, Condition: assets},
			},
		},
		{
			name: "an unconditional atelet viewer does not stand in for the prefix grant",
			existing: []*iampb.Binding{
				{Role: "roles/storage.objectAdmin", Members: []string{member("ate-api-server")}},
				{Role: "roles/storage.bucketViewer", Members: []string{member("ate-api-server")}},
				{Role: "roles/storage.objectViewer", Members: []string{member("atelet")}},
			},
			want: []string{"roles/storage.objectViewer " + member("atelet") + " conditional"},
		},
		{
			name: "a grant under another condition does not count",
			existing: []*iampb.Binding{
				{Role: "roles/storage.objectAdmin", Members: []string{member("ate-api-server")}, Condition: assets},
				{Role: "roles/storage.bucketViewer", Members: []string{member("ate-api-server")}},
				{Role: "roles/storage.objectViewer", Members: []string{member("atelet")}, Condition: assets},
			},
			want: []string{"roles/storage.objectAdmin " + member("ate-api-server")},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			policy := &iam.Policy3{Bindings: slices.Clone(tc.existing)}
			changed := addBucketGrants(policy, cfg, snapshotBucketGrants)
			var got []string
			for _, b := range policy.Bindings[len(tc.existing):] {
				s := b.Role + " " + strings.Join(b.Members, ",")
				if b.Condition != nil {
					if b.Condition.Expression != assets.Expression {
						t.Errorf("condition = %q, want %q", b.Condition.Expression, assets.Expression)
					}
					s += " conditional"
				}
				got = append(got, s)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("added bindings:\n got  %q\n want %q", got, tc.want)
			}
			if changed != (len(tc.want) > 0) {
				t.Errorf("changed = %v, want %v", changed, len(tc.want) > 0)
			}
		})
	}
}
