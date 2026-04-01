// Copyright 2024 Syntio Ltd.
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

package gcs

import (
	"context"

	"cloud.google.com/go/iam"
	"cloud.google.com/go/storage"
	"github.com/pkg/errors"

	"github.com/dataphos/lib-brokers/internal/errtemplates"
)

// testPermissions tests if the service account linked to the given resource has permissions matching the ones given.
func testPermissions(ctx context.Context, handle *iam.Handle, permissions []string) error {
	subset, err := handle.TestPermissions(ctx, permissions)
	if err != nil {
		return errors.Wrap(err, "testing permissions failed")
	}

	if len(permissions) != len(subset) {
		return errtemplates.PermissionLevelNotSufficient(permissions, subset)
	}

	return nil
}

// doBucketHealthCheck checks if the bucket resource exists and then checks if the service account linked to it
// has sufficient permissions for writing objects.
func doBucketHealthCheck(ctx context.Context, bucket *storage.BucketHandle) error {
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return errors.Wrap(err, "bucket is not accessible")
	}

	return testPermissions(ctx, bucket.IAM(), []string{"storage.objects.create"})
}
