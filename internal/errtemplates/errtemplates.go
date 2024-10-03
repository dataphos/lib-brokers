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

// Package errtemplates offers convenience functions to standardize error messages and simplify proper error wrapping.
package errtemplates

import (
	"strings"

	"github.com/pkg/errors"
)

const (
	permissionLevelNotSufficientTemplate = "provided permission level is not sufficient, requested permissions: %s; provided: %s"
	batchSizeTooBigTemplate              = "batch size needs to be less than %d but %d was provided"
)

// PermissionLevelNotSufficient returns an error stating that given permissions to a resource are not sufficient.
func PermissionLevelNotSufficient(requested []string, given []string) error {
	return errors.Errorf(permissionLevelNotSufficientTemplate, strings.Join(requested, ","), strings.Join(given, ","))
}

func BatchSizeTooBig(max int, provided int) error {
	return errors.Errorf(batchSizeTooBigTemplate, max, provided)
}
