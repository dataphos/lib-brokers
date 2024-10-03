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

package kafka

import "github.com/prometheus/client_golang/prometheus"

// PrometheusConfig defines the configuration properties needed for exposing Prometheus metrics.
//
// All fields of the struct need to be set in order to successfully initialize metrics.
type PrometheusConfig struct {
	// Namespace is a prefix relevant to the domain the metric belongs to.
	Namespace string

	// Registerer is the Prometheus interface for the part of a registry
	// in charge of registering and unregistering metrics.
	// Users of custom registries should use Registerer as type for registration purposes
	// rather than the Registry type directly.
	Registerer prometheus.Registerer

	// Gatherer is the Prometheus interface for the part of a registry in charge of
	// gathering the collected metrics.
	// Same as Registerer, users should use Gatherer as type for gathering purposes
	// rather than the Registry type directly.
	Gatherer prometheus.Gatherer
}
