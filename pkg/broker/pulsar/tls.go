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

package pulsar

import (
	"crypto/tls"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
)

// configureTLS adds options to configure the Pulsar client to use TLS.
func configureTLS(
	tlsConfig *tls.Config,
	tlsTrustCertsFilePath string,
	tlsAllowInsecureConnection bool,
	options pulsar.ClientOptions,
) (pulsar.ClientOptions, error) {
	authentication := pulsar.NewAuthenticationFromTLSCertSupplier(func() (*tls.Certificate, error) {
		// This seems to be the only way to supply a certificate without any context.
		if len(tlsConfig.Certificates) != 1 {
			return nil, errors.New("pulsar client supports TLS configuration only with exactly one certificate")
		}

		return &tlsConfig.Certificates[0], nil
	})

	options.Authentication = authentication

	// If insecure connections are not allowed, the path to trusted certificate must be set.
	if !tlsAllowInsecureConnection && tlsTrustCertsFilePath == "" {
		return options, errors.New("must provide TLSTrustCertsFilePath or allow insecure connection")
	}

	options.TLSTrustCertsFilePath = tlsTrustCertsFilePath
	options.TLSAllowInsecureConnection = tlsAllowInsecureConnection

	return options, nil
}
