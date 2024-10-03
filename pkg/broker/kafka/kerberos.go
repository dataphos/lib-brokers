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

import (
	krbclient "github.com/jcmturner/gokrb5/v8/client"
	krbconf "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
)

// KerberosConfig defines the configuration properties needed for using SASL Kerberos authentication.
//
// All fields of the struct need to be set in order to successfully initialize the Kerberos authentication.
// Currently, only supports authenticating with kerberos via Keytab and conf file.
type KerberosConfig struct {
	// KeyTabPath path to the keytab file.
	//
	// A keytab is a file containing pairs of Kerberos principals and encrypted keys that are derived from
	// the Kerberos password. You can use this file to log on to Kerberos without being prompted for a password.
	// One of the ways to get the keytab and config files can be found here:
	// https://syntio.atlassian.net/wiki/spaces/SJ/pages/2105475207/Kerberized+Janitor#Getting-the-Kerberos-files
	KeyTabPath string

	// ConfigPath krb5.conf path.
	//
	// The krb5. conf file contains Kerberos configuration information, including the locations of KDCs and
	// administration daemons for the Kerberos realms of interest, defaults for the current realm and for Kerberos
	// applications, and mappings of host names onto Kerberos realms. This file must reside on all Kerberos clients.
	ConfigPath string

	// A Kerberos Realm is the domain over which a Kerberos authentication server has the authority to
	// authenticate a user, host or service. A realm name is often, but not always the upper case version
	// of the name of the DNS domain over which it presides.
	Realm string

	// A Kerberos Service is the service name we will get a ticket for.
	Service string

	// Username of the service principal.
	Username string
}

func configureKerberos(opts []kgo.Opt, krb5conf *krbconf.Config, krb5keytab *keytab.Keytab, username string, realm string, service string) []kgo.Opt {
	opts = append(opts, kgo.SASL(kerberos.Auth{
		Client: krbclient.NewWithKeytab(
			username,
			realm,
			krb5keytab,
			krb5conf,
		),
		ClientFn:         nil,
		Service:          service,
		PersistAfterAuth: false,
	}.AsMechanism()))

	return opts
}
