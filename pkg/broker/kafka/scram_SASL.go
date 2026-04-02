package kafka

// ScramSASLConfig defines the configuration properties needed for using SASL/SCRAM-SHA-512 authentication.
//
// User and Pass fields need to be set in order to successfully initialize the SASL/SCRAM-SHA-512 authentication.
type ScramSASLConfig struct {
	// Zid is an optional authorization ID to use in authenticating.
	Zid string

	// User is username to use for authentication.
	User string

	// Pass is the password to use for authentication.
	Pass string
}
