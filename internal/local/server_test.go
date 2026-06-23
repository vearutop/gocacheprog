package local

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/acme/autocert"
)

func TestWarmAutocertCertificateLoadsCachedECDSACert(t *testing.T) {
	cacheDir := t.TempDir()
	cache := autocert.DirCache(cacheDir)
	require.NoError(t, cache.Put(context.Background(), "example.com", testAutocertPEM(t, "example.com")))

	manager := &autocert.Manager{
		Cache:      cache,
		Prompt:     autocert.AcceptTOS,
		HostPolicy: autocert.HostWhitelist("example.com"),
	}

	require.NoError(t, warmAutocertCertificate(manager, "example.com"))
}

func testAutocertPEM(t *testing.T, host string) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: host},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		DNSNames:              []string{host},
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	var out bytes.Buffer
	require.NoError(t, pem.Encode(&out, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	require.NoError(t, pem.Encode(&out, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}))

	return out.Bytes()
}
