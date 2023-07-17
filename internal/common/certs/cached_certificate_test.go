package certs

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	certFilePath = "testdata/tls.crt"
	keyFilePath  = "testdata/tls.key"
)

func TestCachedCertificateService_LoadsCertificateOnStartup(t *testing.T) {
	defer cleanup()
	cert, certData, keyData := createCerts(t)
	writeCerts(t, certData, keyData)

	cachedCertService := NewCachedCertificateService(certFilePath, keyFilePath, time.Second)

	result := cachedCertService.GetCertificate()

	assert.Equal(t, cert, result)
}

func TestCachedCertificateService_PanicIfInitialLoadFails(t *testing.T) {
	defer cleanup()

	assert.Panics(t, func() { NewCachedCertificateService(certFilePath, keyFilePath, time.Second) })
}

func TestCachedCertificateService_ReloadsCert_IfFileOnDiskChanges(t *testing.T) {
	defer cleanup()
	cert, certData, keyData := createCerts(t)
	writeCerts(t, certData, keyData)
	cachedCertService := NewCachedCertificateService(certFilePath, keyFilePath, time.Second)

	assert.Equal(t, cert, cachedCertService.GetCertificate())

	newCert, certData, keyData := createCerts(t)

	// Update files on disk
	writeCerts(t, certData, keyData)
	// Certificate won't change until refresh is called
	assert.NotEqual(t, newCert, cachedCertService.GetCertificate())

	err := cachedCertService.refresh()
	assert.NoError(t, err)
	assert.Equal(t, newCert, cachedCertService.GetCertificate())
}

func TestCachedCertificateService_HandlesPartialUpdates(t *testing.T) {
	defer cleanup()
	originalCert, certData, keyData := createCerts(t)
	writeCerts(t, certData, keyData)
	cachedCertService := NewCachedCertificateService(certFilePath, keyFilePath, time.Second)

	assert.Equal(t, originalCert, cachedCertService.GetCertificate())

	newCert, certData, keyData := createCerts(t)

	// Update only 1 file on disk - which leaves the representation on disk in an invalid state
	writeCerts(t, certData, nil)
	err := cachedCertService.refresh()
	assert.Error(t, err)

	// Certificate provided should not change, as there is no valid new cert yet
	assert.Equal(t, originalCert, cachedCertService.GetCertificate())

	// Update the other file, so now files on disk are now both updated and consistent
	writeCerts(t, nil, keyData)
	err = cachedCertService.refresh()
	assert.NoError(t, err)

	assert.Equal(t, newCert, cachedCertService.GetCertificate())
}

func TestCachedCertificateService_ReloadsCertPeriodically_WhenUsingRun(t *testing.T) {
	defer cleanup()
	cert, certData, keyData := createCerts(t)
	writeCerts(t, certData, keyData)
	cachedCertService := NewCachedCertificateService(certFilePath, keyFilePath, time.Second)
	assert.Equal(t, cert, cachedCertService.GetCertificate())

	go func() {
		cachedCertService.Run(context.Background())
	}()

	newCert, certData, keyData := createCerts(t)
	writeCerts(t, certData, keyData)
	time.Sleep(time.Second * 2)
	assert.Equal(t, newCert, cachedCertService.GetCertificate())
}

func writeCerts(t *testing.T, certData *bytes.Buffer, keyData *bytes.Buffer) {
	if certData != nil {
		err := os.WriteFile(certFilePath, certData.Bytes(), 0o644)
		require.NoError(t, err)
	}

	if keyData != nil {
		err := os.WriteFile(keyFilePath, keyData.Bytes(), 0o644)
		require.NoError(t, err)
	}
}

func cleanup() {
	os.Remove(certFilePath)
	os.Remove(keyFilePath)
}

func createCerts(t *testing.T) (*tls.Certificate, *bytes.Buffer, *bytes.Buffer) {
	// set up our CA certificate
	ca := &x509.Certificate{
		SerialNumber:          big.NewInt(2019),
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// create our private and public key
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)

	// create the CA
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	// pem encode
	caPEM := new(bytes.Buffer)
	err = pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})
	require.NoError(t, err)

	caPrivKeyPEM := new(bytes.Buffer)
	err = pem.Encode(caPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(caPrivKey),
	})
	require.NoError(t, err)

	// set up our server certificate
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(2019),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	certPrivKey, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)

	certBytes, err := x509.CreateCertificate(rand.Reader, cert, ca, &certPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	certPEM := new(bytes.Buffer)
	err = pem.Encode(certPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})
	require.NoError(t, err)

	certPrivKeyPEM := new(bytes.Buffer)
	err = pem.Encode(certPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
	})
	require.NoError(t, err)

	certificate, err := tls.X509KeyPair(certPEM.Bytes(), certPrivKeyPEM.Bytes())
	require.NoError(t, err)

	return &certificate, certPEM, certPrivKeyPEM
}
