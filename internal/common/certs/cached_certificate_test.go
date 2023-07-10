package certs

import (
	"bytes"
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

const certFilePath = "testdata/tls.crt"
const keyFilePath = "testdata/tls.key"

func TestCachedCertificateService_LoadsCertificateOnStartup(t *testing.T) {
	defer cleanup()
	cert, certData, keyData := createCerts()
	writeCerts(t, certData, keyData)

	cachedCertService := NewCachedCertificateService(certFilePath, keyFilePath)

	result := cachedCertService.GetCertificate()

	assert.Equal(t, cert, result)
}

func TestCachedCertificateService_PanicIfInitialLoadFails(t *testing.T) {
	defer cleanup()

	assert.Panics(t, func() { NewCachedCertificateService(certFilePath, keyFilePath) })
}

func TestCachedCertificateService_ReloadsCert_IfFileOnDiskChanges(t *testing.T) {
	defer cleanup()
	cert, certData, keyData := createCerts()
	writeCerts(t, certData, keyData)
	cachedCertService := NewCachedCertificateService(certFilePath, keyFilePath)

	assert.Equal(t, cert, cachedCertService.GetCertificate())

	newCert, certData, keyData := createCerts()

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
	originalCert, certData, keyData := createCerts()
	writeCerts(t, certData, keyData)
	cachedCertService := NewCachedCertificateService(certFilePath, keyFilePath)

	assert.Equal(t, originalCert, cachedCertService.GetCertificate())
	
	newCert, certData, keyData := createCerts()

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

func writeCerts(t *testing.T, certData *bytes.Buffer, keyData *bytes.Buffer) {
	if certData != nil {
		err := os.WriteFile(certFilePath, certData.Bytes(), 0644)
		require.NoError(t, err)
	}

	if keyData != nil {
		err := os.WriteFile(keyFilePath, keyData.Bytes(), 0644)
		require.NoError(t, err)
	}
}

func cleanup() {
	os.Remove(certFilePath)
	os.Remove(keyFilePath)
}

func createCerts() (*tls.Certificate, *bytes.Buffer, *bytes.Buffer) {
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
	if err != nil {
		panic(err)
	}

	// create the CA
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		panic(err)
	}

	// pem encode
	caPEM := new(bytes.Buffer)
	pem.Encode(caPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})

	caPrivKeyPEM := new(bytes.Buffer)
	pem.Encode(caPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(caPrivKey),
	})

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
	if err != nil {
		panic(err)
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, cert, ca, &certPrivKey.PublicKey, caPrivKey)
	if err != nil {
		panic(err)
	}

	certPEM := new(bytes.Buffer)
	pem.Encode(certPEM, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})

	certPrivKeyPEM := new(bytes.Buffer)
	pem.Encode(certPrivKeyPEM, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
	})

	certificate, err := tls.X509KeyPair(certPEM.Bytes(), certPrivKeyPEM.Bytes())
	if err != nil {
		panic(err)
	}

	return &certificate, certPEM, certPrivKeyPEM
}
