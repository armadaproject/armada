package certs

import (
	"context"
	"crypto/tls"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type CachedCertificateService struct {
	certPath string
	keyPath  string

	fileInfoLock sync.Mutex
	certFileInfo os.FileInfo
	keyFileInfo  os.FileInfo

	certificateLock sync.Mutex
	certificate     *tls.Certificate

	refreshInterval time.Duration
}

func NewCachedCertificateService(certPath string, keyPath string, refreshInternal time.Duration) *CachedCertificateService {
	cert := &CachedCertificateService{
		certPath:        certPath,
		keyPath:         keyPath,
		certificateLock: sync.Mutex{},
		fileInfoLock:    sync.Mutex{},
		refreshInterval: refreshInternal,
	}
	// Initialise the certificate
	err := cert.refresh()
	if err != nil {
		panic(err)
	}
	return cert
}

func (c *CachedCertificateService) GetCertificate() *tls.Certificate {
	c.certificateLock.Lock()
	defer c.certificateLock.Unlock()
	return c.certificate
}

func (c *CachedCertificateService) updateCertificate(certificate *tls.Certificate) {
	c.certificateLock.Lock()
	defer c.certificateLock.Unlock()
	c.certificate = certificate
}

func (c *CachedCertificateService) Run(ctx context.Context) {
	ticker := time.NewTicker(c.refreshInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := c.refresh()
			if err != nil {
				log.WithError(err).Errorf("failed refreshing certificate from files cert: %s key: %s", c.certPath, c.keyPath)
			}
		}
	}
}

func (c *CachedCertificateService) refresh() error {
	updatedCertFileInfo, err := os.Stat(c.certPath)
	if err != nil {
		return err
	}

	updatedKeyFileInfo, err := os.Stat(c.keyPath)
	if err != nil {
		return err
	}

	modified := false

	if c.certFileInfo == nil || updatedCertFileInfo.ModTime().After(c.certFileInfo.ModTime()) {
		modified = true
	}

	if c.keyFileInfo == nil || updatedKeyFileInfo.ModTime().After(c.keyFileInfo.ModTime()) {
		modified = true
	}

	if modified {
		log.Infof("refreshing certificate from files cert: %s key: %s", c.certPath, c.keyPath)
		certFileData, err := os.ReadFile(c.certPath)
		if err != nil {
			return err
		}

		keyFileData, err := os.ReadFile(c.keyPath)
		if err != nil {
			return err
		}

		cert, err := tls.X509KeyPair(certFileData, keyFileData)
		if err != nil {
			return err
		}

		c.updateData(updatedCertFileInfo, updatedKeyFileInfo, &cert)
	}

	return nil
}

func (c *CachedCertificateService) updateData(certFileInfo os.FileInfo, keyFileInfo os.FileInfo, newCert *tls.Certificate) {
	c.fileInfoLock.Lock()
	defer c.fileInfoLock.Unlock()
	c.certFileInfo = certFileInfo
	c.keyFileInfo = keyFileInfo

	c.updateCertificate(newCert)
}
