package podwriter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdClientConfig configures the etcd connection.
type EtcdClientConfig struct {
	Endpoints   []string
	CAFile      string
	CertFile    string
	KeyFile     string
	DialTimeout time.Duration
}

// NewEtcdClient builds an etcd client. TLS is optional; when one certificate
// path is set, all three must be set.
func NewEtcdClient(cfg EtcdClientConfig) (*clientv3.Client, error) {
	tlsConfig, err := loadTLSConfig(cfg.CAFile, cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	return clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: cfg.DialTimeout,
		TLS:         tlsConfig,
	})
}

func loadTLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	if caFile == "" && certFile == "" && keyFile == "" {
		return nil, nil
	}
	if caFile == "" || certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("etcd TLS requires CA file, certificate file and key file")
	}
	caData, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read etcd CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caData) {
		return nil, fmt.Errorf("parse etcd CA: no certificates found")
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load etcd client certificate: %w", err)
	}
	return &tls.Config{
		RootCAs:      pool,
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// EtcdKV adapts a clientv3 KV to the writer's small KV interface.
type EtcdKV struct {
	client *clientv3.Client
}

func NewEtcdKV(client *clientv3.Client) *EtcdKV {
	return &EtcdKV{client: client}
}

func (e *EtcdKV) CreateBatch(ctx context.Context, kvs []KeyValue) (bool, error) {
	compares := make([]clientv3.Cmp, 0, len(kvs))
	puts := make([]clientv3.Op, 0, len(kvs))
	for _, kv := range kvs {
		compares = append(compares, clientv3.Compare(clientv3.Version(kv.Key), "=", 0))
		puts = append(puts, clientv3.OpPut(kv.Key, string(kv.Value)))
	}
	response, err := e.client.Txn(ctx).If(compares...).Then(puts...).Commit()
	if err != nil {
		return false, err
	}
	return response.Succeeded, nil
}

func (e *EtcdKV) Get(ctx context.Context, key string) ([]byte, error) {
	response, err := e.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(response.Kvs) == 0 {
		return nil, nil
	}
	return response.Kvs[0].Value, nil
}
