package configuration

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-playground/validator/v10"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
	profilingconfig "github.com/armadaproject/armada/internal/common/profiling/configuration"
)

// QueryIngesterConfig is the config object for the Query Ingester
type QueryIngesterConfig struct {
	// Port on which prometheus metrics will be served
	MetricsPort uint16
	// Number of event messages that will be batched together before being inserted into the database
	BatchSize int
	// Maximum time since the last batch before a batch will be inserted into the database
	BatchDuration time.Duration
	// Pulsar Subscription Configuration
	Pulsar commonconfig.PulsarConfig
	// Clickhouse Configuration
	ClickHouse ClickHouseConfig
	// User annotations have a common prefix to avoid clashes with other annotations.  This prefix will be stripped from
	// The annotation before storing in the db
	UserAnnotationPrefix string
	// If non-nil, configures pprof profiling
	Profiling *profilingconfig.ProfilingConfig
}

func (c QueryIngesterConfig) Validate() error {
	validate := validator.New()
	return validate.Struct(c)
}

// TODO: put this in a clickhouse pakage in common
type TLSConfig struct {
	Enabled            bool
	InsecureSkipVerify bool
	ServerName         string
	CAFile             string
	CertFile           string
	KeyFile            string
}

type ClickHouseConfig struct {
	Addr             []string
	Database         string
	Username         string
	Password         string
	Debug            bool
	Settings         map[string]any
	Compression      string
	DialTimeout      time.Duration
	MaxOpenConns     int
	MaxIdleConns     int
	ConnMaxLifetime  time.Duration
	ConnOpenStrategy string
	FreeBufOnRelease bool
	TLS              TLSConfig
}

// ---- Builder: Config -> clickhouse.Options (TCP only) ----

func (c ClickHouseConfig) BuildOptions() (*clickhouse.Options, error) {
	addrs := normalizeAddrs(c.Addr) // ensure :9000 and proper IPv6 bracketing

	opts := &clickhouse.Options{
		Protocol:             clickhouse.Native, // TCP only
		Addr:                 addrs,
		Auth:                 clickhouse.Auth{Database: c.Database, Username: c.Username, Password: c.Password},
		Debug:                c.Debug,
		Settings:             clickhouse.Settings(c.Settings),
		FreeBufOnConnRelease: c.FreeBufOnRelease,
	}

	// Compression
	switch strings.ToLower(strings.TrimSpace(c.Compression)) {
	case "", "lz4":
		opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	case "zstd":
		opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionZSTD}
	case "none":
		// leave nil
	default:
		return nil, fmtErr("unsupported compression %q (use lz4 | zstd | none)", c.Compression)
	}

	// Pool/timeout tuning (only set when provided)
	if c.DialTimeout > 0 {
		opts.DialTimeout = c.DialTimeout
	}
	if c.MaxOpenConns > 0 {
		opts.MaxOpenConns = c.MaxOpenConns
	}
	if c.MaxIdleConns > 0 {
		opts.MaxIdleConns = c.MaxIdleConns
	}
	if c.ConnMaxLifetime > 0 {
		opts.ConnMaxLifetime = c.ConnMaxLifetime
	}
	switch strings.ToLower(c.ConnOpenStrategy) {
	case "":
		// default
	case "in_order":
		opts.ConnOpenStrategy = clickhouse.ConnOpenInOrder
	case "round_robin":
		opts.ConnOpenStrategy = clickhouse.ConnOpenRoundRobin
	default:
		return nil, fmtErr("unsupported conn_open_strategy %q (use in_order | round_robin)", c.ConnOpenStrategy)
	}

	// TLS (optional)
	if c.TLS.Enabled {
		tlsConf := &tls.Config{
			InsecureSkipVerify: c.TLS.InsecureSkipVerify,
			ServerName:         c.TLS.ServerName,
		}
		// Root CAs
		if c.TLS.CAFile != "" {
			pem, err := os.ReadFile(c.TLS.CAFile)
			if err != nil {
				return nil, err
			}
			cp := x509.NewCertPool()
			if !cp.AppendCertsFromPEM(pem) {
				return nil, fmtErr("failed to parse ca_file %q", c.TLS.CAFile)
			}
			tlsConf.RootCAs = cp
		}
		// Client cert
		if c.TLS.CertFile != "" && c.TLS.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(c.TLS.CertFile, c.TLS.KeyFile)
			if err != nil {
				return nil, err
			}
			tlsConf.Certificates = []tls.Certificate{cert}
		}
		opts.TLS = tlsConf
	}
	return opts, nil
}

func normalizeAddrs(in []string) []string {
	out := make([]string, 0, len(in))
	for _, a := range in {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		// If a has a port -> keep; else add :9000.
		if _, _, err := net.SplitHostPort(a); err == nil {
			out = append(out, a)
		} else {
			out = append(out, net.JoinHostPort(a, "9000")) // adds brackets for IPv6 automatically
		}
	}
	return out
}

func fmtErr(format string, v ...any) error {
	return fmt.Errorf(format, v...)
}
