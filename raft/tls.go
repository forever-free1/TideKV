package raft

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfig TLS 配置
type TLSConfig struct {
	// 证书文件
	CertFile string
	// 私钥文件
	KeyFile string
	// CA 证书文件（用于 mTLS 客户端验证）
	CAFile string
	// 是否启用客户端证书验证（mTLS）
	VerifyClientCert bool
}

// LoadTLSCertificate 加载 TLS 证书
func LoadTLSCertificate(certFile, keyFile string) (tls.Certificate, error) {
	return tls.LoadX509KeyPair(certFile, keyFile)
}

// LoadCACertPool 加载 CA 证书池
func LoadCACertPool(caFile string) (*x509.CertPool, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("读取 CA 证书失败: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("解析 CA 证书失败")
	}

	return certPool, nil
}

// NewServerTLSConfig 创建服务器 TLS 配置
func NewServerTLSConfig(cfg *TLSConfig) (*tls.Config, error) {
	cert, err := LoadTLSCertificate(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("加载服务器证书失败: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// 如果配置了 CA，则启用客户端证书验证（mTLS）
	if cfg.CAFile != "" {
		caPool, err := LoadCACertPool(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("加载 CA 证书失败: %w", err)
		}

		tlsConfig.ClientCAs = caPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsConfig, nil
}

// NewClientTLSConfig 创建客户端 TLS 配置
func NewClientTLSConfig(cfg *TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// 如果配置了 CA，则验证服务器证书
	if cfg.CAFile != "" {
		caPool, err := LoadCACertPool(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("加载 CA 证书失败: %w", err)
		}
		tlsConfig.RootCAs = caPool
	}

	// 如果配置了客户端证书，则使用 mTLS
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := LoadTLSCertificate(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("加载客户端证书失败: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}
