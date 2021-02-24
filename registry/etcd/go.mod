module github.com/eaglc/fit/registry/etcd

go 1.14

require (
	github.com/coreos/bbolt v1.3.5 // indirect
	github.com/coreos/etcd v3.3.25+incompatible // indirect
	github.com/eaglc/lamer v0.0.2-alpha.0
	github.com/prometheus/client_golang v1.9.0 // indirect
	go.etcd.io/etcd v3.3.13+incompatible
	golang.org/x/crypto v0.0.0-20201221181555-eec23a3978ad // indirect
	google.golang.org/grpc v1.34.1 // indirect
)

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.5

replace google.golang.org/grpc => google.golang.org/grpc v1.26.0
