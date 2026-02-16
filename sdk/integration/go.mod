module github.com/mstrYoda/goraphdb/sdk/integration

go 1.24.3

require (
	github.com/mstrYoda/goraphdb v0.0.0
	github.com/mstrYoda/goraphdb/sdk/goraphdb v0.0.0
)

require (
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.etcd.io/bbolt v1.4.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
)

replace (
	github.com/mstrYoda/goraphdb => ../../
	github.com/mstrYoda/goraphdb/sdk/goraphdb => ../goraphdb
)
