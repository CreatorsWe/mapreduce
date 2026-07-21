# Makefile
.PHONY: proto
proto:
	protoc --go-grpc_out=rpc --go_out=rpc rpc/*.proto
