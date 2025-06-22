.PHONY: proto clean setup-dirs

proto: setup-dirs
	protoc  --go_out=. --go-grpc_out=. proto/sftp.proto
	@copy proto\sftp\*.go pkg\pb\ >nul
	@echo Protobuf files generated and copied successfully!

setup-dirs:
	@if not exist "proto\sftp" mkdir proto\sftp
	@if not exist "pkg\pb" mkdir pkg\pb

clean:
	@if exist "proto\sftp\*.go" del proto\sftp\*.go
	@if exist "pkg\pb\*.go" del pkg\pb\*.go
	@echo Cleaned generated files!
