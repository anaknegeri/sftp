# Use the base image from the root Dockerfile
FROM golang:1.24-alpine AS builder

# Install git
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go.work and all module files
COPY go.work go.work.sum ./
COPY api-service/go.mod api-service/go.sum ./api-service/
COPY sftp-service/go.mod sftp-service/go.sum ./sftp-service/
COPY notification-service/go.mod notification-service/go.sum ./notification-service/

# Copy the source code
COPY api-service/ ./api-service/
COPY sftp-service/ ./sftp-service/
COPY notification-service/ ./notification-service/

# Set up Go workspace
RUN go work use ./sftp-service ./sftp-service

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -o /app/bin/sftp-service ./sftp-service/cmd/app

# Final stage
FROM alpine:3.21

# Add ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/bin/sftp-service .

# Set timezone to UTC
ENV TZ=Asia/Jakarta

# Run the binary
CMD ["./sftp-service"]
