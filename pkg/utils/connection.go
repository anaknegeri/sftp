package utils

import "strings"

// ConnectionErrorKeywords contains common connection error patterns
var ConnectionErrorKeywords = []string{
	"connection lost",
	"connection reset",
	"connection refused",
	"broken pipe",
	"network is unreachable",
	"connection timed out",
	"EOF",
	"ssh: disconnect",
	"use of closed network connection",
	"connection closed",
	"pipe: broken pipe",
	"i/o timeout",
	"client not connected",
	"session not connected",
	"connection reset by peer",
	"no route to host",
	"network unreachable",
	"connection aborted",
	"transport endpoint is not connected",
}

// IsConnectionError checks if the error indicates a connection problem
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	return IsConnectionErrorString(err.Error())
}

// IsConnectionErrorString checks if the error string indicates a connection problem
func IsConnectionErrorString(errorStr string) bool {
	if errorStr == "" {
		return false
	}

	// Convert to lowercase for case-insensitive comparison
	lowerErrorStr := strings.ToLower(errorStr)

	for _, connErr := range ConnectionErrorKeywords {
		if ContainsSubstring(lowerErrorStr, strings.ToLower(connErr)) {
			return true
		}
	}

	return false
}

// Contains checks if string s contains substring
func Contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

// ContainsSubstring is an alternative implementation that checks substring manually
// Kept for compatibility with existing code that might need manual checking
func ContainsSubstring(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(s) < len(substr) {
		return false
	}

	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ContainsAny checks if string s contains any of the given substrings
func ContainsAny(s string, substrings []string) bool {
	for _, substr := range substrings {
		if Contains(s, substr) {
			return true
		}
	}
	return false
}

// IsTemporaryError checks if the error might be temporary and worth retrying
func IsTemporaryError(err error) bool {
	if err == nil {
		return false
	}

	errorStr := strings.ToLower(err.Error())
	temporaryErrors := []string{
		"timeout",
		"temporary failure",
		"try again",
		"resource temporarily unavailable",
		"operation would block",
		"interrupted system call",
	}

	return ContainsAny(errorStr, temporaryErrors)
}

// IsNetworkError checks if the error is network-related
func IsNetworkError(err error) bool {
	if err == nil {
		return false
	}

	errorStr := strings.ToLower(err.Error())
	networkErrors := []string{
		"network",
		"dns",
		"host",
		"route",
		"unreachable",
		"timeout",
		"connection",
	}

	return ContainsAny(errorStr, networkErrors)
}

// IsAuthenticationError checks if the error is authentication-related
func IsAuthenticationError(err error) bool {
	if err == nil {
		return false
	}

	errorStr := strings.ToLower(err.Error())
	authErrors := []string{
		"authentication failed",
		"permission denied",
		"access denied",
		"unauthorized",
		"invalid credentials",
		"login failed",
		"bad password",
		"public key",
		"private key",
	}

	return ContainsAny(errorStr, authErrors)
}

// GetErrorCategory categorizes the error type
func GetErrorCategory(err error) string {
	if err == nil {
		return "none"
	}

	if IsConnectionError(err) {
		return "connection"
	}
	if IsAuthenticationError(err) {
		return "authentication"
	}
	if IsTemporaryError(err) {
		return "temporary"
	}
	if IsNetworkError(err) {
		return "network"
	}

	return "unknown"
}
