package config

import "github.com/joho/godotenv"

type TenantConfig struct {
	ID      string
	Name    string
	SFTP    SFTPConfig
	Enabled bool
}

type SFTPConfig struct {
	Host         string
	Port         string
	User         string
	Password     string
	KeyPath      string
	BasePath     string
	CombinedPath string
}

// GetTenants returns tenant configurations
func GetTenants() map[string]*TenantConfig {
	godotenv.Load()
	return map[string]*TenantConfig{
		"e7f12e57-271a-42f9-8f44-ac238924a131": {
			ID:   "e7f12e57-271a-42f9-8f44-ac238924a131",
			Name: "MAP",
			SFTP: SFTPConfig{
				Host:         getEnv("SFTP_HOST", "localhost"),
				Port:         getEnv("SFTP_PORT", "22"),
				User:         getEnv("SFTP_USER", "user"),
				Password:     getEnv("SFTP_PASSWORD", "password"),
				KeyPath:      getEnv("SFTP_KEY_PATH", ""),
				BasePath:     getEnv("SFTP_PATH", "/upload"),
				CombinedPath: getEnv("SFTP_COMBINED_PATH", "/upload/combined"),
			},
			Enabled: true,
		},
		"192de208-42dc-4fdf-99a0-2b5f9b1cc124": {
			ID:   "192de208-42dc-4fdf-99a0-2b5f9b1cc124",
			Name: "MAP Fasion",
			SFTP: SFTPConfig{
				Host:         getEnv("SFTP_HOST", "localhost"),
				Port:         getEnv("SFTP_PORT", "22"),
				User:         getEnv("SFTP_USER", "user"),
				Password:     getEnv("SFTP_PASSWORD", "password"),
				KeyPath:      getEnv("SFTP_KEY_PATH", ""),
				BasePath:     getEnv("SFTP_PATH", "/upload"),
				CombinedPath: getEnv("SFTP_COMBINED_PATH", "/upload/combined"),
			},
			Enabled: true,
		},
	}
}

// GetEnabledTenants returns only enabled tenants
func GetEnabledTenants() map[string]*TenantConfig {
	allTenants := GetTenants()
	enabled := make(map[string]*TenantConfig)

	for id, tenant := range allTenants {
		if tenant.Enabled {
			enabled[id] = tenant
		}
	}

	return enabled
}

// GetTenantByID returns specific tenant configuration
func GetTenantByID(tenantID string) (*TenantConfig, bool) {
	tenants := GetTenants()
	tenant, exists := tenants[tenantID]
	return tenant, exists && tenant.Enabled
}
