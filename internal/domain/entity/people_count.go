package entity

type DailyReport struct {
	Date         string
	TenantID     string
	LocationID   string
	LocationCode string
	LocationName string
	DeviceID     string
	DeviceName   string
	TotalIn      int64
	TotalOut     int64
}
