package entity

type Location struct {
	ID           string `gorm:"type:uuid;primaryKey"`
	TenantID     string `gorm:"type:uuid;not null"`
	Name         string `gorm:"type:varchar(100);not null"`
	LocationCode string `gorm:"type:varchar(50);not null"`
	Status       string `gorm:"type:varchar(20);default:'active'"`
}
