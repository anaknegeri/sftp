package utils

import (
	"jarvist/sftp/internal/domain/entity"
	"os"
	"path/filepath"
)

func DetermineFileType(fileName string) string {
	if filepath.Ext(fileName) != ".csv" {
		return entity.FileType30Min
	}

	// Check daily report first (format: locationcode_YYYYMMDD.csv)
	if len(fileName) > 12 {
		possibleDate := fileName[len(fileName)-12 : len(fileName)-4] // Extract YYYYMMDD part
		if len(possibleDate) == 8 && possibleDate[0] == '2' && possibleDate[1] == '0' {
			// Verify it's pure date format (no time component after)
			afterDate := fileName[len(fileName)-4:] // Should be ".csv"
			if afterDate == ".csv" {
				return entity.FileTypeDaily
			}
		}
	}

	// Check 30-minute report (format: locationcode_YYYYMMDD_HHMM.csv)
	if len(fileName) > 17 {
		possibleTime := fileName[len(fileName)-8 : len(fileName)-4] // Extract HHMM part
		if len(possibleTime) == 4 {
			hour := possibleTime[0:2]
			minute := possibleTime[2:4]
			if hour >= "00" && hour <= "23" && minute >= "00" && minute <= "59" {
				return entity.FileType30Min
			}
		}
	}

	// Default to daily if pattern doesn't match 30-minute format
	return entity.FileTypeDaily
}

func CountFileRecords(filePath string) int {
	file, err := os.Open(filePath)
	if err != nil {
		return 0
	}
	defer file.Close()

	lineCount := 0
	buffer := make([]byte, 32*1024)

	for {
		c, err := file.Read(buffer)
		if err != nil {
			break
		}
		for i := 0; i < c; i++ {
			if buffer[i] == '\n' {
				lineCount++
			}
		}
	}

	if lineCount > 0 {
		lineCount--
	}
	return lineCount
}
