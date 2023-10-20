package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	if err := exportDataToSQLFile(); err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Data exported to output.sql")
	}
}

func exportDataToSQLFile() error {
	db, err := connectDB()
	if err != nil {
		return err
	}
	defer db.Close()

	query := `SELECT 
    amr.date, 
    amr.department, 
    amr.file_type, 
    amr.part_number, 
    amr.part_description, 
    amr.to_date_aval, 
    amr.net_month_shortage, 
    amr.amr, 
    amr.created_by, 
    amr.created_at, 
    amr.updated_by, 
    amr.updated_at,
    part.part_id,
    department.departmentid AS department_id,
    files.id AS file_type_id
FROM amr
JOIN part ON amr.part_number = part.part_number
JOIN department ON amr.department = department.department
JOIN files ON amr.file_type = files.file_name
WHERE amr.date > '2023-01-01'`
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	file, err := os.Create("output.sql")
	if err != nil {
		return err
	}
	defer file.Close()

	for rows.Next() {
		var date, department, file_type, part_number, part_description, to_date_aval, net_month_shortage, amr, created_by, created_at, updated_by, updated_at, part_id, department_id, file_type_id string
		if err := rows.Scan(&date, &department, &file_type, &part_number, &part_description, &to_date_aval, &net_month_shortage, &amr, &created_by, &created_at, &updated_by, &updated_at, &part_id, &department_id, &file_type_id); err != nil {
			return err
		}
		// Create SQL INSERT statements and write to the file
		insertSQL := fmt.Sprintf("INSERT INTO table_name (date, department, file_type, part_number, part_description, to_date_aval, net_month_shortage, amr, created_by, created_at, updated_by, updated_at, part_id, department_id, file_type_id) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');\n", date, department, file_type, part_number, part_description, to_date_aval, net_month_shortage, amr, created_by, created_at, updated_by, updated_at, part_id, department_id, file_type_id)
		_, err = file.WriteString(insertSQL)
		if err != nil {
			return err
		}
	}

	return nil
}

func connectDB() (*sql.DB, error) {
	your_postgres_connection_string := "host=localhost port=5432 dbname=ceo_assist user=postgres password=postgres sslmode=disable"

	db, err := sql.Open("postgres", your_postgres_connection_string)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func runQueryAndSaveToCSV(db *sql.DB, query string, filename string) error {
	// Execute the SQL query
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Create a new CSV file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the header row
	headerRow := []string{
		"id",
		"department",
		"department_id",
		"file_type",
		"file_type_id",
		"part_number",
		"part_description",
		"part_id",
		"to_date_aval",
		"net_month_shortage",
		"amr",
		"created_by",
		"created_at",
		"updated_by",
		"updated_at",
		"date",
	}
	err = writer.Write(headerRow)
	if err != nil {
		return err
	}

	// Iterate through the result rows and write to the CSV file
	for rows.Next() {
		var (
			id               int
			department       string
			departmentID     int
			fileType         string
			fileTypeID       int
			partNumber       string
			partDescription  string
			partID           int
			toDateAval       string
			netMonthShortage float64
			amr              float64
			createdBy        string
			createdAt        string
			updatedBy        string
			updatedAt        string
			date             string
		)

		err := rows.Scan(
			&id,
			&department,
			&departmentID,
			&fileType,
			&fileTypeID,
			&partNumber,
			&partDescription,
			&partID,
			&toDateAval,
			&netMonthShortage,
			&amr,
			&createdBy,
			&createdAt,
			&updatedBy,
			&updatedAt,
			&date,
		)
		if err != nil {
			return err
		}

		// Create a new row for the CSV
		csvRow := []string{
			fmt.Sprintf("%d", id),
			department,
			fmt.Sprintf("%d", departmentID),
			fileType,
			fmt.Sprintf("%d", fileTypeID),
			partNumber,
			partDescription,
			fmt.Sprintf("%d", partID),
			toDateAval,
			fmt.Sprintf("%f", netMonthShortage),
			fmt.Sprintf("%f", amr),
			createdBy,
			createdAt,
			updatedBy,
			updatedAt,
			date,
		}

		// Write the row to the CSV file
		err = writer.Write(csvRow)
		if err != nil {
			return err
		}
	}

	return nil
}
