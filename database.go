package gdt

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq" // Импортируем драйвер для PostgreSQL
)

// Database представляет структуру для работы с базой данных
type Database struct {
	conn *sql.DB
}

// NewDatabase создает новое подключение к базе данных
func NewDatabase(driver, dsn string) (*Database, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return &Database{conn: db}, nil
}

// Close закрывает подключение к базе данных
func (db *Database) Close() error {
	return db.conn.Close()
}

// Query выполняет SQL-запрос и возвращает результаты в виде DataFrame
func (db *Database) Query(query string, args ...interface{}) (*DataFrame, error) {
	rows, err := db.conn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	df := NewDataFrame()
	for _, col := range columns {
		df.columns[col] = []interface{}{}
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		for i, col := range columns {
			var v interface{}
			rawValue := values[i]

			switch columnTypes[i].DatabaseTypeName() {
			case "VARCHAR", "TEXT":
				v = string(rawValue.([]byte))
			case "INT", "INTEGER":
				v = int(rawValue.(int64))
			case "FLOAT", "DOUBLE":
				v = float64(rawValue.(float64))
			default:
				v = rawValue
			}

			df.columns[col] = append(df.columns[col], v)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return df, nil
}

// Insert вставляет данные из DataFrame в таблицу
func (db *Database) Insert(table string, df *DataFrame) error {
	if len(df.columns) == 0 {
		return errors.New("dataframe is empty")
	}

	columns := make([]string, 0, len(df.columns))
	placeholders := make([]string, 0, len(df.columns))
	values := make([]interface{}, 0)

	for col := range df.columns {
		columns = append(columns, col)
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, join(columns, ","), join(placeholders, ","))

	for i := 0; i < df.RowCount(); i++ {
		row := df.GetRow(i)
		for _, col := range columns {
			values = append(values, row[col])
		}
		_, err := db.conn.Exec(query, values...)
		if err != nil {
			return err
		}
		values = values[:0]
	}

	return nil
}

// join объединяет элементы среза в строку с заданным разделителем
func join(elems []string, sep string) string {
	if len(elems) == 0 {
		return ""
	}
	result := elems[0]
	for _, elem := range elems[1:] {
		result += sep + elem
	}
	return result
}
