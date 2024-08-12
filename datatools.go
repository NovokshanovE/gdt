package gdt

import (
	"errors"
)

// DataFrame представляет аналог pandas DataFrame
type DataFrame struct {
	columns map[string][]interface{}
}

// NewDataFrame создает новый DataFrame
func NewDataFrame() *DataFrame {
	return &DataFrame{
		columns: make(map[string][]interface{}),
	}
}

// AddColumn добавляет новый столбец в DataFrame
func (df *DataFrame) AddColumn(name string, data []interface{}) {
	df.columns[name] = data
}

// RemoveColumn удаляет столбец из DataFrame
func (df *DataFrame) RemoveColumn(name string) {
	delete(df.columns, name)
}

// GetColumn возвращает данные столбца
func (df *DataFrame) GetColumn(name string) ([]interface{}, error) {
	data, exists := df.columns[name]
	if !exists {
		return nil, errors.New("column does not exist")
	}
	return data, nil
}

// AddRow добавляет новую строку в DataFrame
func (df *DataFrame) AddRow(row map[string]interface{}) error {
	for colName, colData := range df.columns {
		value, exists := row[colName]
		if !exists {
			return errors.New("missing value for column: " + colName)
		}
		df.columns[colName] = append(colData, value)
	}
	return nil
}

// FilterRows фильтрует строки по заданному условию
func (df *DataFrame) FilterRows(condition func(map[string]interface{}) bool) *DataFrame {
	newDF := NewDataFrame()
	for colName := range df.columns {
		newDF.columns[colName] = []interface{}{}
	}

	for i := 0; i < df.RowCount(); i++ {
		row := df.getRow(i)
		if condition(row) {
			for colName, value := range row {
				newDF.columns[colName] = append(newDF.columns[colName], value)
			}
		}
	}

	return newDF
}

// RowCount возвращает количество строк в DataFrame
func (df *DataFrame) RowCount() int {
	for _, col := range df.columns {
		return len(col)
	}
	return 0
}

// getRow возвращает строку данных в виде мапы
func (df *DataFrame) getRow(index int) map[string]interface{} {
	row := make(map[string]interface{})
	for colName, colData := range df.columns {
		row[colName] = colData[index]
	}
	return row
}

// Join выполняет join между двумя DataFrame
func (df *DataFrame) Join(other *DataFrame, on string, how string) (*DataFrame, error) {
	newDF := NewDataFrame()

	leftCol, leftExists := df.columns[on]
	rightCol, rightExists := other.columns[on]

	if !leftExists || !rightExists {
		return nil, errors.New("join column does not exist in one of the DataFrames")
	}

	for colName := range df.columns {
		newDF.columns[colName] = []interface{}{}
	}
	for colName := range other.columns {
		if colName != on {
			newDF.columns[colName] = []interface{}{}
		}
	}

	switch how {
	case "inner":
		for i, leftVal := range leftCol {
			for j, rightVal := range rightCol {
				if leftVal == rightVal {
					for colName, colData := range df.columns {
						newDF.columns[colName] = append(newDF.columns[colName], colData[i])
					}
					for colName, colData := range other.columns {
						if colName != on {
							newDF.columns[colName] = append(newDF.columns[colName], colData[j])
						}
					}
				}
			}
		}
	case "left":
		for i, leftVal := range leftCol {
			matchFound := false
			for j, rightVal := range rightCol {
				if leftVal == rightVal {
					for colName, colData := range df.columns {
						newDF.columns[colName] = append(newDF.columns[colName], colData[i])
					}
					for colName, colData := range other.columns {
						if colName != on {
							newDF.columns[colName] = append(newDF.columns[colName], colData[j])
						}
					}
					matchFound = true
				}
			}
			if !matchFound {
				for colName, colData := range df.columns {
					newDF.columns[colName] = append(newDF.columns[colName], colData[i])
				}
				for colName := range other.columns {
					if colName != on {
						newDF.columns[colName] = append(newDF.columns[colName], nil)
					}
				}
			}
		}
	case "right":
		for j, rightVal := range rightCol {
			matchFound := false
			for i, leftVal := range leftCol {
				if leftVal == rightVal {
					for colName, colData := range df.columns {
						newDF.columns[colName] = append(newDF.columns[colName], colData[i])
					}
					for colName, colData := range other.columns {
						if colName != on {
							newDF.columns[colName] = append(newDF.columns[colName], colData[j])
						}
					}
					matchFound = true
				}
			}
			if !matchFound {
				for colName := range df.columns {
					newDF.columns[colName] = append(newDF.columns[colName], nil)
				}
				for colName, colData := range other.columns {
					if colName != on {
						newDF.columns[colName] = append(newDF.columns[colName], colData[j])
					}
				}
			}
		}
	case "outer":
		usedLeft := make([]bool, len(leftCol))
		usedRight := make([]bool, len(rightCol))

		for i, leftVal := range leftCol {
			for j, rightVal := range rightCol {
				if leftVal == rightVal {
					for colName, colData := range df.columns {
						newDF.columns[colName] = append(newDF.columns[colName], colData[i])
					}
					for colName, colData := range other.columns {
						if colName != on {
							newDF.columns[colName] = append(newDF.columns[colName], colData[j])
						}
					}
					usedLeft[i] = true
					usedRight[j] = true
				}
			}
		}

		for i, used := range usedLeft {
			if !used {
				for colName, colData := range df.columns {
					newDF.columns[colName] = append(newDF.columns[colName], colData[i])
				}
				for colName := range other.columns {
					if colName != on {
						newDF.columns[colName] = append(newDF.columns[colName], nil)
					}
				}
			}
		}

		for j, used := range usedRight {
			if !used {
				for colName := range df.columns {
					newDF.columns[colName] = append(newDF.columns[colName], nil)
				}
				for colName, colData := range other.columns {
					if colName != on {
						newDF.columns[colName] = append(newDF.columns[colName], colData[j])
					}
				}
			}
		}
	default:
		return nil, errors.New("unsupported join type")
	}

	return newDF, nil
}
