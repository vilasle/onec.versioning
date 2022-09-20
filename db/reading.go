package db

import (
	"database/sql"
	"fmt"
)

type Result []map[string]string

func ReadRows(rows *sql.Rows) (Result, error) {

	cols, err := rows.Columns()

	if err != nil {
		return nil, err
	}

	rs := make([]map[string]string, 0)

	forScn := createSliceForScanning(cols)

	for rows.Next() {

		if err := rows.Scan(forScn[:]...); err != nil {
			return nil, err
		}

		row := make(map[string]string)

		for i := range forScn {
			val := *forScn[i].(*interface{})
			row[cols[i]] = getColumnValueAsString(val)
		}

		rs = append(rs, row)
	}

	return rs, nil
}

func ReadRow(rows *sql.Rows) (map[string]string, error) {
	cols, err := rows.Columns()

	if err != nil {
		return nil, err
	}

	rs := make(map[string]string)

	forScn := createSliceForScanning(cols)

	for rows.Next() {

		if err := rows.Scan(forScn[:]...); err != nil {
			return nil, err
		}

		row := make(map[string]string)

		for i := range forScn {
			val := *forScn[i].(*interface{})
			row[cols[i]] = getColumnValueAsString(val)
		}

		rs = row
		break
	}

	return rs, nil
}

func getColumnValueAsString(val interface{}) (res string) {

	if val == nil {
		return "NULL"
	}
	switch v := val.(type) {
	case []byte:
		res = string(v)
	default:
		res = fmt.Sprintf("%v", v)
	}

	return res
}

func createSliceForScanning(cols []string) []interface{} {
	results := make([]interface{}, len(cols))
	for i := range results {
		results[i] = new(interface{})
	}
	return results
}

func (r Result) Empty() bool {
	return len(r) == 0
}

func (r Result) First() (map[string]string, error) {
	if r.Empty() {
		return nil, fmt.Errorf("result is empty") 		
	}
	return r[0], nil
}