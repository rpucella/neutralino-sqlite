package main

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rpucella/go-neutralino-extensions"
	"log"
	"os"
)

// Mostly adapted from https://github.com/gorilla/websocket/blob/main/examples/echo/client.go

func main() {
	log.Println("starting Neutralinojs SQLite extension")

	if len(os.Args) < 2 {
		log.Fatal("extension requires database file argument")
	}

	dbFile := os.Args[1]
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		log.Fatal(fmt.Errorf("cannot open db file: %w", err))
	}
	defer db.Close()

	connInfo, err := neutralinoext.ReadConnInfo(os.Stdin)
	if err != nil {
		log.Fatal(fmt.Errorf("cannot read connection info: %w", err))
	}

	processMsg := mkProcessMsg(db)

	if err := connInfo.StartMessageLoop(processMsg); err != nil {
		panic(err)
	}
}

func mkProcessMsg(db *sql.DB) neutralinoext.ProcessFn {
	processMsg := func(event string, data any) (map[string]any, error) {
		switch event {
		case "query":
			return processQuery(db, data)
		case "exec":
			return processExec(db, data)
		default:
			return nil, nil
		}
	}
	return processMsg
}

func processQuery(db *sql.DB, data any) (map[string]any, error) {
	dataObj, ok := data.(map[string]any)
	if !ok {
		return nil, errors.New("data not an object")
	}
	query, err := getString(dataObj, "sql")
	if err != nil {
		return nil, err
	}
	params, err := getList(dataObj, "params")
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(query, params...)
	if err != nil {
		return nil, fmt.Errorf("cannot query: %w", err)
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("cannot get columns: %w", err)
	}
	// Accumulate the resulting rows.
	resultRows := make([][]any, 0)
	for rows.Next() {
		row := make([]any, len(cols))
		rowAddrs := make([]any, len(cols))
		for i := range row {
			rowAddrs[i] = &row[i]
		}
		err = rows.Scan(rowAddrs...)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}
		resultRows = append(resultRows, row)
	}
	result := make(map[string]any)
	result["rows"] = resultRows
	return result, nil
}

func processExec(db *sql.DB, data any) (map[string]any, error) {
	dataObj, ok := data.(map[string]any)
	if !ok {
		return nil, errors.New("data not an object")
	}
	query, err := getString(dataObj, "sql")
	if err != nil {
		return nil, err
	}
	params, err := getList(dataObj, "params")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(query, params...)
	if err != nil {
		return nil, fmt.Errorf("cannot exec: %w", err)
	}
	// Return an empty JSON object.
	result := make(map[string]any)
	result["done"] = true
	return result, nil
}

func getString(m map[string]any, key string) (string, error) {
	ifc, ok := m[key]
	if !ok {
		return "", nil
	}
	s, ok := ifc.(string)
	if !ok {
		return "", fmt.Errorf("not a string %v", ifc)
	}
	return s, nil
}

func getList(m map[string]any, key string) ([]any, error) {
	ifc, ok := m[key]
	if !ok {
		return nil, nil
	}
	lst, ok := ifc.([]any)
	if !ok {
		return nil, fmt.Errorf("not a list %v", ifc)
	}
	return lst, nil
}
