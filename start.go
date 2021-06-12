package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgx"
	"log"
	"time"

	"github.com/jackc/pgx/v4"

	"snow/db"
)

type Sheet struct {
	id string
	versionNum int
	creationTime time.Time
	payload Payload
	isCurrentId bool
	tags [3]string
}

type Payload struct {
	Title string `json:"title"`
	Scale string `json:"scale"`
}

func main() {
	db.InitializeDatabaseProperties()

	connectionString := fmt.Sprintf("postgres://%s:%s@%s:26257/documents?sslmode=verify-full&sslrootcert=%s&options=--cluster=%s",
		db.UserName, db.Password, db.Host, db.PathToCert, db.ClusterName)

	fmt.Printf("\n \n Database Connection URL: %s \n \n", connectionString)

	config, err := pgx.ParseConfig(connectionString)
	if err != nil {
		log.Fatal("error configuring the database: ", err)
	}

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatal("error connecting to the database: ", err)
	}
	defer conn.Close(context.Background())

	id := "555bbb1a-c95a-11eb-b8bc-0242ac130999"

	err = crdbpgx.ExecuteTx(context.Background(), conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		return insertSheet(context.Background(), tx, id)
	})
	if err == nil {
		fmt.Println("\nTransaction Successful\n ")
	} else {
		fmt.Printf("Transaction Not Successful. Error: %s \n", err)
	}

	//getMaxVersion(conn, "555bbb1a-c95a-11eb-b8bc-0242ac130999")

	logSheetsAllSheetsUnderId(conn, id)
}

func logSheetsAllSheetsUnderId(conn *pgx.Conn, id string) {
	sheets := getSheetsById(conn, id)

	for _, sheet := range sheets {
		fmt.Println(sheet.id)
		fmt.Println(sheet.versionNum)
		fmt.Println(sheet.creationTime)
		fmt.Println(sheet.tags)
		fmt.Println(sheet.payload.Scale)
		fmt.Println(sheet.payload.Title)
		fmt.Println(sheet.isCurrentId)
		fmt.Println("")
		fmt.Println("")
	}
}

func insertSheet(ctx context.Context, tx pgx.Tx, id string) error {

	var currentVersion int

	queryErr := tx.QueryRow(ctx,
		"SELECT version_num FROM sheets WHERE (id, version_num) IN (SELECT id, MAX(version_num) FROM sheets GROUP BY id HAVING id = $1)", id).Scan(&currentVersion)

	if queryErr != nil {
		if queryErr.Error() == "no rows in result set" {
			fmt.Println("No rows returned")
			currentVersion = 0
		} else {
			return queryErr
		}
	}

	fmt.Printf("Current Version: %d \n", currentVersion)

	var s Sheet
	s.id = id
	s.versionNum = currentVersion + 1
	s.creationTime = time.Now()
	s.isCurrentId = true
	s.tags = [3]string{"three", "one", "eleven"}
	valJson := "{\"title\":\"testTitle3\",\"scale\":\"testScale3\"}"

	if _, insertErr := tx.Exec(ctx,
		"INSERT INTO sheets (id, version_num, creation_time, is_current_ind, payload, tags) VALUES ($1, $2, $3, $4, $5, $6)",
		s.id, s.versionNum, s.creationTime, s.isCurrentId, valJson, s.tags);
	insertErr != nil {
		return insertErr
	}

	if currentVersion > 1 {
		if _, updateErr := tx.Exec(ctx, "UPDATE sheets SET is_current_ind = false WHERE id = $1 and version_num = $2", id, currentVersion);
		updateErr != nil {
			return updateErr
		}
	} else {
		fmt.Println("No need to revert previous version since it is the first version")
	}

	return nil
}

func getMaxVersion(conn *pgx.Conn, id string) int {
	var output int

	row := conn.QueryRow(context.Background(), "SELECT version_num FROM sheets WHERE (id, version_num) IN" +
		" (SELECT id, MAX(version_num) FROM sheets GROUP BY id HAVING id = $1)", id)
	err := row.Scan(&output)

	if row == nil {
		fmt.Println("No rows returned")
	}

	if err != nil {
		if err.Error() == "no rows in result set" {
			fmt.Println("No rows returned")
			return 0
		} else {
			log.Fatal("Unexpected error getting max version: " + err.Error())
		}
	}

	fmt.Printf("%d", output)

	return output
}

func getLatestVersions(conn *pgx.Conn, id string, versionNum int) []Sheet {
	output := make([]Sheet, 0)

	rows, err := conn.Query(context.Background(), "SELECT * FROM sheets WHERE id = $1 AND version_num = $2", id, versionNum)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var s Sheet
		if err := rows.Scan(&s.id, &s.versionNum, &s.creationTime, &s.payload, &s.isCurrentId, &s.tags); err != nil {
			log.Fatal(err)
		}

		output = append(output, s)
		fmt.Println("payload: " + s.payload.Title)
		payload := Payload{}
		payloadBytes, bytesError := json.Marshal(s.payload);
		if bytesError != nil {
			log.Println(err)
		}

		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			log.Println(err)
		}
		fmt.Printf("%s | %s", payload.Title, payload.Scale)
		fmt.Println("")
		fmt.Println("")
	}
	return output
}

func getAllSheets(conn *pgx.Conn) []Sheet {
	output := make([]Sheet, 0)

	rows, err := conn.Query(context.Background(), "SELECT * FROM sheets")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var s Sheet
		if err := rows.Scan(&s.id, &s.versionNum, &s.creationTime, &s.payload, &s.isCurrentId, &s.tags); err != nil {
			log.Fatal(err)
		}

		output = append(output, s)
		fmt.Println("payload: " + s.payload.Title)
		payload := Payload{}
		payloadBytes, bytesError := json.Marshal(s.payload);
		if bytesError != nil {
			log.Println(err)
		}

		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			log.Println(err)
		}
		fmt.Printf("%s | %s", payload.Title, payload.Scale)
		fmt.Println("")
		fmt.Println("")
	}
	return output
}

func getSheetsById(conn *pgx.Conn, id string) []Sheet {
	output := make([]Sheet, 0)

	rows, err := conn.Query(context.Background(), "SELECT * FROM sheets WHERE id = $1", id)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var s Sheet
		if err := rows.Scan(&s.id, &s.versionNum, &s.creationTime, &s.payload, &s.isCurrentId, &s.tags); err != nil {
			log.Fatal(err)
		}

		output = append(output, s)
		fmt.Println("payload: " + s.payload.Title)
		payload := Payload{}
		payloadBytes, bytesError := json.Marshal(s.payload);
		if bytesError != nil {
			log.Println(err)
		}

		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			log.Println(err)
		}
		fmt.Printf("%s | %s", payload.Title, payload.Scale)
		fmt.Println("")
		fmt.Println("")
	}
	return output
}
