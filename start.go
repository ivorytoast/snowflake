package main

import (
	"context"
	"encoding/json"
	"fmt"
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

	id := "aaabbb1a-c95a-11eb-b8bc-0242ac130003"

	revertCurrentIndForPreviousVersion(conn, id)

	//getMaxVersion(conn, id)

	//insertSheet(conn)

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



	//fmt.Println("Initial balances:")
	//getBalances(conn)
	//
	//err = crdbpgx.ExecuteTx(context.Background(), conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
	//	return transferFunds(context.Background(), tx, 1 /* from acct# */, 2 /* to acct# */, 100 /* amount */)
	//})
	//if err == nil {
	//	fmt.Println("\nTransaction Successful\n ")
	//} else {
	//	log.Fatal("error: ", err)
	//}
	//
	//fmt.Println("Balances after transaction:")
	//getBalances(conn)
}

func insertSheet(conn *pgx.Conn) {
	var p Payload
	p.Title = "New Title One"
	p.Scale = "New Scale One"

	var s Sheet
	s.id = "aaabbb1a-c95a-11eb-b8bc-0242ac130003"
	s.versionNum = 3
	s.creationTime = time.Now()
	s.isCurrentId = true
	s.tags = [3]string{"two", "eight", "four"}
	valJson := "{\"title\":\"suppy\",\"scale\":\"luppy\"}"
	if _, err := conn.Exec(context.Background(),
		"INSERT INTO sheets (id, version_num, creation_time, is_current_ind, payload, tags) " +
		"VALUES ($1, $2, $3, $4, $5, $6)", s.id, s.versionNum, s.creationTime, s.isCurrentId, valJson, s.tags); err != nil {
		log.Fatal(err)
	}
}

func getMaxVersion(conn *pgx.Conn, id string) int {
	var output int

	err := conn.QueryRow(context.Background(), "SELECT version_num FROM sheets WHERE (id, version_num) IN" +
		" (SELECT id, MAX(version_num) FROM sheets GROUP BY id HAVING id = $1)", id).Scan(&output)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%d", output)

	return output
}

func revertCurrentIndForPreviousVersion(conn *pgx.Conn, id string) {
	latestVersion := getMaxVersion(conn, id)

	latestVersion = latestVersion - 1

	if _, err := conn.Exec(context.Background(),
		"UPDATE sheets SET is_current_ind = false WHERE id = $1 and version_num = $2", id, latestVersion); err != nil {
		log.Fatal(err)
	}
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

func getBalances(conn *pgx.Conn) {
	rows, err := conn.Query(context.Background(), "SELECT id, balance FROM accounts")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, balance int
		if err := rows.Scan(&id, &balance); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%d %d\n", id, balance)
	}
}

func transferFunds(ctx context.Context, tx pgx.Tx, from int, to int, amount int) error {
	var fromBalance int
	if err := tx.QueryRow(ctx,
		"SELECT balance FROM accounts WHERE id = $1", from).Scan(&fromBalance); err != nil {
		return err
	}

	if fromBalance < amount {
		return fmt.Errorf("insufficient funds")
	}

	if _, err := tx.Exec(ctx,
		"UPDATE accounts SET balance = balance - $1 WHERE id = $2", amount, from); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx,
		"UPDATE accounts SET balance = balance + $1 WHERE id = $2", amount, to); err != nil {
		return err
	}
	return nil
}
