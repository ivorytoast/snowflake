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

	sheets := getAllSheets(conn)

	for _, sheet := range sheets {
		fmt.Println(sheet.id)
		fmt.Println(sheet.versionNum)
		fmt.Println(sheet.creationTime)
		fmt.Println(sheet.tags)
		fmt.Println(sheet.payload.Scale)
		fmt.Println(sheet.payload.Title)
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
