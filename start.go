package main

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgx"
	"log"

	"github.com/jackc/pgx/v4"

	"snow/db"
)

func main() {
	db.InitializeDatabaseProperties()

	connectionString := fmt.Sprintf("postgres://%s:%s@%s:26257/bank?sslmode=verify-full&sslrootcert=%s&options=--cluster=%s",
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

	fmt.Println("Initial balances:")
	getBalances(conn)

	err = crdbpgx.ExecuteTx(context.Background(), conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		return transferFunds(context.Background(), tx, 1 /* from acct# */, 2 /* to acct# */, 100 /* amount */)
	})
	if err == nil {
		fmt.Println("\nTransaction Successful\n ")
	} else {
		log.Fatal("error: ", err)
	}

	fmt.Println("Balances after transaction:")
	getBalances(conn)
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
