package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// Tested
func sellHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID         string
		Amount         float64 // Dollar value to sell
		Symbol         string
		TransactionNum int
	}{"", 0.0, "", 0}

	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")
	logUserCommand(req.TransactionNum, "transaction-server", "SELL", req.UserID, req.Symbol, "", req.Amount)

	price := getQuote(req.Symbol, req.TransactionNum, req.UserID)

	// Calculate the number of the stock to sell
	sellNumber := int(req.Amount / price)
	salePrice := price * float64(sellNumber)

	// TODO: Should handle an attempted sale of unowned stocks
	// Check that the user has enough stocks to sell
	queryString := "SELECT quantity FROM stocks WHERE user_id = $1 and symbol = $2"

	stmt, err := db.Prepare(queryString)
	failOnError(err, "Failed to prepare query")

	// Number of given stock owned by user
	var balance int
	err = stmt.QueryRow(req.UserID, req.Symbol).Scan(&balance)
	defer stmt.Close()

	if err != nil {
		//fmt.Println("Failed to retrieve number of given stock owned by user")
		w.Write([]byte("Failed to retrieve number of given stock owned by user"))
		return
	}

	// Check if the user has enough
	if balance >= sellNumber {
		queryString = "UPDATE stocks SET quantity = quantity - $1 where user_id = $2 and symbol = $3;"
		stmt, err = db.Prepare(queryString)
		failOnError(err, "Failed to prepare query")

		// Withdraw the stocks to sell from user's account
		res, err := stmt.Exec(sellNumber, req.UserID, req.Symbol)
		failOnError(err, "Failed to reserve stocks to sell")

		numrows, err := res.RowsAffected()
		if numrows < 1 {
			failOnError(err, "Failed to reserve stocks to sell")
		}
		fmt.Println(salePrice)
		cache.LPush(req.UserID+":sell", req.Symbol+":"+strconv.FormatFloat(salePrice, 'f', -1, 64))
	}
	w.WriteHeader(http.StatusOK)
}

// Tested
func commitSellHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID         string
		TransactionNum int
	}{"", 0}

	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")

	logUserCommand(req.TransactionNum, "transaction-server", "COMMIT_SELL", req.UserID, "", "", 0.0)

	task := cache.LPop(req.UserID + ":sell")
	tasks := strings.Split(task.Val(), ":")

	if len(tasks) <= 1 {
		w.Write([]byte("Failed to commit sell transaction: no sell orders exist"))
		return
	}

	queryString := "UPDATE users SET balance = balance + $1 WHERE user_id = $2;"
	stmt, err := db.Prepare(queryString)
	failOnError(err, "Failed to prepare query")
	res, err := stmt.Exec(tasks[1], req.UserID)
	failOnError(err, "Failed to refund money for stock sale")

	numrows, err := res.RowsAffected()
	if numrows < 1 {
		failGracefully(err, "Failed to refund money for stock sale")
		return
	}
	w.WriteHeader(http.StatusOK)

}

func sellStock(UserID string, Symbol string, quantity string, transactionNum int) {
	f, err := strconv.ParseFloat(quantity, 64)
	failOnError(err, "Failed to parse float")
	logAccountTransaction(transactionNum, "transaction-server", "SELL", UserID, f)

	queryString := "UPDATE stocks SET quantity = quantity - $1 where user_id = $2 and symbol = $3;"
	stmt, err := db.Prepare(queryString)
	failOnError(err, "Failed to prepare query")

	// Withdraw the stocks to sell from user's account
	res, err := stmt.Exec(quantity, UserID, Symbol)
	if err != nil {
		fmt.Println("Failed to reserve stocks to sell")
		return
	}
	numrows, err := res.RowsAffected()
	if numrows < 1 {
		failGracefully(err, "Failed to reserve stocks to sell")
	}
}

// Tested
func cancelSellHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID         string
		TransactionNum int
	}{"", 0}

	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")

	logUserCommand(req.TransactionNum, "transaction-server", "CANCEL_SELL", req.UserID, "", "", 0.0)

	cache.LPop(req.UserID + ":sell")
	w.WriteHeader(http.StatusOK)
}
