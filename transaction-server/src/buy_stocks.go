package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// Tested
func buyHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID         string
		Amount         float64 // dolar amount of a stock to buy
		Symbol         string
		TransactionNum int
	}{"", 0.0, "", 0}

	// Read request json data into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	logUserCommand(req.TransactionNum, "transaction-server", "BUY", req.UserID, req.Symbol, "", req.Amount)

	if req.Amount < 0 {
		panic("Can't purchase a negative amount")
	}

	// Get price of requested stock
	price := getQuote(req.Symbol, req.TransactionNum, req.UserID)
	// Calculate total cost to buy given amount of given stock
	buyNumber := int(req.Amount / price)
	cost := float64(buyNumber) * price

	// Query to get the current balance of the user
	queryString := "SELECT balance FROM users WHERE user_id = $1;"
	// Try to prepare query
	stmt, err := db.Prepare(queryString)
	failOnError(err, "Failed to prepare query")

	var balance float64

	// Try to perform the query and get the user's balance
	// TODO: this should probably handle a user buy when the user doesn't exist, but it doesn't right now.
	err = stmt.QueryRow(req.UserID).Scan(&balance)
	failOnError(err, "Failed to get user balance")
	defer stmt.Close()

	// Check user balance against cost of requested stock purchase
	if balance >= cost {
		// User has enough, reserve the funds by pulling them from the account
		queryString = "UPDATE users SET balance = balance - $1 WHERE user_id = $2"
		stmt, err := db.Prepare(queryString)
		failOnError(err, "Failed to prepare withdraw query")

		// Withdraw funds from user's account
		res, err := stmt.Exec(cost, req.UserID)
		failOnError(err, "Failed to withdraw money from user account")

		numrows, err := res.RowsAffected()
		if numrows < 1 {
			failOnError(err, "Failed to reserve funds")
		}
		// Add buy transaction to front of user's transaction list
		cache.LPush(req.UserID+":buy", req.Symbol+":"+strconv.Itoa(buyNumber))
	}
	w.WriteHeader(http.StatusOK)
}

// Tested
func commitBuyHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID         string
		TransactionNum int
	}{"", 0}

	// Parse request parameters into struct (just user_id)
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")

	logUserCommand(req.TransactionNum, "transaction-server", "COMMIT_BUY", req.UserID, "", "", 0.0)

	// Get most recent buy transaction
	task := cache.LPop(req.UserID + ":buy")
	tasks := strings.Split(task.Val(), ":")

	// Check if there are any buy transactions to perform
	if len(tasks) <= 1 {
		w.Write([]byte("Failed to commit buy transaction: no buy orders exist"))
		return
	}

	// Add new stocks to user's account
	buyStock(req.UserID, tasks[0], tasks[1], req.TransactionNum)
	w.WriteHeader(http.StatusOK)
}

func buyStock(UserID string, Symbol string, quantity string, transactionNum int) {
	// Add new stocks to user's account
	queryString := "INSERT INTO stocks (quantity, symbol, user_id) VALUES ($1, $2, $3) " +
		"ON CONFLICT (user_id, symbol) DO UPDATE SET quantity = stocks.quantity + $1;"
	stmt, err := db.Prepare(queryString)
	failOnError(err, "Failed to prepare query")
	res, err := stmt.Exec(quantity, Symbol, UserID)
	failGracefully(err, "Failed to add stocks to account")

	numrows, err := res.RowsAffected()
	if numrows < 1 {
		failGracefully(err, "Failed to add stocks to account")
	}

	f, err := strconv.ParseFloat(quantity, 64)
	failOnError(err, "Failed to parse float")
	logAccountTransaction(transactionNum, "transaction-server", "BUY", UserID, f)
}

// Tested
func cancelBuyHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID         string
		TransactionNum int
	}{"", 0}

	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")

	cache.LPop(req.UserID + ":buy")

	logUserCommand(req.TransactionNum, "transaction-server", "CANCEL_BUY", req.UserID, "", "", 0.0)
	w.WriteHeader(http.StatusOK)
}
