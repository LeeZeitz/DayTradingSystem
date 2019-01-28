package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"strconv"

	_ "github.com/herenow/go-crate"
	"github.com/go-redis/redis"
)

var (
	dbstring = "http://localhost:4200/"
	db       = loadDb()
	cache = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
)

func RedisClient() {
	err := cache.Set("key", "value", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := cache.Get("key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	val2, err := cache.Get("key2").Result()
	if err == redis.Nil {
		fmt.Println("key2 does not exist")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("key2", val2)
	}
	// Output: key value
	// key2 does not exist
}


func failOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s", msg, err)
		panic(err)
	}
}

func loadDb() *sql.DB {
	db, err := sql.Open("crate", dbstring)

	// If can't connect to DB
	failOnError(err, "Couldn't connect to CrateDB")
	// err := db.Ping()
	// failOnError(err, "Couldn't ping CrateDB")

	return db
}

func addHandler(w http.ResponseWriter, r *http.Request) {

	RedisClient()
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID  string
		Balance float64
	}{"", 0.0}

	// Read request json into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	
	// Insert new user if they don't already exist, otherwise update their balance
	queryString := "INSERT INTO users (user_id, balance) VALUES ($1, $2)" +
		"ON CONFLICT (user_id) DO UPDATE SET balance = balance + $2"

	stmt, err := db.Prepare(queryString)
	failOnError(err, "Failed to prepare query")

	res, err := stmt.Exec(req.UserID, req.Balance)
	failOnError(err, "Failed to add balance")

	// Check the query actually did something (because this one should always modify something, unless add 0..?)
	numrows, err := res.RowsAffected()
	if numrows < 1 {
		failOnError(err, "Failed to add balance")
	}

	//w.WriteHeader(http.StatusOK)
}


func getQuote (symbol string) float64 {
	return 50.0
}


func quoteHandler(w http.ResponseWriter, r *http.Request) {
	//decoder := json.NewDecoder(r.Body)

}


func buyHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID string
		Amount float64
		Symbol string
	}{"", 0.0, ""}

	// Read request json data into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	// Get price of requested stock
	price := getQuote(req.Symbol)
	
	// Calculate total cost to buy given amount of given stock
	cost := price * req.Amount

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
		// User has enough, do reserve the funds by pulling them from the account
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
		cache.LPush(req.UserID + ":buy", req.Symbol + ":" + strconv.FormatFloat(req.Amount, 'f', -1, 64))
	}
}


func commitBuyHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID string
	}{""}

	// Parse request parameters into struct (just user_id)
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")

	// Get most recent buy transaction 
	task := cache.LPop(req.UserID + ":buy")

	tasks := strings.Split(task.Val(), ":")

	// Check if there are any buy transactions to perform
	if len(tasks) <= 1 {
		w.Write([]byte("Failed to commit buy transaction: no buy orders exist"))
		return
	}

	// Add new stocks to user's account
	queryString := "INSERT INTO stocks (quantity, symbol, user_id) VALUES ($1, $2, $3) " +
		"ON CONFLICT (user_id, symbol) DO UPDATE SET quantity = quantity + $1;"
	stmt, err := db.Prepare(queryString)
	failOnError(err, "Failed to prepare query")

	res, err := stmt.Exec(tasks[1], tasks[0], req.UserID)
	failOnError(err, "Failed to add stocks to account")

	numrows, err := res.RowsAffected()
	if numrows < 1 {
		failOnError(err, "Failed to add stocks to account")
	}

}


func cancelBuyHandler(w http.ResponseWriter, r *http.Request) {

}

func sellHandler(w http.ResponseWriter, r *http.Request) {

}

func commitSellHandler(w http.ResponseWriter, r *http.Request) {

}

func cancelSellHandler(w http.ResponseWriter, r *http.Request) {

}

func setBuyAmountHandler(w http.ResponseWriter, r *http.Request) {

}

func cancelSetBuyHandler(w http.ResponseWriter, r *http.Request) {

}

func setBuyTriggerHandler(w http.ResponseWriter, r *http.Request) {

}

func setSellAmountHandler(w http.ResponseWriter, r *http.Request) {

}

func setSellTriggerHandler(w http.ResponseWriter, r *http.Request) {

}

func cancelSetSellHandler(w http.ResponseWriter, r *http.Request) {

}

func dumpLogHandler(w http.ResponseWriter, r *http.Request) {

}

func displaySummaryHandler(w http.ResponseWriter, r *http.Request) {

}
func main() {
	port := ":8080"
	http.HandleFunc("/add", addHandler)
	http.HandleFunc("/quote", quoteHandler)
	http.HandleFunc("/buy", buyHandler)
	http.HandleFunc("/commit_buy", commitBuyHandler)
	http.HandleFunc("/cancel_buy", cancelBuyHandler)
	http.HandleFunc("/sell", sellHandler)
	http.HandleFunc("/commit_sell", commitSellHandler)
	http.HandleFunc("/cancel_sell", cancelSellHandler)
	http.HandleFunc("/set_buy_amount", setBuyAmountHandler)
	http.HandleFunc("/cancel_set_buy", cancelSetBuyHandler)
	http.HandleFunc("/set_buy_trigger", setBuyTriggerHandler)
	http.HandleFunc("/set_sell_amount", setSellAmountHandler)
	http.HandleFunc("/set_sell_trigger", setSellTriggerHandler)
	http.HandleFunc("/cancel_set_sell", cancelSetSellHandler)
	http.HandleFunc("/dumplog", dumpLogHandler)
	http.HandleFunc("/display_summary", displaySummaryHandler)
	http.ListenAndServe(port, nil)
}
