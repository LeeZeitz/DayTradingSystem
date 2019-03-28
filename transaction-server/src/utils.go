package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"strconv"

	"net"

	"github.com/go-redis/redis"
	_ "github.com/herenow/go-crate"
)

// Connects to the database
func loadDb() *sql.DB {
	db, err := sql.Open("crate", dbstring)
	// If can't connect to DB
	failOnError(err, "Couldn't connect to CrateDB")
	return db
}

func runningInDocker() bool {
	if _, err := os.Stat("/.dockerenv"); !os.IsNotExist(err) {
		return true
	}
	return false
}

// Checks and panics on error
// Parameters:
// 		err: 	the error to check
// 		msg: 	a message to print to the console if an error is found
//
func failOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s", msg, err)

	}
}

func failGracefully(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s", msg, err)
	}
}
func SocketClient(symbol string, userID string) string {
	addr := "quoteserve.seng.uvic.ca:4452"
	conn, err := net.Dial("tcp", addr)

	defer conn.Close()

	failOnError(err, "Failed to connect to quote server")
	payload := fmt.Sprintf("%s,%s\r", symbol, userID)
	conn.Write([]byte(payload))

	buff := make([]byte, 2048)
	n, err := conn.Read(buff)
	if err != nil {
		fmt.Println("Error reading stock quote from quote server")
	}

	quote := string(buff[:n])
	return quote
}

// Returns a fresh quote for a given stock symbol.
// If there is a fresh quote cached, then that value is returned. Otherwise, it fetches and stores one.
// Parameters:
//		symbol: 	(string) symbol of the stock to quote
//
func getQuote(symbol string, transactionNum int, userID string) float64 {
	// Check if symbol is in cache
	quote, err := cache.Get(symbol).Result()

	if err == redis.Nil {

		if os.Getenv("DEBUG") == "TRUE" {

			//Get quote from the quote server and store it with ttl 60s
			r, err := http.Get("http://localhost:3000/quote")
			failOnError(err, "Failed to retrieve quote from quote server")
			defer r.Body.Close()

			failOnError(err, "Failed to parse quote server response")
			decoder := json.NewDecoder(r.Body)

			res := struct {
				CryptoKey string
				Quote     float64
			}{"", 0.0}

			err = decoder.Decode(&res)
			failOnError(err, "Failed to parse quote server response data")

			quoteServerTime := time.Now().UTC().Unix()
			logQuoteServer(transactionNum, "transaction-server", userID, symbol, res.CryptoKey, quoteServerTime, res.Quote)

			cache.Set(symbol, strconv.FormatFloat(res.Quote, 'f', -1, 64), 60000000000)
			return res.Quote
		} else {
			r := SocketClient(symbol, userID)
			var err error

			res := struct {
				CryptoKey       string
				Quote           float64
				QuoteServerTime int64
			}{"", 0.0, 0}
			spl := strings.Split(r, ",")
			res.QuoteServerTime, err = strconv.ParseInt(spl[3], 10, 64)
			res.CryptoKey = spl[4]
			res.Quote, err = strconv.ParseFloat(spl[0], 64)
			failOnError(err, "failed to get stuff from quote")

			logQuoteServer(transactionNum, "transaction-server", userID, symbol, res.CryptoKey, res.QuoteServerTime, res.Quote)
			cache.Set(symbol, strconv.FormatFloat(res.Quote, 'f', -1, 64), 60000000000)
			return res.Quote
		}
	} else {

		// Otherwise, return the cached value
		quote, err := strconv.ParseFloat(quote, 32)
		failOnError(err, "Failed to parse float from quote")
		return quote
	}
}

// Reserves the given amount of money from the given user
// Parameters:
// 		UserID: 	the userID for the user to reserve funds from
// 		amount:		the amount of money to reserve
//
func ReserveFunds(UserID string, amount float64) {

}

func ReleaseFunds(UserID string, amount float64) {

}
