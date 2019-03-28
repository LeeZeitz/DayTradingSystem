package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	_ "github.com/herenow/go-crate"
	"github.com/streadway/amqp"
)

var (
	dbstring = func() string {
		if runningInDocker() {
			return "http://transaction-db:4200"
		}
		return "http://localhost:4200"
	}()

	db = loadDb()

	auditServer = func() string {
		if runningInDocker() {
			return "http://audit:8081"
		}
		return "http://localhost:8081"
	}()

	redishost = func() string {
		if runningInDocker() {
			return "redis:6379"
		}
		return "http://localhost:6379"
	}()

	cache = redis.NewClient(&redis.Options{
		Addr:     redishost,
		Password: "",
		DB:       0,
	})
	auditmq   *amqp.Channel
	ucQueue   amqp.Queue
	qseQueue  amqp.Queue
	atQueue   amqp.Queue
	seQueue   amqp.Queue
	auditChan = make(chan interface{})
)

func logSystemEvent(transactionNum int, server string, command string, username string, stock string, filename string, funds float64) {
	req := SystemEvent{TransactionNum: transactionNum, Server: server, Command: command, Username: username, Stock: stock, Filename: filename, Funds: funds}
	auditChan <- req
}

func logUserCommand(transactionNum int, server string, command string, username string, stock string, filename string, funds float64) {
	req := UserCommand{TransactionNum: transactionNum, Server: server, Command: command, Username: username, Stock: stock, Filename: filename, Funds: funds}
	auditChan <- req
}

func logAccountTransaction(transactionNum int, server string, action string, username string, funds float64) {
	req := AccountTransaction{TransactionNum: transactionNum, Server: server, Action: action, Username: username, Funds: funds}
	auditChan <- req
}

func logQuoteServer(transactionNum int, server string, username string, stock string, cryptoKey string, quoteServerTime int64, price float64) {
	req := QuoteServerEvent{TransactionNum: transactionNum, Server: server, Username: username, Stock: stock, CryptoKey: cryptoKey, QuoteServerTime: quoteServerTime, Price: price}
	auditChan <- req
}

// Tested
func addHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID         string
		Amount         float64
		TransactionNum int
	}{"", 0.0, 0}

	// Read request json into struct
	err := decoder.Decode(&req)
	if err != nil {

		failGracefully(err, "Failed to get add request")
	}

	logUserCommand(req.TransactionNum, "transaction-server", "ADD", req.UserID, "", "", req.Amount)

	if req.Amount < 0 {
		fmt.Println("Can't add a negative balance")
		return
	}

	logAccountTransaction(req.TransactionNum, "transaction-server", "add", req.UserID, req.Amount)

	// Insert new user if they don't already exist, otherwise update their balance
	queryString := "INSERT INTO users (user_id, balance) VALUES ($1, $2)" +
		"ON CONFLICT (user_id) DO UPDATE SET balance = balance + $2"

	stmt, err := db.Prepare(queryString)
	if err != nil {

		failGracefully(err, "Failed to prep query")
	}

	res, err := stmt.Exec(req.UserID, req.Amount)
	if err != nil {

		failGracefully(err, "Failed to do something with query")
	}

	// Check the query actually did something (because this one should always modify something, unless add 0..?)
	numrows, err := res.RowsAffected()
	if numrows < 1 {
		failOnError(err, "Failed to add balance")
	}
	w.WriteHeader(http.StatusOK)
}

// Tested
func quoteHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	// Parse request into struct
	req := struct {
		UserID         string
		Symbol         string
		TransactionNum int
	}{"", "", 0}

	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")
	logUserCommand(req.TransactionNum, "transaction-server", "QUOTE", req.UserID, req.Symbol, "", 0.0)

	// Get quote for the requested stock symbol
	quote := getQuote(req.Symbol, req.TransactionNum, req.UserID)

	// Return UserID, Symbol, and stock quote in comma-delimited string
	w.Write([]byte(req.UserID + "," + req.Symbol + "," + strconv.FormatFloat(quote, 'f', -1, 64)))
}

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
		"ON CONFLICT (user_id, symbol) DO UPDATE SET quantity = quantity + $1;"
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
	logAccountTransaction(transactionNum, "transaction-server", "BUY", UserID, f) // todo add transnum
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

// Tested
func setBuyAmountHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID         string  // id of the user buying
		Symbol         string  // symbol of the stock to buy
		Amount         float64 // dollar amount of stock to buy
		TransactionNum int
	}{"", "", 0.0, 0}

	// Parse request into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")

	logUserCommand(req.TransactionNum, "transaction-server", "SET_BUY_AMOUNT", req.UserID, req.Symbol, "", req.Amount)

	// Add buy amount to user's account. If a buy amount already exists for the requested stock, add this to it
	queryString := "INSERT INTO buy_amounts (user_id, symbol, quantity) VALUES ($1, $2, $3) " +
		"ON CONFLICT (user_id, symbol) DO UPDATE SET quantity = quantity + $3;"

	stmt, err := db.Prepare(queryString)
	failOnError(err, "Failed to prepare query")
	res, err := stmt.Exec(req.UserID, req.Symbol, req.Amount)

	if err != nil {
		failGracefully(err, "Failed to update buy amount")
		return
	}

	numrows, err := res.RowsAffected()
	if numrows < 1 {
		failGracefully(err, "Failed to update buy amount")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Tested
func cancelSetBuyHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID         string
		Symbol         string
		TransactionNum int
	}{"", "", 0}

	// Parse request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")

	logUserCommand(req.TransactionNum, "transaction-server", "CANCEL_SET_BUY", req.UserID, req.Symbol, "", 0.0)

	queryString1 := "DELETE FROM buy_amounts WHERE user_id = $1 AND symbol = $2;"
	queryString2 := "DELETE FROM triggers WHERE user_id = $1 AND symbol = $2 AND method = 'buy';"

	rows1, err := db.Query(queryString1, req.UserID, req.Symbol)
	defer rows1.Close()

	if err != nil {
		fmt.Println("Failed to delete buy amount")
		w.Write([]byte("Failed to delete buy amount"))
		return
	}

	rows2, err := db.Query(queryString2, req.UserID, req.Symbol)
	defer rows2.Close()
	if err != nil {
		w.Write([]byte("Failed to delete trigger"))
		return
	}
}

// TODO: Every 60 seconds, see if price is cached. If it is, check it against triggers. If it's not and there's a trigger
// that exists, get quote for that stock and evaluate trigger.
func setBuyTriggerHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID         string
		Symbol         string
		Price          float64
		TransactionNum int
	}{"", "", 0.0, 0}

	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")

	logUserCommand(req.TransactionNum, "transaction-server", "SET_BUY_TRIGGER", req.UserID, req.Symbol, "", req.Price)

	queryString := "INSERT INTO triggers (user_id, symbol, price, method, transaction_num) VALUES ($1, $2, $3, 'buy', $4) " +
		"ON CONFLICT (user_id, symbol, method) DO UPDATE SET price = $3;"

	stmt, err := db.Prepare(queryString)
	failOnError(err, "Failed to prepare query statement")

	res, err := stmt.Exec(req.UserID, req.Symbol, req.Price, req.TransactionNum)
	if err != nil {
		failGracefully(err, "Failed to add trigger")
		w.Write([]byte("Failed to add trigger"))
		return
	}

	numrows, err := res.RowsAffected()
	if numrows < 1 {
		failGracefully(err, "Failed to add trigger")
		w.Write([]byte("Failed to add trigger"))
		return
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go monitorTrigger(req.UserID, req.Symbol, "buy")
	w.WriteHeader(http.StatusOK)
}

// Tested
func setSellAmountHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID         string
		Symbol         string
		Amount         float64 // dollar amount of stock to sell
		TransactionNum int
	}{"", "", 0.0, 0}

	// Parse request into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")

	logUserCommand(req.TransactionNum, "transaction-server", "SET_SELL_AMOUNT", req.UserID, req.Symbol, "", req.Amount)

	// Add buy amount to user's account. If a buy amount already exists for the requested stock, add this to it
	queryString := "INSERT INTO sell_amounts (user_id, symbol, quantity) VALUES ($1, $2, $3) " +
		"ON CONFLICT (user_id, symbol) DO UPDATE SET quantity = quantity + $3;"

	stmt, err := db.Prepare(queryString)
	failOnError(err, "Failed to prepare query")

	res, err := stmt.Exec(req.UserID, req.Symbol, req.Amount)
	if err != nil {
		failGracefully(err, "Failed to update sell amount")
		w.Write([]byte("Failed to add trigger"))
	}

	numrows, err := res.RowsAffected()
	if numrows < 1 {
		failGracefully(err, "Failed to update sell amount")
		w.Write([]byte("Failed to update sell amount"))
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Tested
func setSellTriggerHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID         string
		Symbol         string
		Price          float64
		TransactionNum int
	}{"", "", 0.0, 0}

	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")

	logUserCommand(req.TransactionNum, "transaction-server", "SET_SELL_TRIGGER", req.UserID, req.Symbol, "", req.Price)

	queryString := "INSERT INTO triggers (user_id, symbol, price, method, transaction_num) VALUES ($1, $2, $3, 'sell', $4) " +
		"ON CONFLICT (user_id, symbol, method) DO UPDATE SET price = $3;"

	stmt, err := db.Prepare(queryString)
	failOnError(err, "Failed to prepare query statement")

	res, err := stmt.Exec(req.UserID, req.Symbol, req.Price, req.TransactionNum)

	if err != nil {
		failGracefully(err, "Failed to add sell trigger")
		w.Write([]byte("Failed to add trigger"))
		return
	}

	numrows, err := res.RowsAffected()
	if numrows < 1 {
		failGracefully(err, "Failed to add sell trigger")
		w.Write([]byte("Failed to add trigger"))
		return
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go monitorTrigger(req.UserID, req.Symbol, "sell")
	w.WriteHeader(http.StatusOK)
}

// Tested
func cancelSetSellHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		UserID         string
		Symbol         string
		TransactionNum int
	}{"", "", 0}

	// Parse request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")
	logUserCommand(req.TransactionNum, "transaction-server", "CANCEL_SET_SELL", req.UserID, req.Symbol, "", 0.0)

	queryString1 := "DELETE FROM sell_amounts WHERE user_id = $1 AND symbol = $2;"
	queryString2 := "DELETE FROM triggers WHERE user_id = $1 AND symbol = $2 AND method = 'sell';"

	rows1, err := db.Query(queryString1, req.UserID, req.Symbol)

	if err != nil {
		failGracefully(err, "Failed to delete sell amount")
		w.Write([]byte("Failed to delete sell amount"))
		return
	}
	defer rows1.Close()

	rows2, err := db.Query(queryString2, req.UserID, req.Symbol)
	if err != nil {
		failGracefully(err, "Failed to delete sell trigger")
		w.Write([]byte("Failed to delete sell trigger"))
		return
	}
	defer rows2.Close()
	w.WriteHeader(http.StatusOK)
}

func dumpLogHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		TransactionNum int
		Filename       string
		UserID         string
	}{0, "", ""}

	// Parse request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")

	if req.UserID == "" {
		logUserCommand(req.TransactionNum, "transaction-server", "DUMPLOG", "", "", req.Filename, 0.0)
	} else {
		logUserCommand(req.TransactionNum, "transaction-server", "DUMPLOG", req.UserID, "", req.Filename, 0.0)
	}

	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)

	if req.UserID == "" {
		res, err := http.Post(auditServer+"/dumpLog", "application/json; charset=utf-8", b)
		failOnError(err, "Failed to retrieve quote from quote server")
		w.Write([]byte("Failed to log"))
		defer res.Body.Close()
	} else {
		res, err := http.Post(auditServer+"/dumpUserLog", "application/json; charset=utf-8", b)
		failOnError(err, "Failed to retrieve quote from quote server")
		w.Write([]byte("Failed to log"))
		defer res.Body.Close()
	}
}

func displaySummaryHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		TransactionNum int
		UserID         string
	}{0, ""}

	// Parse request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse request")
	logUserCommand(req.TransactionNum, "transaction-server", "DISPLAY_SUMMARY", req.UserID, "", "", 0.0)
	w.WriteHeader(http.StatusOK)
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	enableCors(&w)

	req := struct {
		UserID string
	}{""}

	response := struct {
		Balance float64
	}{0}

	_ = decoder.Decode(&req)

	queryString := "SELECT balance FROM users WHERE user_id = $1;"

	stmt, _ := db.Prepare(queryString)

	err := stmt.QueryRow(req.UserID).Scan(&response.Balance)
	defer stmt.Close()

	// If query returns nothing, we need to add the user to the database with a balance of 0
	if err != nil {
		queryString = "INSERT INTO users (user_id, balance) VALUES ($1, $2)"

		stmt, _ := db.Prepare(queryString)

		res, _ := stmt.Exec(req.UserID, 0)

		numrows, err := res.RowsAffected()
		if numrows < 1 {
			failOnError(err, "Failed to add balance")
		}
		response.Balance = 0.0
	}
	payload, _ := json.Marshal(response)

	w.Header().Set("Content-Type", "application/json")
	w.Write(payload)
}

func enableCors(w *http.ResponseWriter) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
}

func audit(audits <-chan interface{}) {
	channelName := ""
	for audit := range audits {
		
		switch audit.(type) {
		case UserCommand:
			channelName = "uc"
		case AccountTransaction:
			channelName = "at"
		case QuoteServerEvent:
			channelName = "qse"
		case SystemEvent:
			channelName = "se"
		}
		body, err := json.Marshal(audit)
		failOnError(err, "Failed to marshall data")
		err = auditmq.Publish(
			"",          // exchange
			channelName, // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType:     "application/json",
				ContentEncoding: "",
				Body:            []byte(body),
			})
		failOnError(err, "Failed to publish a "+channelName+" message")
	}
}

func initRabbit() {
	conn, err := amqp.Dial("amqp://guest:guest@audit-mq:5672/")
	for err != nil {
		conn, err = amqp.Dial("amqp://guest:guest@audit-mq:5672/")
		fmt.Println(err)
		time.Sleep(time.Duration(5) * time.Second)
	}
	failOnError(err, "Failed to connect to RabbitMQ")

	auditmq, err = conn.Channel()
	failOnError(err, "Failed to open a channel")

	ucQueue, err = auditmq.QueueDeclare(
		"uc",  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	qseQueue, err = auditmq.QueueDeclare(
		"qse", // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	seQueue, err = auditmq.QueueDeclare(
		"se",  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	atQueue, err = auditmq.QueueDeclare(
		"at",  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")
}

func main() {
	initRabbit()
	defer auditmq.Close()
	go audit(auditChan)
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
	http.HandleFunc("/login", loginHandler)
	http.ListenAndServe(port, nil)
}
