package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
)

const (
	host   = "transaction-db"
	port   = 5432
	user   = "postgres"
	dbname = "postgres"
)

var (
	//	dbstring = func() string {
	//		if runningInDocker() {
	//			return "http://transaction-db:4200"
	//		}
	//		return "http://localhost:4200"
	//	}()

	dbstring = fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable", host, port, user, dbname)

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

func createTimestamp() int64 {
	return time.Now().UTC().UnixNano() / int64(time.Millisecond)
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
	fmt.Println(req.Amount)
	logUserCommand(req.TransactionNum, "transaction-server", "ADD", req.UserID, "", "", req.Amount)

	if req.Amount < 0 {
		fmt.Println("Can't add a negative balance")
		return
	}

	logAccountTransaction(req.TransactionNum, "transaction-server", "add", req.UserID, req.Amount)

	// Insert new user if they don't already exist, otherwise update their balance
	queryString := "INSERT INTO users (user_id, balance) VALUES ($1, $2)" +
		"ON CONFLICT (user_id) DO UPDATE SET balance = users.balance + $2"

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
		"ON CONFLICT (user_id, symbol) DO UPDATE SET quantity = buy_amounts.quantity + $3;"

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
		"ON CONFLICT (user_id, symbol) DO UPDATE SET quantity = sell_amounts.quantity + $3;"

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
