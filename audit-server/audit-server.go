package main

import (
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"time"

	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
)

const (
	host   = "audit-db"
	port   = 5432
	user   = "postgres"
	dbname = "postgres"
)

var (
	/*
	auditstring = func() string {
		if runningInDocker() {
			return "http://audit-db:4200"
		}
		return "http://localhost:4201"
	}()
	*/
	auditstring = fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable", host, port, user, dbname)

	db = loadDb(auditstring, 0)
)

func runningInDocker() bool {
	if _, err := os.Stat("/.dockerenv"); !os.IsNotExist(err) {
		return true
	}
	return false
}

func failOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s\n", msg, err)
		panic(err)
	}
}

var count = 0

func loadDb(dbstring string, count int) *sql.DB {
	db, err := sql.Open("postgres", auditstring)
	
	// If can't connect to DB
	failOnError(err, "Couldn't connect to DB")
	err = db.Ping()
	/*
	failOnError(err, "Couldn't ping DB")
	*/
	if err != nil && count <= 100 {
		db = loadDb(dbstring, count + 1)
	}else {
		failOnError(err, "couldn't Ping to DB")
	}
	return db
}

func createTimestamp() int64 {
	return time.Now().UTC().UnixNano() / int64(time.Millisecond)
}

// UserCommand data type
type UserCommand struct {
	XMLName        xml.Name `xml:"userCommand"`
	Timestamp      int      `xml:"timestamp"`
	Server         string   `xml:"server"`
	TransactionNum int      `xml:"transactionNum"`
	Command        string   `xml:"command"`
	Username       string   `xml:"username,omitempty"`
	StockSymbol    string   `xml:"stockSymbol,omitempty"`
	Filename       string   `xml:"filename,omitempty"`
	Funds          float64  `xml:"funds,omitempty"`
}

func (uc UserCommand) GetTimestamp() int {
	return uc.Timestamp
}

func (uc UserCommand) GetTransactionNum() int {
	return uc.TransactionNum
}

// SystemEvent data type
type SystemEvent struct {
	XMLName        xml.Name `xml:"systemEvent"`
	Timestamp      int      `xml:"timestamp"`
	Server         string   `xml:"server"`
	TransactionNum int      `xml:"transactionNum"`
	Command        string   `xml:"command"`
	Username       string   `xml:"username,omitempty"`
	StockSymbol    string   `xml:"stockSymbol,omitempty"`
	Filename       string   `xml:"filename,omitempty"`
	Funds          float64  `xml:"funds,omitempty"`
}

func (se SystemEvent) GetTimestamp() int {
	return se.Timestamp
}

func (se SystemEvent) GetTransactionNum() int {
	return se.TransactionNum
}

// QuoteServer data type
type QuoteServer struct {
	XMLName         xml.Name `xml:"quoteServer"`
	Timestamp       int      `xml:"timestamp"`
	Server          string   `xml:"server"`
	TransactionNum  int      `xml:"transactionNum"`
	Price           float64  `xml:"price"`
	StockSymbol     string   `xml:"stockSymbol"`
	Username        string   `xml:"username"`
	QuoteServerTime int      `xml:"quoteServerTime"`
	CryptoKey       string   `xml:"cryptokey"`
}

func (qs QuoteServer) GetTimestamp() int {
	return qs.Timestamp
}

func (qs QuoteServer) GetTransactionNum() int {
	return qs.TransactionNum
}

// AccountTransaction data type
type AccountTransaction struct {
	XMLName        xml.Name `xml:"accountTransaction"`
	Timestamp      int      `xml:"timestamp"`
	Server         string   `xml:"server"`
	TransactionNum int      `xml:"transactionNum"`
	Action         string   `xml:"action"`
	Username       string   `xml:"username"`
	Funds          float64  `xml:"funds"`
}

func (at AccountTransaction) GetTimestamp() int {
	return at.Timestamp
}

func (at AccountTransaction) GetTransactionNum() int {
	return at.TransactionNum
}

// ErrorEvent data type
type ErrorEvent struct {
	XMLName        xml.Name `xml:"errorEvent"`
	Timestamp      int      `xml:"timestamp"`
	Server         string   `xml:"server"`
	TransactionNum int      `xml:"transactionNum"`
	Command        string   `xml:"command"`
	Username       string   `xml:"username,omitempty"`
	StockSymbol    string   `xml:"stockSymbol,omitempty"`
	Filename       string   `xml:"filename,omitempty"`
	Funds          float64  `xml:"funds,omitempty"`
	ErrorMessage   string   `xml:"errorMessage,omitempty"`
}

func (ee ErrorEvent) GetTimestamp() int {
	return ee.Timestamp
}

func (ee ErrorEvent) GetTransactionNum() int {
	return ee.TransactionNum
}

type LogType interface {
	GetTimestamp() int
	GetTransactionNum() int
}

func logUserCommandHandler() {
	conn, err := amqp.Dial("amqp://guest:guest@audit-mq:5672/")
	for err != nil {
		conn, err = amqp.Dial("amqp://guest:guest@audit-mq:5672/")
		fmt.Println(err)
		time.Sleep(time.Duration(5) * time.Second)
	}
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"uc",  // name
		false, // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {

			req := struct {
				TransactionNum int
				Server         string
				Command        string
				Username       string
				Stock          string
				Filename       string
				Funds          float64
				Timestamp      int64
			}{0, "", "", "", "", "", 0.0, 0}
			err := json.Unmarshal(d.Body, &req)
			failOnError(err, "Failed to parse the request")

			queryString := "INSERT INTO user_commands (command, filename, funds, server, stock, timestamp, transaction_num, user_id)" +
				" VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

			stmt, err := db.Prepare(queryString)
			failOnError(err, "Failed to prepare user command log query")

			res, err := stmt.Exec(req.Command, req.Filename, req.Funds, req.Server, req.Stock, req.Timestamp, req.TransactionNum, req.Username)
			failOnError(err, "Failed to add user command log")

			numrows, err := res.RowsAffected()
			if numrows < 1 {
				failOnError(err, "Failed to add user command log")
			}
		}
	}()

	log.Printf(" [*] Waiting for user commands.")
	<-forever

}

func logSystemEventHandler() {
	conn, err := amqp.Dial("amqp://guest:guest@audit-mq:5672/")
	for err != nil {
		conn, err = amqp.Dial("amqp://guest:guest@audit-mq:5672/")
		fmt.Println(err)
		time.Sleep(time.Duration(5) * time.Second)
	}
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"se",  // name
		false, // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			req := struct {
				TransactionNum int
				Server         string
				Command        string
				Username       string
				Stock          string
				Filename       string
				Funds          float64
				Timestamp      int64
			}{0, "", "", "", "", "", 0.0, 0}
			err := json.Unmarshal(d.Body, &req)
			failOnError(err, "Failed to parse the request")

			queryString := "INSERT INTO system_events (command, filename, funds, server, stock, timestamp, transaction_num, user_id)" +
				" VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

			stmt, err := db.Prepare(queryString)
			failOnError(err, "Failed to prepare system event log query")

			res, err := stmt.Exec(req.Command, req.Filename, req.Funds, req.Server, req.Stock, req.Timestamp, req.TransactionNum, req.Username)
			failOnError(err, "Failed to add system event log")

			numrows, err := res.RowsAffected()
			if numrows < 1 {
				failOnError(err, "Failed to add system event log")
			}
		}
	}()

	log.Printf(" [*] Waiting for system events.")
	<-forever
}

func logQuoteServerHandler() {
	conn, err := amqp.Dial("amqp://guest:guest@audit-mq:5672/")
	for err != nil {
		conn, err = amqp.Dial("amqp://guest:guest@audit-mq:5672/")
		fmt.Println(err)
		time.Sleep(time.Duration(5) * time.Second)
	}
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"qse", // name
		false, // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			req := struct {
				TransactionNum  int
				Server          string
				Username        string
				Stock           string
				CryptoKey       string
				QuoteServerTime int
				Price           float64
				Timestamp       int64
			}{0, "", "", "", "", 0, 0.0, 0}
			err := json.Unmarshal(d.Body, &req)
			failOnError(err, "Failed to parse the request")

			queryString := "INSERT INTO quote_server_events (crypto_key, price, quote_server_time, server, stock, timestamp, transaction_num, user_id)" +
				" VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

			stmt, err := db.Prepare(queryString)
			failOnError(err, "Failed to prepare quote server event log query")

			res, err := stmt.Exec(req.CryptoKey, req.Price, req.QuoteServerTime, req.Server, req.Stock, req.Timestamp, req.TransactionNum, req.Username)
			failOnError(err, "Failed to add quote server event log")

			numrows, err := res.RowsAffected()
			if numrows < 1 {
				failOnError(err, "Failed to add quote server event log")
			}
		}
	}()
	log.Printf(" [*] Waiting for quote server events.")
	<-forever
}

func logAccountTransactionHandler() {
	conn, err := amqp.Dial("amqp://guest:guest@audit-mq:5672/")
	for err != nil {
		conn, err = amqp.Dial("amqp://guest:guest@audit-mq:5672/")
		fmt.Println(err)
		time.Sleep(time.Duration(5) * time.Second)
	}
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"at",  // name
		false, // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			req := struct {
				TransactionNum int
				Server         string
				Action         string
				Username       string
				Funds          float64
				Timestamp      int64
			}{0, "", "", "", 0.0, 0}
			err := json.Unmarshal(d.Body, &req)
			failOnError(err, "Failed to parse the request")

			queryString := "INSERT INTO account_transactions (action, funds, server, timestamp, transaction_num, user_id)" +
				" VALUES ($1, $2, $3, $4, $5, $6)"

			stmt, err := db.Prepare(queryString)
			failOnError(err, "Failed to prepare account transaction log query")

			res, err := stmt.Exec(req.Action, req.Funds, req.Server, req.Timestamp, req.TransactionNum, req.Username)
			failOnError(err, "Failed to add account transaction log")

			numrows, err := res.RowsAffected()
			if numrows < 1 {
				failOnError(err, "Failed to add account transaction log")
			}
		}
	}()
	log.Printf(" [*] Waiting for account transactions.")
	<-forever
}

func logErrorEventHandler() {
	conn, err := amqp.Dial("amqp://guest:guest@audit-mq:5672/")
	for err != nil {
		conn, err = amqp.Dial("amqp://guest:guest@audit-mq:5672/")
		fmt.Println(err)
		time.Sleep(time.Duration(5) * time.Second)
	}
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"ee",  // name
		false, // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			req := struct {
				TransactionNum int
				Server         string
				Command        string
				Username       string
				Stock          string
				Filename       string
				ErrorMessage   string
				Funds          float64
			}{0, "", "", "", "", "", "", 0.0}
			err := json.Unmarshal(d.Body, &req)

			failOnError(err, "Failed to parse the request")

			queryString := "INSERT INTO error_events (error_message, filename, funds, server, stock, timestamp, transaction_num, user_id)" +
				" VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

			timestamp := createTimestamp()

			stmt, err := db.Prepare(queryString)
			failOnError(err, "Failed to prepare error events log query")

			res, err := stmt.Exec(req.ErrorMessage, req.Filename, req.Funds, req.Server, req.Stock, timestamp, req.TransactionNum, req.Username)
			failOnError(err, "Failed to add error events log")

			numrows, err := res.RowsAffected()
			if numrows < 1 {
				failOnError(err, "Failed to add error events log")
			}
		}
	}()
	log.Printf(" [*] Waiting for error events.")
	<-forever
}

func dumpLogHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		Filename string
	}{""}
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	dumpLog(req.Filename, "", false)
}

func dumpUserLogHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	req := struct {
		Filename string
		UserID   string
	}{"", ""}
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	dumpLog(req.Filename, req.UserID, true)
}

func dumpLog(filename string, username string, isUser bool) {
	userquery := " LIMIT 1200000"
	if isUser {
		userquery = " WHERE user_id = '" + username + "' LIMIT 1200000"
	}
	logs := []LogType{}

	// Get usercommands
	queryString := "SELECT command, filename, funds, server, stock, timestamp, transaction_num, user_id FROM user_commands" + userquery
	rows, err := db.Query(queryString)
	failOnError(err, "Failed to prepare query")
	defer rows.Close()

	for rows.Next() {

		logEvent := UserCommand{}

		if err := rows.Scan(&logEvent.Command, &logEvent.Filename, &logEvent.Funds, &logEvent.Server,
			&logEvent.StockSymbol, &logEvent.Timestamp, &logEvent.TransactionNum, &logEvent.Username); err != nil {
			fmt.Println("-----------------------HERE YOOOOOOO-------------------------")
			log.Fatal(err)
		}

		logs = append(logs, logEvent)
	}

	// Get systemevents
	queryString = "SELECT command, filename, funds, server, stock, timestamp, transaction_num, user_id FROM system_events" + userquery

	rows, err = db.Query(queryString)
	failOnError(err, "Failed to prepare query")
	defer rows.Close()

	for rows.Next() {
		logEvent := SystemEvent{}

		if err := rows.Scan(&logEvent.Command, &logEvent.Filename, &logEvent.Funds, &logEvent.Server,
			&logEvent.StockSymbol, &logEvent.Timestamp, &logEvent.TransactionNum, &logEvent.Username); err != nil {
			fmt.Println("---------------------------HERERER TOOOOOO-------------------------------")
			log.Fatal(err)
		}

		logs = append(logs, logEvent)
	}

	// Get quoteserver
	queryString = "SELECT crypto_key, price, quote_server_time, server, stock, timestamp, transaction_num, user_id FROM quote_server_events" + userquery

	rows, err = db.Query(queryString)
	failOnError(err, "Failed to prepare query")
	defer rows.Close()

	for rows.Next() {
		logEvent := QuoteServer{}

		if err := rows.Scan(&logEvent.CryptoKey, &logEvent.Price, &logEvent.QuoteServerTime,
			&logEvent.Server, &logEvent.StockSymbol, &logEvent.Timestamp, &logEvent.TransactionNum, &logEvent.Username); err != nil {
			fmt.Println("--------------------------Still herererererer-------------------------------")
			log.Fatal(err)
		}

		logs = append(logs, logEvent)
	}

	// Get accounttransactions
	queryString = "SELECT action, funds, server, timestamp, transaction_num, user_id FROM account_transactions" + userquery

	rows, err = db.Query(queryString)
	failOnError(err, "Failed to prepare query")
	defer rows.Close()

	for rows.Next() {
		logEvent := AccountTransaction{}

		if err := rows.Scan(&logEvent.Action, &logEvent.Funds, &logEvent.Server, &logEvent.Timestamp,
			&logEvent.TransactionNum, &logEvent.Username); err != nil {
			fmt.Println("-------------------------- ALMOST THER-------------------------------")
			log.Fatal(err)
		}

		logs = append(logs, logEvent)
	}

	// Get errorevents
	queryString = "SELECT error_message, filename, funds, server, stock, timestamp, transaction_num, user_id FROM error_events" + userquery

	rows, err = db.Query(queryString)
	failOnError(err, "Failed to prepare query")
	defer rows.Close()

	for rows.Next() {
		logEvent := ErrorEvent{}

		if err := rows.Scan(&logEvent.ErrorMessage, &logEvent.Filename, &logEvent.Funds, &logEvent.Server,
			&logEvent.StockSymbol, &logEvent.Timestamp, &logEvent.TransactionNum, &logEvent.Username); err != nil {
			fmt.Println("----------------------------LAST ONE WOOO-------------------------------")
			log.Fatal(err)
		}

		logs = append(logs, logEvent)
	}

	// Sort by timestamp then by transactionNum
	sort.Slice(logs, func(i, j int) bool {
		return logs[i].GetTransactionNum() < logs[j].GetTransactionNum()
	})

	// Write to file
	file, err := os.Create(filename)
	failOnError(err, "File couldn't be created")
	defer file.Close()
	file.Write([]byte("<?xml version=\"1.0\"?>\n"))
	file.Write([]byte("<log>\n"))
	test, err := xml.MarshalIndent(logs, "  ", "    ")
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}

	n1, err := file.Write(test)
	file.Write([]byte("\n</log>"))
	fmt.Printf("wrote %d bytes\n", n1)

	failOnError(err, "Failed to write log files to XML")

}

func main() {
	go logUserCommandHandler()
	go logQuoteServerHandler()
	go logAccountTransactionHandler()
	go logSystemEventHandler()
	go logErrorEventHandler()

	port := ":8081"

	http.HandleFunc("/dumpLog", dumpLogHandler)
	http.HandleFunc("/dumpUserLog", dumpUserLogHandler)
	http.ListenAndServe(port, nil)
}
