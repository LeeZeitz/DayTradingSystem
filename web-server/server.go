package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"hash/fnv"
)

func runningInDocker() bool {
	if _, err := os.Stat("/.dockerenv"); !os.IsNotExist(err) {
		return true
	}
	return false
}

var transactionServer = func() string {
	if runningInDocker() {
		print("Using http://transaction:8080")
		return "http://transaction:8080"
	}
	return "http://localhost:8080"
}()

// Checks and panics on error
// Parameters:
//     err:    the error to check
//     msg:    a message to print to the console if an error is found

func hashServer(user string) string {
	server1 := "http://192.168.1.169:8080"
	server2 := "http://192.168.1.156:8080"
	server3 := "http://192.168.1.180:8080"
	server4 := "http://192.168.1.181:8080"
	server5 := "http://192.168.1.217:8080"
	h := fnv.New32a()
        h.Write([]byte(user))
	hash := h.Sum32()
	hash = hash%5
	switch hash {
	case 0:
		return server1
	case 1:
		return server2
	case 2:
		return server3
	case 3: 
		return server4
	case 4:
		return server5
	}

	return server1
}


func failOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s", msg, err)
		panic(err)
	}
}

func addHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	w.WriteHeader(http.StatusOK)

	req := struct {
		UserID         string
		Amount         float64
		TransactionNum int
	}{"", 0.0, 0}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	// Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	
	r1, err1 := http.Post(hashServer(req.UserID)+"/add", "application/json; charset=utf-8", b)
	failOnError(err1, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func quoteHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		UserID         string
		Symbol         string
		TransactionNum int
	}{"", "", 0}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	r1, err := http.Post(hashServer(req.UserID)+"/quote", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func buyHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		UserID         string
		Amount         float64 // dolar amount of a stock to buy
		Symbol         string
		TransactionNum int
	}{"", 0.0, "", 0}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	
	r1, err := http.Post(hashServer(req.UserID)+"/buy", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func commitBuyHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		UserID         string
		TransactionNum int
	}{"", 0}
	
	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")
	
	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	r1, err := http.Post(hashServer(req.UserID)+"/commit_buy", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")
	
	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func cancelBuyHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		UserID         string
		TransactionNum int
	}{"", 0}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	r1, err := http.Post(hashServer(req.UserID)+"/cancel_buy", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func sellHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		UserID         string
		Amount         float64 // Dollar value to sell
		Symbol         string
		TransactionNum int
	}{"", 0.0, "", 0}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	r1, err := http.Post(hashServer(req.UserID)+"/sell", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func commitSellHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		UserID         string
		TransactionNum int
	}{"", 0}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	r1, err := http.Post(hashServer(req.UserID)+"/commit_sell", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func cancelSellHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		UserID         string
		TransactionNum int
	}{"", 0}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	r1, err := http.Post(hashServer(req.UserID)+"/cancel_sell", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func setBuyAmountHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		UserID         string  // id of the user buying
		Symbol         string  // symbol of the stock to buy
		Amount         float64 // dollar amount of stock to buy
		TransactionNum int
	}{"", "", 0.0, 0}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	r1, err := http.Post(hashServer(req.UserID)+"/set_buy_amount", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func cancelSetBuyHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		UserID         string
		Symbol         string
		TransactionNum int
	}{"", "", 0}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	r1, err := http.Post(hashServer(req.UserID)+"/cancel_set_buy", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func setBuyTriggerHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		UserID         string
		Symbol         string
		Price          float64
		TransactionNum int
	}{"", "", 0.0, 0}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	r1, err := http.Post(hashServer(req.UserID)+"/set_buy_trigger", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func setSellAmountHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		UserID         string
		Symbol         string
		Amount         float64 // dollar amount of stock to sell
		TransactionNum int
	}{"", "", 0.0, 0}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	r1, err := http.Post(hashServer(req.UserID)+"/set_sell_amount", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func setSellTriggerHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		UserID         string
		Symbol         string
		Price          float64
		TransactionNum int
	}{"", "", 0.0, 0}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	r1, err := http.Post(hashServer(req.UserID)+"/set_sell_trigger", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func cancelSetSellHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		UserID         string
		Symbol         string
		TransactionNum int
	}{"", "", 0}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	r1, err := http.Post(hashServer(req.UserID)+"/cancel_set_sell", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func dumpLogHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		TransactionNum int
		Filename       string
		UserID         string
	}{0, "", ""}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	r1, err := http.Post(hashServer(req.UserID)+"/dumplog", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func displaySummaryHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	w.WriteHeader(http.StatusOK)
	req := struct {
		TransactionNum int
		UserID         string
	}{0, ""}

	// Decode request parameters into struct
	err := decoder.Decode(&req)
	failOnError(err, "Failed to parse the request")

	//Encode request parameters into a struct
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)
	r1, err := http.Post(hashServer(req.UserID)+"/display_summary", "application/json; charset=utf-8", b)
	failOnError(err, "Failed to post the request")

	body, err := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	req := struct {
		UserID string
	}{""}

	_ = decoder.Decode(&req)
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(req)

	r1, _ := http.Post(hashServer(req.UserID)+"/login", "application/json; charset=utf-8", b)

	body, _ := ioutil.ReadAll(r1.Body)
	w.Write([]byte(body))
}

func main() {
	port := ":8123"
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
