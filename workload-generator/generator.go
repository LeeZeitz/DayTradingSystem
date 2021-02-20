package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"fmt"
	"time"
)

var (
	client = &http.Client{
	}
)

type Add struct {
	UserID         string
	Amount         float64
	TransactionNum int
}

type Quote struct {
	UserID         string
	Symbol         string
	TransactionNum int
}

type Default struct {
	UserID         string
	Symbol         string
	Amount         float64
	TransactionNum int
}

type DefaultTrig struct {
	UserID         string
	Symbol         string
	Price          int
	TransactionNum int
}

type User struct {
	UserID         string
	TransactionNum int
}

type Dumplog struct {
	Filename       string
	TransactionNum int
	Username       string
}

func sendRequest(line string) {
	arguments := strings.Split(line, ",")
	command_type_and_tn := strings.Split(arguments[0], " ")
	command_type := command_type_and_tn[1]
	transactionNum := command_type_and_tn[0][1 : len(command_type_and_tn[0])-1]
	
	fmt.Println(transactionNum)
	if command_type == "ADD" {
		req := Add{}
		req.TransactionNum, _ = strconv.Atoi(transactionNum)
		req.UserID = strings.Trim(arguments[1], " ")
		req.Amount, _ = strconv.ParseFloat(arguments[2], 64)
		sendToWebServer(req, command_type)
	}

	if command_type == "BUY" || command_type == "SELL" || command_type == "SET_BUY_AMOUNT" || command_type == "SET_SELL_AMOUNT" {
		req := Default{}
		req.TransactionNum, _ = strconv.Atoi(transactionNum)
		req.UserID = strings.Trim(arguments[1], " ")
		req.Symbol = strings.Trim(arguments[2], " ")
		req.Amount, _ = strconv.ParseFloat(arguments[3], 64)
		sendToWebServer(req, command_type)
	}

	if command_type == "SET_BUY_TRIGGER" || command_type == "SET_SELL_TRIGGER" {
		req := DefaultTrig{}
		req.TransactionNum, _ = strconv.Atoi(transactionNum)
		req.UserID = strings.Trim(arguments[1], " ")
		req.Symbol = strings.Trim(arguments[2], " ")
		req.Price, _ = strconv.Atoi(arguments[3])
		sendToWebServer(req, command_type)
	}

	if command_type == "QUOTE" || command_type == "CANCEL_SET_BUY" || command_type == "CANCEL_SET_SELL" {
		req := Quote{}
		req.TransactionNum, _ = strconv.Atoi(transactionNum)
		req.UserID = strings.Trim(arguments[1], " ")
		req.Symbol = strings.Trim(arguments[2], " ")
		sendToWebServer(req, command_type)
	}

	if command_type == "COMMIT_BUY" || command_type == "CANCEL_BUY" || command_type == "COMMIT_SELL" || command_type == "CANCEL_SELL" || command_type == "DISPLAY_SUMMARY" {
		req := User{}
		req.TransactionNum, _ = strconv.Atoi(transactionNum)
		req.UserID = strings.Trim(arguments[1], " ")
		sendToWebServer(req, command_type)
	}

}

func sendToWebServer(r interface{}, s string) {
	jsonValue, _ := json.Marshal(r)
	req, err := http.NewRequest("POST", "http://192.168.1.223:8123/"+strings.ToLower(s), bytes.NewBuffer(jsonValue))
	req.Close = true
	fmt.Println(err)
	req.Header.Set("Content-Type", "application/json")
	resp, _ := client.Do(req)

	if resp != nil {
		resp.Body.Close()
	}

}

func main() {
	file, _ := os.Open("workloads/final_workload_2019")
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {

		commandText := scanner.Text()
	

		go sendRequest(commandText)
		time.Sleep(time.Millisecond)
	}
}
