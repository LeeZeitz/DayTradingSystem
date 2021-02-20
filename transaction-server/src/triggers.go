package main

import (
	"database/sql"

	"strconv"
	"time"
)

// Consumes a trigger and performs any buy/sell actions associated with it
// Parameters:
// 		UserID: 		(string) id of the user who owns the trigger to fire
// 		Symbol: 		(string) the symbol of the stock being triggered
//		method:			(string) the type of action to perform, one of ("buy", "sell")
//
func fireTrigger(UserID string, Symbol string, method string) {

	// Get transaction num
	var transactionNum int
	queryString := "SELECT transaction_num FROM triggers WHERE user_id = $1 AND symbol = $2 AND method = $3;"
	stmt, err := db.Prepare(queryString)
	if err != nil {
		failGracefully(err, "Failed to prepare transactionNum query")
		return
	}
	err = stmt.QueryRow(UserID, Symbol, method).Scan(&transactionNum)

	if err != nil {
		failGracefully(err, "Failed to get transactionNum")
		return
	}

	// Consume trigger
	queryString = "DELETE FROM triggers WHERE user_id = $1 AND symbol = $2 AND method = $3;"
	rows, err := db.Query(queryString, UserID, Symbol, method)
	if err != nil{
		failGracefully(err, "Failed to delete trigger after firing")
		return
	}
	defer rows.Close()

	whereCond := "WHERE user_id = $1 AND symbol = $2"

	var amount float64
	var quantity int
	// Get quantity of stock to buy/sell
	queryString = "SELECT quantity FROM " + method + "_amounts " + whereCond
	stmt, err = db.Prepare(queryString)
	if err != nil {
		failGracefully(err, "Failed to prepare SELECT quantity query")
		return
	}
	err = stmt.QueryRow(UserID, Symbol).Scan(&amount)
	if err != nil {
		failGracefully(err, "Failed to get quantity from "+method+"_amounts")
		return
	}

	quantity = int(amount / getQuote(Symbol, transactionNum, UserID))

	// Delete buy/sell amount from user's account
	queryString = "DELETE FROM " + method + "_amounts " + whereCond
	rows, err = db.Query(queryString, UserID, Symbol)
	if err != nil {

		failGracefully(err, "Failed to delete "+method+" amount after trigger fire")
	}
	defer rows.Close()

	// Add/subtract the stocks to user's account
	if method == "buy" {
		buyStock(UserID, Symbol, strconv.Itoa(quantity), transactionNum)
		logSystemEvent(transactionNum, "transaction-server", "BUY", UserID, Symbol, "", float64(quantity))
	} else {
		sellStock(UserID, Symbol, strconv.Itoa(quantity), transactionNum)
		logSystemEvent(transactionNum, "transaction-server", "SELL", UserID, Symbol, "", float64(quantity))
	}
}

// Evaluates whether or not a given trigger should fire
// Parameters:
// 		UserID: 		(string) id of the user who owns the trigger to fire
// 		Symbol: 		(string) the symbol of the stock being triggered
//		method:			(string) the type of action to perform, one of ("buy", "sell")
//
func evalTrigger(UserID string, Symbol string, method string) bool {
	queryString := "SELECT price, transaction_num FROM triggers WHERE symbol = $1 AND user_id = $2 and method = $3;"	

	stmt, err := db.Prepare(queryString)
	if err != nil {

		failGracefully(err, "Failed to prepare query")
	}
	res := struct {
		triggerPrice   float64
		transactionNum int
	}{0.0, 0}

	// Try to get a trigger for given user, symbol, and method
	err = stmt.QueryRow(Symbol, UserID, method).Scan(&res.triggerPrice, &res.transactionNum)
	defer stmt.Close()

	// If no trigger exists, stop the routine monitoring it
	if err == sql.ErrNoRows {
		return true
	} else {
		// If trigger still exists, check the value of the trigger against the price
		quote := getQuote(Symbol, res.transactionNum, UserID)
		diff := res.triggerPrice - quote
		if method == "sell" {
			diff *= -1.0
		}
		// If the difference if greater than or equal to 0, fire the trigger!
		if diff >= 0 {
			fireTrigger(UserID, Symbol, method)
			return true
		} else {
			// The trigger still exists, but should not be fired yet so we are not done monitoring yet
			return false
		}
	}
}

// Monitors a trigger (in a goroutine, should probably be called in a goroutine as well) as long as it exists.
// Retrieves a quote for the trigger's stock every 60 seconds until the trigger fires or gets cancelled.
// Parameters:
// 		UserID: 		(string) id of the user who owns the trigger to fire
// 		Symbol: 		(string) the symbol of the stock being triggered
//		method:			(string) the type of action to perform, one of ("buy", "sell")
//
func monitorTrigger(UserID string, Symbol string, method string) {
	// Create a ticker that fires every 60 seconds
	ticker := time.NewTicker(10 * time.Second)

	// Every time the ticker fires, check the trigger
	for _ = range ticker.C {
		//fmt.Println("Tick at", i)
		done := evalTrigger(UserID, Symbol, method)
		if done {
			//fmt.Println("here")
			return
		}
	}
}
