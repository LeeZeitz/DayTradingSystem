package main

func logSystemEvent(transactionNum int, server string, command string, username string, stock string, filename string, funds float64) {
	timestamp := createTimestamp()
	req := SystemEvent{TransactionNum: transactionNum, Server: server, Command: command, Username: username, Stock: stock, Filename: filename, Funds: funds, Timestamp: timestamp}
	auditChan <- req
}

func logUserCommand(transactionNum int, server string, command string, username string, stock string, filename string, funds float64) {
	timestamp := createTimestamp()
	req := UserCommand{TransactionNum: transactionNum, Server: server, Command: command, Username: username, Stock: stock, Filename: filename, Funds: funds, Timestamp: timestamp}
	auditChan <- req
}

func logAccountTransaction(transactionNum int, server string, action string, username string, funds float64) {
	timestamp := createTimestamp()
	req := AccountTransaction{TransactionNum: transactionNum, Server: server, Action: action, Username: username, Funds: funds, Timestamp: timestamp}
	auditChan <- req
}

func logQuoteServer(transactionNum int, server string, username string, stock string, cryptoKey string, quoteServerTime int64, price float64) {
	timestamp := createTimestamp()
	req := QuoteServerEvent{TransactionNum: transactionNum, Server: server, Username: username, Stock: stock, CryptoKey: cryptoKey, QuoteServerTime: quoteServerTime, Price: price, Timestamp: timestamp}
	auditChan <- req
}
