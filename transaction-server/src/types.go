package main

type SystemEvent struct {
	TransactionNum int
	Server         string
	Command        string
	Username       string
	Stock          string
	Filename       string
	Funds          float64
}

type UserCommand struct {
	TransactionNum int
	Server         string
	Command        string
	Username       string
	Stock          string
	Filename       string
	Funds          float64
}

type AccountTransaction struct {
	TransactionNum int
	Server         string
	Action         string
	Username       string
	Funds          float64
}

type QuoteServerEvent struct {
	TransactionNum  int
	Server          string
	Username        string
	Stock           string
	CryptoKey       string
	QuoteServerTime int64
	Price           float64
}
