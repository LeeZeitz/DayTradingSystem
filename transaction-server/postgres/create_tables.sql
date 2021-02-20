CREATE USER trade_user;

CREATE TABLE IF NOT EXISTS users(
    user_id VARCHAR(64),
    balance FLOAT,
    PRIMARY KEY (user_id)
);
CREATE TABLE IF NOT EXISTS stocks(
    user_id VARCHAR(64),
    symbol VARCHAR(64),
    quantity INT,
    PRIMARY KEY (user_id, symbol)
);
CREATE TABLE IF NOT EXISTS buy_amounts(
    user_id VARCHAR(64),
    symbol VARCHAR(64),  
    quantity REAL,
    PRIMARY KEY (user_id, symbol)    
);
CREATE TABLE IF NOT EXISTS sell_amounts(
    user_id VARCHAR(64),
    symbol VARCHAR(64),  
    quantity REAL,
    PRIMARY KEY (user_id, symbol)
);
CREATE TABLE IF NOT EXISTS triggers(
    user_id VARCHAR(64),
    symbol VARCHAR(64),
    price REAL,
    method VARCHAR(64),
    transaction_num INT,
    PRIMARY KEY (user_id, symbol, method)
);
