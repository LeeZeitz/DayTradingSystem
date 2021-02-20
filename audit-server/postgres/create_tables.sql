CREATE TABLE IF NOT EXISTS user_commands(
    timestamp BIGINT,
    transaction_num INT,
    server VARCHAR(64),
    command VARCHAR(64),
    stock VARCHAR(64),
    filename VARCHAR(64),
    funds REAL,
    user_id VARCHAR(64)
);
CREATE TABLE IF NOT EXISTS system_events(
    timestamp BIGINT,
    transaction_num INT,
    server VARCHAR(64),
    user_id VARCHAR(64),
    command VARCHAR(64),
    stock VARCHAR(64),
    filename VARCHAR(64),
    funds REAL
);
CREATE TABLE IF NOT EXISTS quote_server_events(
    timestamp BIGINT,
    transaction_num INT,
    server VARCHAR(64),
    user_id VARCHAR(64),	  
    stock VARCHAR(64),  
    crypto_key VARCHAR(64),
    quote_server_time BIGINT,
    price REAL    
);
CREATE TABLE IF NOT EXISTS account_transactions(
    timestamp BIGINT,
    server VARCHAR(64),
    transaction_num INT, 
    action VARCHAR(64),
    user_id VARCHAR(64),
    funds REAL  
);
CREATE TABLE IF NOT EXISTS error_events(
    timestamp BIGINT,
    server VARCHAR(64),
    transaction_num INT,
    user_id VARCHAR(64),
    stock VARCHAR(64),
    filename VARCHAR(64),
    error_message VARCHAR(64),
    funds REAL     
);
