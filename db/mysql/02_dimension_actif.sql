CREATE TABLE IF NOT EXISTS actif (
    symbol     VARCHAR(16)  NOT NULL,             -- ex. 'AAPL', 'BTC-USD', '^GSPC'
    name       VARCHAR(128),                      -- ex. 'Apple Inc.'
    type       VARCHAR(20),                       -- EQUITY / CRYPTOCURRENCY / INDEX
    sector     VARCHAR(64),                       -- NULL pour crypto/indices
    industry   VARCHAR(128),
    country    VARCHAR(64),
    currency   VARCHAR(8),                        -- ex. 'USD', 'EUR'
    exchange   VARCHAR(32),                       -- place de cotation
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                         ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
