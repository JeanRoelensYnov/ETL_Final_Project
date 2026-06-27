CREATE TABLE IF NOT EXISTS cours_journalier (
    symbol      VARCHAR(16) NOT NULL,
    trade_date  DATE        NOT NULL,
    open        DECIMAL(14,4),
    high        DECIMAL(14,4),
    low         DECIMAL(14,4),
    close       DECIMAL(14,4),
    volume      BIGINT,

    PRIMARY KEY (symbol, trade_date),        -- un cours par symbole et par jour
    CONSTRAINT fk_journalier_actif
        FOREIGN KEY (symbol) REFERENCES actif (symbol)
        ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
