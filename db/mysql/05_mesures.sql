CREATE TABLE IF NOT EXISTS mesures (
    symbol        VARCHAR(16) NOT NULL,
    trade_date    DATE        NOT NULL,
    log_return    DECIMAL(12,8),     -- rendement logarithmique du jour
    volatility_30 DECIMAL(12,8),     -- écart-type des rendements log sur 30 jours
    sma_20        DECIMAL(14,4),     -- moyenne mobile simple 20 jours (tendance court terme)
    sma_50        DECIMAL(14,4),     -- moyenne mobile simple 50 jours (tendance moyen terme)
    volume_ma_20  BIGINT,            -- volume moyen sur 20 jours
    trend         VARCHAR(12),       -- 'haussiere' / 'baissiere' (SMA20 vs SMA50)

    PRIMARY KEY (symbol, trade_date),
    CONSTRAINT fk_mesures_actif
        FOREIGN KEY (symbol) REFERENCES actif (symbol)
        ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
