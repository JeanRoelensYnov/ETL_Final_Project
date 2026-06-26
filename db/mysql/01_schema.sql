CREATE TABLE IF NOT EXISTS cours_bourse (
    id           BIGINT       NOT NULL AUTO_INCREMENT,
    symbol       VARCHAR(16)  NOT NULL,             -- ticker, ex. 'AAPL'
    ts           DATETIME     NOT NULL,             -- horodatage du cours (en UTC)
    open         DECIMAL(14,4),                     
    high         DECIMAL(14,4),                     
    low          DECIMAL(14,4),
    close        DECIMAL(14,4),
    volume       BIGINT,                            -- nombre d'unités échangées
    ingested_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP, 

    PRIMARY KEY (id),
    -- Un même (symbole, instant) ne doit exister qu'une fois :
    -- permet l'INSERT idempotent (gère les doublons qu'on a déjà vus en Kafka).
    UNIQUE KEY uq_symbol_ts (symbol, ts),
    -- Index pour les requêtes du type "tous les cours d'un symbole dans le temps".
    KEY idx_symbol_ts (symbol, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
