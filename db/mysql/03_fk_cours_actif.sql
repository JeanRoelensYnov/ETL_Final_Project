ALTER TABLE cours_bourse
    ADD CONSTRAINT fk_cours_actif
    FOREIGN KEY (symbol) REFERENCES actif (symbol)
    ON UPDATE CASCADE      -- si un symbole est renommé dans actif, les cours suivent
    ON DELETE RESTRICT;    -- interdit de supprimer un actif encore référencé par des cours
