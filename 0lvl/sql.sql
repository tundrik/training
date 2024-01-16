DROP TABLE item;
DROP TABLE trade;

CREATE TABLE trade (
    pk        VARCHAR(32) PRIMARY KEY,
    rang      BIGINT,
    entity    JSONB
);

CREATE TABLE trade (
    pk        VARCHAR(32) PRIMARY KEY,
    entry     VARCHAR(16)
);

CREATE TABLE item (
    trade_pk   VARCHAR(32) REFERENCES trade,
    chrt_id    INTEGER
);



WITH new_order AS (INSERT INTO trade (pk, entry) VALUES ('rwmr69dshdq9nojsv98ep3qeejd6mytt', 'WBIL') RETURNING pk) 
INSERT INTO item(trade_pk, chrt_id) 
SELECT 'rwmr69dshdq9nojsv98ep3qeejd6mytt' trade_pk, chrt_id
FROM    unnest(ARRAY[1345,2345,3345,4356,524]) chrt_id;

SELECT pk, entry FROM trade;

SELECT trade.pk, trade.entry, array_agg(item) item
FROM trade
LEFT OUTER JOIN item
ON trade.pk = item.trade_pk
GROUP BY trade.pk;

SELECT DISTINCT i.chrt_id, r.pk, r.entry FROM item i JOIN trade r ON r.pk = i.trade_pk;


SELECT * FROM trade WHERE entity ?| array['items', 'b'];