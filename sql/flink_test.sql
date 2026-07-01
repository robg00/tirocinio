SET 'execution.runtime-mode' = 'batch';

CREATE TABLE test_source (
    id INT,
    name STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1000',
    'number-of-rows' = '5',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '5'
);

CREATE TABLE test_sink (
    id INT,
    name STRING
) WITH (
    'connector' = 'print'
);

INSERT INTO test_sink SELECT * FROM test_source;
