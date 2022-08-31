CREATE TABLE metadata (
    id serial PRIMARY KEY,
    name VARCHAR(250) UNIQUE NOT NULL,
    alias VARCHAR(250) NOT NULL
);

CREATE TABLE metadata_table_main (
    id serial PRIMARY KEY,
    table_name VARCHAR(250) UNIQUE NOT NULL,
    table_number INT NOT NULL,
    metadata_id INT, 
    FOREIGN KEY (metadata_id) REFERENCES metadata (id)
);

CREATE TABLE metadata_table_vt (
    id serial PRIMARY KEY,
    table_name VARCHAR(250) UNIQUE NOT NULL,
    table_number INT NOT NULL,
    metadata_table_main_id INT, 
    FOREIGN KEY (metadata_table_main_id) REFERENCES metadata_table_main (id)
);

CREATE TABLE field ( 
    id serial PRIMARY KEY, 
    field_name VARCHAR(250) NOT NULL, 
    alias VARCHAR(250) NOT NULL, 
    table_id_main INT,
    table_id_vt INT

);

CREATE TABLE field_type ( 
    id serial PRIMARY KEY, 
    type_name VARCHAR(250) NOT NULL, 
    is_simple BOOLEAN NOT NULL,
    table_name VARCHAR(250) NOT NULL, 
    field_id INT NOT NULL
);

CREATE TABLE version_ref ( 
    id serial PRIMARY KEY, 
    metadata_id INT NOT NULL, 
    table_name VARCHAR(150), 
    FOREIGN KEY (metadata_id) REFERENCES metadata(id)
);

CREATE TABLE enums ( 
    id serial PRIMARY KEY, 
    enum_name VARCHAR(150) 
);

CREATE TABLE enums_value ( 
    id serial PRIMARY KEY, 
    alias VARCHAR(150),
    order_enum INT NOT NULL,
    enum_id INT NOT NULL,
    FOREIGN KEY (enum_id) REFERENCES enums(id)
);

-- CREATE TABLE IF NOT EXISTS version_431 ( 
--     id serial PRIMARY KEY, 
--     ref VARCHAR(32) NOT NULL, 
--     keywords TEXT, 
--     content TEXT NOT NULL,
--     version_number INT NOT NULL
-- );


-- SELECT t2.alias
-- FROM metadata_table_main as t1 
-- LEFT JOIN metadata as t2 
--     ON t1.metadata_id = t2.id
-- WHERE t1.table_number = 431;
-- SELECT 
--     t1.table_name AS main_table, 
--     t1.metadata_id AS metadata,  
--     t2.field_name AS field, 
--     t2.alias AS alias, 
--     t3.type_name AS type_name, 
--     t3.table_name AS field_table,
--     t5.alias AS field_object_alias 
-- FROM metadata_table_main AS t1 
--     LEFT JOIN field AS t2 
--         ON t1.id = t2.table_id_main 
--     LEFT JOIN field_type AS t3 
--         ON t2.id = t3.field_id
--     LEFT JOIN metadata_table_main AS t4
--         ON t3.table_name = t4.table_name
--     LEFT JOIN metadata as t5 
--         ON t4.metadata_id = t5.id 
-- WHERE 
--     t1.table_number = 431;
