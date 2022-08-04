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

-- CREATE TABLE version_{entity_id} ( 
--     version_id serial PRIMARY KEY, 
--     version__ref_id INT NOT NULL, 
--     tags TEXT, 
--     keywords TEXT, 
--     version_num INT NOT NULL, 
--     object_id VARCHAR(16), 
--     content TEXT, 
--     FOREIGN KEY (version__ref_id) REFERENCES version__ref(version__ref_id)
-- )

-- CREATE TABLE version_431 ( 
--     id serial PRIMARY KEY, 
--     ref VARCHAR(32) NOT NULL, 
--     keywords TEXT, 
--     content TEXT NOT NULL,
--     version_ref_id INT NOT NULL, 
--     FOREIGN KEY (version_ref_id) REFERENCES version_ref(id)
-- )


SELECT t1.table_name as name,t2.id as id, t3.id as ref_id, t3.table_name as ver_with_table
FROM metadata_table_main as t1
    LEFT JOIN metadata as t2 ON t1.metadata_id = t2.id
    LEFT JOIN version_ref as t3 ON t2.id = t3.metadata_id
WHERE
    t1.table_number = 431;

SELECT 
    t1.table_name as name,
    t2.id as id, 
    t3.id as ref_id, 
    t3.table_name as ver_with_table 
FROM metadata_table_main 
    LEFT JOIN metadata as t2 
        ON t1.metadata_id = t2.id 
    LEFT JOIN version_ref as t3 
    ON t2.id = t3.metadata_id 
WHERE t1.table_number = $1


select t1.table_name as table, t2.field_name as field, t3
from metadata_table_main as t1
left join field as t2 on t1.id = t2.table_id
left join field_type as t3 on t2.id = t3.field_id
where t1.table_number = 431 and t2.vt = 'f';
