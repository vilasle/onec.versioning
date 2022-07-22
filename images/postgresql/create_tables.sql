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
    vt BOOLEAN NOT NULL,
    table_id INT NOT NULL
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