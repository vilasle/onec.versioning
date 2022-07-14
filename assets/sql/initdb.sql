CREATE TABLE entity ( 
    entity_id serial PRIMARY KEY, 
    entity_name VARCHAR(250) UNIQUE NOT NULL, 
    alias VARCHAR(250) NOT NULL
);


CREATE TABLE entity_vt ( 
    entity_vt_id serial PRIMARY KEY, 
    entity_id INT NOT NULL, 
    name VARCHAR(250) UNIQUE NOT NULL, 
    alias VARCHAR(250) NOT NULL, 
    FOREIGN KEY (entity_id) REFERENCES entity (entity_id)
);


CREATE TABLE field ( 
    field_id serial PRIMARY KEY, 
    entity_id INT, 
    entity_vt_id INT, 
    field_name VARCHAR(250) NOT NULL, 
    alias VARCHAR(250) NOT NULL, 
    FOREIGN KEY (entity_id) REFERENCES entity (entity_id)
);


CREATE TABLE field_vt ( 
    field_id serial PRIMARY KEY, 
    entity_id INT, 
    entity_vt_id INT, 
    field_name VARCHAR(250) NOT NULL, 
    alias VARCHAR(250) NOT NULL, 
    FOREIGN KEY (entity_vt_id) REFERENCES entity_vt (entity_vt_id)
);


CREATE TABLE version_ref ( 
    version_id serial PRIMARY KEY, 
    entity_id INT NOT NULL, 
    table_name VARCHAR(150), 
    FOREIGN KEY (entity_id) REFERENCES entity(entity_id)
);


CREATE TABLE version_ref_{entity_id} ( 
    version_id serial PRIMARY KEY, 
    entity_id INT NOT NULL, 
    tags TEXT, 
    keywords TEXT, 
    version_num INT NOT NULL, 
    object_id VARCHAR(16), 
    content TEXT, 
    FOREIGN KEY (entity_id) REFERENCES entity(entity_id)
);