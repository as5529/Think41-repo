
CREATE TABLE users (
    id INT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    age INT,
    gender VARCHAR(50),
    state VARCHAR(255),
    street_address VARCHAR(255),
    postal_code VARCHAR(50),
    city VARCHAR(255),
    country VARCHAR(255),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    traffic_source VARCHAR(255),
    created_at TIMESTAMP
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    user_id INT,
    status VARCHAR(50),
    gender VARCHAR(50),
    created_at TIMESTAMP,
    returned_at TIMESTAMP,
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP,
    num_of_item INT,
    FOREIGN KEY (user_id) REFERENCES users(id)
);