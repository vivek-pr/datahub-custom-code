DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'public') THEN
        PERFORM 1;
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(64) NOT NULL,
    last_name VARCHAR(64) NOT NULL,
    email TEXT,
    signup_date DATE DEFAULT CURRENT_DATE,
    notes TEXT
);

INSERT INTO customers (customer_id, first_name, last_name, email, signup_date, notes) VALUES
    (1, 'Alice', 'Anderson', 'alice@example.com', '2024-01-15', 'Loyal customer'),
    (2, 'Bob', 'Brown', 'bob@example.com', '2024-02-20', NULL),
    (3, 'Carlos', 'Chan', 'carlos@example.com', '2024-03-05', 'Prefers SMS contact')
ON CONFLICT (customer_id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    email = EXCLUDED.email,
    signup_date = EXCLUDED.signup_date,
    notes = EXCLUDED.notes;

CREATE TABLE IF NOT EXISTS addresses (
    address_id INT PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(customer_id),
    street VARCHAR(128) NOT NULL,
    city VARCHAR(64) NOT NULL,
    state VARCHAR(32) NOT NULL,
    postal_code VARCHAR(16) NOT NULL,
    notes TEXT
);

INSERT INTO addresses (address_id, customer_id, street, city, state, postal_code, notes) VALUES
    (1, 1, '100 Main St', 'Metropolis', 'CA', '90210', 'Mail drop-off preferred'),
    (2, 2, '200 Pine Ave', 'Gotham', 'NY', '10001', NULL),
    (3, 3, '300 Lake Blvd', 'Star City', 'WA', '98101', 'Has gate code 1234')
ON CONFLICT (address_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    street = EXCLUDED.street,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    postal_code = EXCLUDED.postal_code,
    notes = EXCLUDED.notes;

CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(customer_id),
    order_date DATE NOT NULL,
    status VARCHAR(32) NOT NULL,
    special_instructions TEXT,
    total_cents INT NOT NULL
);

INSERT INTO orders (order_id, customer_id, order_date, status, special_instructions, total_cents) VALUES
    (1, 1, '2024-04-01', 'SHIPPED', 'Leave at side door', 2599),
    (2, 2, '2024-04-05', 'PROCESSING', NULL, 1299),
    (3, 3, '2024-04-07', 'DELIVERED', 'Call on arrival', 4899)
ON CONFLICT (order_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    order_date = EXCLUDED.order_date,
    status = EXCLUDED.status,
    special_instructions = EXCLUDED.special_instructions,
    total_cents = EXCLUDED.total_cents;
