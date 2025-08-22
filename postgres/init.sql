CREATE TABLE parts (
    part_id UUID PRIMARY KEY,
    part_name TEXT,
    supplier TEXT,
    price NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE audit_log (
    id SERIAL PRIMARY KEY,
    action TEXT,
    part_id UUID,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
