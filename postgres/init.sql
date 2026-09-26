-- Set the search path
SET search_path TO parts_catalog, vendors_data, audit_logs;

-- Schemas
CREATE SCHEMA audit_logs;
CREATE SCHEMA vendors_data;
CREATE SCHEMA parts_catalog;

-- Roles Creation
CREATE ROLE admin_user WITH LOGIN PASSWORD 'PASSWORD';
CREATE ROLE parts_admin WITH LOGIN PASSWORD 'PASSWORD';
CREATE ROLE vendor_admin WITH LOGIN PASSWORD 'PASSWORD';
CREATE ROLE parts_reader WITH LOGIN PASSWORD 'PASSWORD';
CREATE ROLE vendor_reader WITH LOGIN PASSWORD 'PASSWORD';

-- Grants for roles
GRANT ALL PRIVILEGES ON DATABASE carparts TO admin_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA parts_catalog TO parts_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA parts_catalog GRANT ALL ON TABLES TO parts_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA vendors_data TO vendor_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA vendors_data GRANT ALL ON TABLES TO vendor_admin;
GRANT USAGE, CREATE ON SCHEMA parts_catalog TO parts_admin;
GRANT USAGE, CREATE ON SCHEMA vendors_data TO vendor_admin;
GRANT USAGE ON SCHEMA parts_catalog TO parts_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA parts_catalog TO parts_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA parts_catalog GRANT SELECT ON TABLES TO parts_reader;
GRANT USAGE ON SCHEMA vendors_data TO vendor_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA vendors_data TO vendor_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA vendors_data GRANT SELECT ON TABLES TO vendor_reader;
GRANT admin_user TO parts_admin, vendor_admin;
GRANT parts_admin TO parts_reader;
GRANT vendor_admin TO vendor_reader;

-- Extension for UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Tables Creation
CREATE TABLE vendors_data.vendors (
    vendor_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vendor_name VARCHAR(255) NOT NULL,
    website TEXT
);

CREATE TABLE parts_catalog.categories (
    category_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    category_name VARCHAR(100) UNIQUE NOT NULL,
    parent_category_id UUID REFERENCES parts_catalog.categories(category_id) -- Self-referencing for hierarchical categories
);

CREATE TABLE parts_catalog.brands (
    brand_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    brand_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE parts_catalog.compatibility (
    compatibility_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    model VARCHAR(100) NOT NULL,
    engine_code VARCHAR(50) NOT NULL,
    years VARCHAR(50) NOT NULL
);

CREATE TABLE parts_catalog.parts (
    part_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    part_number VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    category_id UUID REFERENCES parts_catalog.categories(category_id) ON DELETE SET NULL,
    brand_id UUID REFERENCES parts_catalog.brands(brand_id) ON DELETE SET NULL,
    compatibility_id UUID REFERENCES parts_catalog.compatibility(compatibility_id) ON DELETE SET NULL
);

CREATE TABLE vendors_data.prices (
    price_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    part_id UUID REFERENCES parts_catalog.parts(part_id) ON DELETE CASCADE,
    vendor_id UUID REFERENCES vendors_data.vendors(vendor_id),
    price_zar DECIMAL(10, 2) NOT NULL,
    price_date TIMESTAMP DEFAULT NOW()
);

CREATE TABLE audit_logs.change_history (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    changed_by TEXT NOT NULL,
    change_time TIMESTAMP DEFAULT NOW(),
    old_data JSONB,
    new_data JSONB
);
