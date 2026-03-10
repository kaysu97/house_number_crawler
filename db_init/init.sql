CREATE TABLE IF NOT EXISTS household_records (
    city VARCHAR(50) NOT NULL,
    township VARCHAR(50) NOT NULL,
    village VARCHAR(50),
    neighbor VARCHAR(50),
    address TEXT NOT NULL,
    record_date DATE NOT NULL,
    execution_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_city_township ON household_records(city, township);
