CREATE TABLE IF NOT EXISTS staging.customer (
  tenant_id UUID,
  customer_name TEXT,
  customer_type TEXT,
  country TEXT,
  industry TEXT,
  contract_start_date DATE,
  account_owner TEXT,
  loaded_at TIMESTAMP DEFAULT NOW()
);