-- Initialize Finance Flow databases
CREATE USER finance WITH PASSWORD 'finance';
CREATE DATABASE financeflow OWNER finance;
CREATE DATABASE airflow OWNER airflow;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE financeflow TO finance;

-- Create airflow user if not exists
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
    CREATE USER airflow WITH PASSWORD 'airflow';
  END IF;
END
$$;
CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
