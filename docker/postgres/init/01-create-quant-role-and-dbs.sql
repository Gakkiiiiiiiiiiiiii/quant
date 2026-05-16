DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'quant') THEN
        CREATE ROLE quant LOGIN PASSWORD 'quantpass';
    END IF;
END
$$;

SELECT 'CREATE DATABASE quant_backtest OWNER quant'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'quant_backtest')\gexec

SELECT 'CREATE DATABASE quant_paper OWNER quant'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'quant_paper')\gexec

SELECT 'CREATE DATABASE quant_live OWNER quant'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'quant_live')\gexec
