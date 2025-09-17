#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER hive_user WITH PASSWORD 'hive_password';
    CREATE DATABASE hive_db;
    GRANT ALL PRIVILEGES ON DATABASE hive_db TO hive_user;
    \c hive_db "$POSTGRES_USER";
    GRANT ALL ON SCHEMA public TO hive_user;

    CREATE USER keycloak_user WITH PASSWORD 'keycloak_password';
    CREATE DATABASE keycloak;
    GRANT ALL PRIVILEGES ON DATABASE keycloak TO keycloak_user;
    \c keycloak "$POSTGRES_USER";
    GRANT ALL ON SCHEMA public TO keycloak_user;

    CREATE USER test_user WITH PASSWORD 'test_password';
    CREATE DATABASE testdb;
    GRANT ALL PRIVILEGES ON DATABASE testdb TO test_user;
    \c testdb "$POSTGRES_USER";
    GRANT ALL ON SCHEMA public TO test_user;

    CREATE USER trino_user WITH PASSWORD 'trino_password';
    CREATE DATABASE datalake;
    CREATE DATABASE workspace;

    GRANT ALL PRIVILEGES ON DATABASE datalake TO trino_user;
    GRANT ALL PRIVILEGES ON DATABASE workspace TO trino_user;
    \c datalake "$POSTGRES_USER";

    CREATE SCHEMA IF NOT EXISTS hr;
    CREATE SCHEMA IF NOT EXISTS logistics;
    CREATE SCHEMA IF NOT EXISTS sales;
    GRANT ALL ON SCHEMA hr TO trino_user;
    GRANT ALL ON SCHEMA logistics TO trino_user;
    GRANT ALL ON SCHEMA sales TO trino_user;

    CREATE TABLE hr.employees
    (
        id VARCHAR,
        firstname VARCHAR,
        lastname VARCHAR,
        email VARCHAR,
        phonenumber VARCHAR
    );
    \copy hr.employees from /postgres-data/employees.csv DELIMITER ',' CSV HEADER;

    CREATE TABLE hr.employee_territories
    (
        EmployeeId VARCHAR,
        TerritoryId VARCHAR
    );
    \copy hr.employee_territories from /postgres-data/employee_territories.csv DELIMITER ',' CSV HEADER;

    CREATE TABLE logistics.shippers
    (
        Id VARCHAR,
        CompanyName VARCHAR,
        Phone VARCHAR
    );
    \copy logistics.shippers from /postgres-data/shippers.csv DELIMITER ',' CSV HEADER;

    CREATE TABLE logistics.territories
    (
        Id VARCHAR,
        TerritoryDescription VARCHAR,
        RegionId VARCHAR
    );
    \copy logistics.territories from /postgres-data/territories.csv DELIMITER ',' CSV HEADER;

    CREATE TABLE logistics.regions
    (
        Id VARCHAR,
        RegionDescription VARCHAR
    );
    \copy logistics.regions from /postgres-data/regions.csv DELIMITER ',' CSV HEADER;

    CREATE TABLE logistics.suppliers
    (
        Id VARCHAR,
        CompanyName VARCHAR,
        ContactName VARCHAR,
        Address VARCHAR,
        City VARCHAR,
        PostalCode VARCHAR,
        Country VARCHAR,
        Phone VARCHAR
    );
    \copy logistics.suppliers from /postgres-data/suppliers.csv DELIMITER ',' CSV HEADER;

    CREATE TABLE sales.orders
    (
        Id VARCHAR,
        CustomerId VARCHAR,
        EmployeeId VARCHAR,
        OrderDate VARCHAR
    );
    \copy sales.orders from /postgres-data/orders.csv DELIMITER ',' CSV HEADER;

    CREATE TABLE sales.products
    (
        Id VARCHAR,
        ProductName VARCHAR,
        SupplierId VARCHAR,
        QuantityPerUnit VARCHAR
    );
    \copy sales.orders from /postgres-data/orders.csv DELIMITER ',' CSV HEADER;

    CREATE TABLE sales.customers
    (
        Id VARCHAR,
        CompanyName VARCHAR,
        ContactName VARCHAR,
        ContactTitle VARCHAR,
        Address VARCHAR,
        City VARCHAR,
        Region VARCHAR,
        PostalCode VARCHAR,
        Country VARCHAR,
        Phone VARCHAR
    );
    \copy sales.customers from /postgres-data/customers.csv DELIMITER ',' CSV HEADER;

    CREATE TABLE sales.customer_markets
    (
        Id VARCHAR,
        type_name VARCHAR,
        ContactName VARCHAR,
        ContactTitle VARCHAR,
        Address VARCHAR,
        City VARCHAR,
        Region VARCHAR,
        PostalCode VARCHAR,
        Country VARCHAR,
        Phone VARCHAR
    );
    \copy sales.customer_markets from /postgres-data/customer_markets.csv DELIMITER ',' CSV HEADER;

    CREATE TABLE sales.customer_demographics
    (
        Id VARCHAR,
        CustomerDesc VARCHAR
    );
    \copy sales.customer_demographics from /postgres-data/customer_demographics.csv DELIMITER ',' CSV HEADER;

    GRANT SELECT ON ALL TABLES IN SCHEMA hr TO trino_user;
    GRANT SELECT ON ALL TABLES IN SCHEMA sales TO trino_user;
    GRANT SELECT ON ALL TABLES IN SCHEMA logistics TO trino_user;

    CREATE DATABASE warehouse;
    GRANT ALL PRIVILEGES ON DATABASE warehouse TO trino_user;
    \c warehouse "$POSTGRES_USER";

    CREATE SCHEMA IF NOT EXISTS bronze;

    CREATE TABLE bronze.customers
    (
        Id VARCHAR,
        CompanyName VARCHAR,
        ContactName VARCHAR,
        ContactTitle VARCHAR
    );

    GRANT ALL ON SCHEMA bronze TO trino_user;
    GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO trino_user;

EOSQL