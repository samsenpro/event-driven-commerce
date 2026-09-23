#!/bin/sh
# Crea una base de datos y un usuario propios por servicio. Cada usuario solo puede conectarse a
# su base de datos: ningún servicio puede leer las tablas de otro.
set -e

create_service_database() {
    database="$1"
    user="$2"
    password="$3"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
        CREATE USER ${user} WITH PASSWORD '${password}';
        CREATE DATABASE ${database} OWNER ${user};
        REVOKE ALL ON DATABASE ${database} FROM PUBLIC;
EOSQL
}

create_service_database auth_db auth_service "$AUTH_DB_PASSWORD"
create_service_database order_db order_service "$ORDER_DB_PASSWORD"
create_service_database inventory_db inventory_service "$INVENTORY_DB_PASSWORD"
create_service_database payment_db payment_service "$PAYMENT_DB_PASSWORD"
create_service_database notification_db notification_service "$NOTIFICATION_DB_PASSWORD"
create_service_database shipping_db shipping_service "$SHIPPING_DB_PASSWORD"
