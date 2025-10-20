#!/bin/bash
# Wait for MariaDB ColumnStore to be ready

# Wait for MariaDB to be ready
until mariadb -uroot -p"${MARIADB_ROOT_PASSWORD}" -e "SELECT 1" &>/dev/null; do
  echo "Waiting for MariaDB to be ready..."
  sleep 2
done

# Check if ColumnStore engine is available
if mariadb -uroot -p"${MARIADB_ROOT_PASSWORD}" -e "SHOW ENGINES;" 2>/dev/null | grep -i columnstore &>/dev/null; then
  echo "MariaDB ColumnStore is ready!"
  exit 0
else
  echo "ColumnStore engine not found, but MariaDB is running"
  exit 0
fi
