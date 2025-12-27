import obd
import csv
import time
from datetime import datetime
import psycopg2
from psycopg2 import sql

#--------------------------------------------------------
# Database Configuration
#--------------------------------------------------------
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": 5432
}

#--------------------------------------------------------
# 1️⃣ Connect to PostgreSQL
#--------------------------------------------------------
try:
    pg_conn = psycopg2.connect(**DB_CONFIG)
    pg_cursor = pg_conn.cursor()
    print("✅ Connected to PostgreSQL database")
except Exception as e:
    print(f"❌ Failed to connect to PostgreSQL: {e}")
    exit()

#--------------------------------------------------------
# 2️⃣ Connect to the ELM327 Adapter (Update COM port if needed)
#--------------------------------------------------------
connection = obd.OBD("COM26")  # Windows example
# Linux example: connection = obd.OBD("/dev/ttyUSB0")

if connection.status() != obd.OBDStatus.CAR_CONNECTED:
    print("❌ ECU not connected! Turn the ignition ON and try again.")
    pg_cursor.close()
    pg_conn.close()
    exit()

#--------------------------------------------------------
# 3️⃣ Get Supported PIDs from ECU
#--------------------------------------------------------
supported = connection.supported_commands

#--------------------------------------------------------
# 4️⃣ Create PostgreSQL Table with Dynamic Columns
#--------------------------------------------------------
# Create column definitions based on supported PIDs
columns = ["id SERIAL PRIMARY KEY", "timestamp TIMESTAMP NOT NULL"]

for cmd in supported:
    # Convert command name to valid PostgreSQL column name
    col_name = cmd.name.lower().replace(" ", "_").replace("-", "_")
    columns.append(f"{col_name} TEXT")

# Create table SQL
create_table_query = f"""
CREATE TABLE IF NOT EXISTS obd_telemetry (
    {', '.join(columns)}
);
"""

try:
    pg_cursor.execute(create_table_query)
    pg_conn.commit()
    print("✅ Table 'obd_telemetry' created/verified")
except Exception as e:
    print(f"❌ Error creating table: {e}")
    pg_cursor.close()
    pg_conn.close()
    exit()

#--------------------------------------------------------
# 5️⃣ Setup CSV File for Backup Logging
#--------------------------------------------------------
filename = "hyundai_i20_live_obd_data.csv"
csv_file = open(filename, "w", newline="")
csv_writer = csv.writer(csv_file)

header = ["timestamp"] + [cmd.name for cmd in supported]
csv_writer.writerow(header)

print("\n📡 LIVE DATA LOGGING STARTED")
print("📁 CSV File:", filename)
print("🗄️  PostgreSQL Table: obd_telemetry")
print("Press CTRL + C to stop...\n")

#--------------------------------------------------------
# 6️⃣ Log & Print Data Continuously
#--------------------------------------------------------
try:
    while True:
        timestamp = datetime.now()
        row = [timestamp.strftime("%Y-%m-%d %H:%M:%S")]
        db_values = [timestamp]

        print("--------------------------------------------------")
        print(f"⏱ Timestamp: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")

        for cmd in supported:
            response = connection.query(cmd)

            if response.is_null():
                value = "NA"
            else:
                value = str(response.value)

            row.append(value)
            db_values.append(value)

            # 🖥️ Print each live value on the terminal
            print(f"{cmd.name:30} => {value}")

        # ✍️ Save row to CSV file
        csv_writer.writerow(row)

        # 💾 Insert into PostgreSQL
        try:
            # Build column names for INSERT query
            col_names = ["timestamp"] + [cmd.name.lower().replace(" ", "_").replace("-", "_") for cmd in supported]

            # Create placeholders for values (%s, %s, %s, ...)
            placeholders = ", ".join(["%s"] * len(db_values))

            # Build INSERT query
            insert_query = f"""
            INSERT INTO obd_telemetry ({', '.join(col_names)})
            VALUES ({placeholders})
            """

            pg_cursor.execute(insert_query, db_values)
            pg_conn.commit()
            print("✅ Data saved to PostgreSQL")

        except Exception as e:
            print(f"❌ Error inserting into database: {e}")
            pg_conn.rollback()

        print("--------------------------------------------------\n")
        time.sleep(1)  # Adjust logging speed here (1 second)

except KeyboardInterrupt:
    print("\n🛑 Logging stopped by user.")
    csv_file.close()
    pg_cursor.close()
    pg_conn.close()
    print(f"📦 CSV data saved to: {filename}")
    print("🗄️  Database connection closed.")