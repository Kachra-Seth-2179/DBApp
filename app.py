from flask import Flask, request,jsonify, render_template
from prometheus_client import Counter, generate_latest, CollectorRegistry
import os
import json
import mysql.connector
from datetime import datetime

app = Flask(__name__)

db_config = {
    'host': os.getenv('AZURE_MYSQL_HOST'),
    'user': os.getenv('AZURE_MYSQL_USER'),
    'password': os.getenv('AZURE_MYSQL_PASSWORD'),
    'database': os.getenv('AZURE_MYSQL_NAME')
}

# Initialize Prometheus metrics
registry = CollectorRegistry()
db_response_counter = Counter('db_response_counts', 'Count of responses from logs table', ['status'], registry=registry)


@app.route('/')
def index():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT DATABASE();")
        db_name = cursor.fetchone()
        return f"Connected to database: {db_name[0]}"
    except mysql.connector.Error as err:
        return f"Error: {err}"
    finally:
        if conn:
            cursor.close()
            conn.close()

@app.route('/uploadLogs', methods=['POST'])
def upload_logs():
    if 'file' not in request.files:
        return "No file part", 400

    file = request.files['file']
    if file.filename == '':
        return "No selected file", 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Create the table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            time DATETIME,
            remote_ip VARCHAR(45),
            remote_user VARCHAR(255),
            request TEXT,
            response INT,
            bytes INT,
            referrer TEXT,
            agent TEXT
        );
        """
        cursor.execute(create_table_query)

        batch_size = 1000
        batch_data = []
        
        for line in file:
            log_entry = json.loads(line.strip())
            log_time = datetime.strptime(log_entry['time'], "%d/%b/%Y:%H:%M:%S %z")

            # Prepare the log data
            batch_data.append((
                log_time,
                log_entry['remote_ip'],
                log_entry['remote_user'],
                log_entry['request'],
                log_entry['response'],
                log_entry['bytes'],
                log_entry['referrer'],
                log_entry['agent']
            ))

            # Insert in batches 
            if len(batch_data) >= batch_size:
                insert_query = """
                INSERT INTO logs (time, remote_ip, remote_user, request, response, bytes, referrer, agent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(insert_query, batch_data)
                batch_data = []

        # Insert any remaining data
        if batch_data:
            insert_query = """
            INSERT INTO logs (time, remote_ip, remote_user, request, response, bytes, referrer, agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_query, batch_data)

        conn.commit()
        return "Logs successfully uploaded", 200

    except mysql.connector.Error as err:
        return f"Error: {err}", 500
    except json.JSONDecodeError:
        return "Invalid log format", 400
    finally:
        if conn:
            cursor.close()
            conn.close()

@app.route('/upload', methods=['GET', 'POST'])
def upload_logs_page():
    if request.method == 'POST':
        if 'file' not in request.files:
            return "No file part", 400

        file = request.files['file']
        if file.filename == '':
            return "No selected file", 400
        return upload_logs()

    return render_template('upload.html')


@app.route('/<int:response_code>', methods=['GET'])
def get_last_logs(response_code):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        query = """
        SELECT * FROM logs
        WHERE response = %s
        ORDER BY time DESC
        LIMIT 10
        """
        cursor.execute(query, (response_code,))
        rows = cursor.fetchall()

        # Format the response
        logs = []
        for row in rows:
            logs.append({
                'id': row[0],
                'time': row[1].isoformat(),
                'remote_ip': row[2],
                'remote_user': row[3],
                'request': row[4],
                'response': row[5],
                'bytes': row[6],
                'referrer': row[7],
                'agent': row[8]
            })

        return jsonify(logs), 200

    except mysql.connector.Error as err:
        return f"Error: {err}", 500
    finally:
        if conn:
            cursor.close()
            conn.close()


@app.route('/search', methods=['GET', 'POST'])
def search_logs():
    logs = []
    if request.method == 'POST':
        response_code = request.form.get('response_code')
        if response_code:
            try:
                conn = mysql.connector.connect(**db_config)
                cursor = conn.cursor()
                query = """
                SELECT * FROM logs
                WHERE response = %s
                ORDER BY time DESC
                LIMIT 10
                """
                cursor.execute(query, (response_code,))
                logs = cursor.fetchall()
            except mysql.connector.Error as err:
                return f"Error: {err}", 500
            finally:
                if conn:
                    cursor.close()
                    conn.close()
    else :
        try:
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()
            query = """
            SELECT * FROM logs
            ORDER BY time DESC
            LIMIT 10
            """
            cursor.execute(query)
            logs = cursor.fetchall()
        except mysql.connector.Error as err:
            return f"Error: {err}", 500
        finally:
            if conn:
                cursor.close()
                conn.close()
    return render_template('search.html', logs=logs)

@app.route('/metrics', methods=['GET'])
def metrics():
    return generate_latest(registry), 200

@app.route('/metrics_from_db', methods=['GET'])
def get_metrics_from_db():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Query to count responses
        query = """
        SELECT response, COUNT(*) FROM logs GROUP BY response;
        """
        cursor.execute(query)
        results = cursor.fetchall()

        # Update counters based on the query results
        for response_code, count in results:
            db_response_counter.labels(status=str(response_code))._value.set(count)

        return jsonify({"message": "Metrics updated successfully"}), 200

    except mysql.connector.Error as err:
        return jsonify({"error": f"Database error: {err}"}), 500
    finally:
        if conn:
            cursor.close()
            conn.close()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
