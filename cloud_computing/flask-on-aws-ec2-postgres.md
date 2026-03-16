# TP — Add PostgreSQL to Your Flask Application

In this lab you will extend your Flask application deployed on EC2 by adding a **PostgreSQL database**.

You will:

* install PostgreSQL on the EC2 instance
* create a database and a `users` table
* insert some sample users
* connect Flask to PostgreSQL
* create a new route `/list_users` to display users

Architecture:

```
Browser
   |
   v
Flask App (EC2)
   |
   v
PostgreSQL Database (EC2)
```

---

# Step 1 — Install PostgreSQL

Connect to your EC2 instance:

```
ssh -i key.pem ubuntu@YOUR_PUBLIC_IP
```

Install PostgreSQL:

```
sudo apt update
sudo apt install postgresql postgresql-contrib
```

Check that PostgreSQL is running:

```
sudo systemctl status postgresql
```

---

# Step 2 — Create a Database

Switch to the PostgreSQL user:

```
sudo -i -u postgres
```

Open the PostgreSQL shell:

```
psql
```

Create a database:

```
CREATE DATABASE flaskdb;
```

Create a user:

```
CREATE USER flaskuser WITH PASSWORD 'password';
```

Give privileges to the user:

```
GRANT ALL PRIVILEGES ON DATABASE flaskdb TO flaskuser;
```

Exit PostgreSQL:

```
\q
```

Exit the postgres user:

```
exit
```

---

# Step 3 — Create a Users Table

Reconnect to PostgreSQL:

```
sudo -i -u postgres
psql flaskdb
```

Create the table:

```
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);
```

Insert some users:

```
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com');
INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@example.com');
```

Verify the data:

```
SELECT * FROM users;
```

Exit PostgreSQL:

```
\q
exit
```

---

# Step 4 — Install Python PostgreSQL Driver

Activate your virtual environment:

```
source venv/bin/activate
```

Install the PostgreSQL driver:

```
pip install psycopg2-binary
```

---

# Step 5 — Connect Flask to PostgreSQL

Open your Flask application:

```
app.py
```

Add the following code:

```python
import psycopg2
from flask import Flask, jsonify

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        database="flaskdb",
        user="flaskuser",
        password="password"
    )
    return conn
```

---

# Step 6 — Create the `/list_users` Route

Add the following route:

```python
@app.route("/list_users")
def list_users():

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT id, name, email FROM users")
    rows = cur.fetchall()

    cur.close()
    conn.close()

    users = []

    for row in rows:
        users.append({
            "id": row[0],
            "name": row[1],
            "email": row[2]
        })

    return jsonify(users)
```

---

# Step 7 — Restart Your Application

Restart the Flask service:

```
sudo systemctl restart helloworld
```

---

# Step 8 — Test the New Route

Open your browser:

```
http://YOUR_PUBLIC_IP/list_users
```

Expected result:

```
[
  {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com"
  },
  {
    "id": 2,
    "name": "Bob",
    "email": "bob@example.com"
  },
  {
    "id": 3,
    "name": "Charlie",
    "email": "charlie@example.com"
  }
]
```

---

# Expected Result

Your Flask application now exposes three routes:

```
/
Basic Flask page

/pictures
Images loaded from S3

/list_users
Users stored in PostgreSQL
```

Architecture:

```
Browser
   |
   v
Flask (EC2)
   |
   +--> S3 Bucket
   |
   +--> PostgreSQL Database
```

You now have a simple **cloud application with compute, storage, and database services**.
