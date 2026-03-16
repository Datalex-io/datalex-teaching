# TP — Connect Flask to AWS S3 and Display Images

In this lab you will extend your Flask application deployed on EC2 by connecting it to an **AWS S3 bucket**.

Your Flask application will retrieve images stored in S3 and display them on a new route:

```
/pictures
```

Architecture of the application:

```
Browser
   |
   v
Flask App (EC2)
   |
   v
AWS S3 Bucket
```

---

# Step 1 — Create an S3 Bucket

Go to the AWS Console.

Navigate to:

```
S3 → Create bucket
```

Configuration:

```
Bucket name: flask-images-yourname
Region: same region as your EC2 instance
```

Create the bucket.

---

# Step 2 — Upload Images

Open your bucket and upload **3 to 5 images**.

Example:

```
cat.jpg
dog.jpg
mountain.jpg
beach.jpg
```

These images will later be displayed by the Flask application.

---

# Step 3 — Allow Public Access to Images

Open your bucket.

Go to:

```
Permissions → Bucket Policy
```

Add the following policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicRead",
      "Effect": "Allow",
      "Principal": "*",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::flask-images-yourname/*"]
    }
  ]
}
```

Your images are now accessible using URLs like:

```
https://flask-images-yourname.s3.amazonaws.com/cat.jpg
```

Test one image in your browser.

---

# Step 4 — Install the AWS Python SDK

Connect to your EC2 instance.

```
ssh -i key.pem ubuntu@YOUR_PUBLIC_IP
```

Activate your virtual environment:

```
source venv/bin/activate
```

Install `boto3`:

```
pip install boto3
```

`boto3` is the AWS SDK for Python that allows applications to interact with AWS services.

---

# Step 5 — Give EC2 Permission to Access S3

Instead of using AWS credentials, we will attach an **IAM role** to the EC2 instance.

Go to:

```
IAM → Roles → Create role
```

Configuration:

```
Trusted entity: EC2
Permissions: AmazonS3ReadOnlyAccess
Role name: EC2-S3-Flask-Role
```

Then attach the role to your EC2 instance:

```
EC2 → Instances → Select your instance
Actions → Security → Modify IAM Role
```

Attach:

```
EC2-S3-Flask-Role
```

Your EC2 instance can now access S3 securely.

---

# Step 6 — Update the Flask Application

Open your Flask application file:

```
app.py
```

Import boto3:

```python
import boto3
from flask import Flask, render_template

app = Flask(__name__)

s3 = boto3.client("s3")

BUCKET_NAME = "flask-images-yourname"
```

---

# Step 7 — Retrieve Images from S3

Add a function to list images stored in the bucket.

```python
def get_images():

    response = s3.list_objects_v2(Bucket=BUCKET_NAME)

    images = []

    for obj in response.get("Contents", []):
        url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{obj['Key']}"
        images.append(url)

    return images
```

---

# Step 8 — Create the `/pictures` Route

Add a new route to your Flask application.

```python
@app.route("/pictures")
def pictures():

    images = get_images()

    return render_template("pictures.html", images=images)
```

---

# Step 9 — Create the HTML Template

Create a directory if it does not exist:

```
templates/
```

Create the file:

```
templates/pictures.html
```

Add the following content:

```html
<!DOCTYPE html>
<html>
<head>
<title>S3 Pictures</title>
</head>

<body>

<h1>Pictures from S3</h1>

<div>

{% for img in images %}

<img src="{{ img }}" width="400">

{% endfor %}

</div>

</body>
</html>
```

---

# Step 10 — Restart Your Application

Restart your Flask application:

```
sudo systemctl restart helloworld
```

---

# Step 11 — Test the New Route

Open in your browser:

```
http://YOUR_PUBLIC_IP/pictures
```

You should now see the images stored in your **S3 bucket** displayed on the page.

---

# Expected Result

Your Flask application now exposes two routes:

```
/
Basic Flask page

/pictures
Gallery of images stored in S3
```

Architecture:

```
User → EC2 (Flask App) → S3 Bucket
```

You have built a simple **cloud application using compute and storage services**.
