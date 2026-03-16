from flask import Flask, render_template
import boto3

app = Flask(__name__)


s3 = boto3.client("s3")

BUCKET_NAME = "esme-flask-app"

def get_images():

    response = s3.list_objects_v2(Bucket=BUCKET_NAME)

    images = []

    for obj in response.get("Contents", []):
        url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{obj['Key']}"
        images.append(url)

    return images

@app.route("/pictures")
def pictures():

    images = get_images()

    return render_template("pictures.html", images=images)


@app.route('/')
def hello_world():
        return 'Hello World!'

if __name__ == "__main__":
        app.run()