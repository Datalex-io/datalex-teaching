# Run Flask App on AWS EC2 Instance
Install Python Virtualenv
```bash
sudo apt-get update
sudo apt-get install python3-venv
```
Activate the new virtual environment in a new directory

Create directory
```bash
mkdir helloworld
cd helloworld
```
Create the virtual environment
```bash
python3 -m venv venv
```
Activate the virtual environment
```bash
source venv/bin/activate
```
Install Flask
```bash
pip install Flask
```
Create a Simple Flask API
```bash
sudo vi app.py
```
```bash
// Add this to app.py
from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello_world():
	return 'Hello World!'

if __name__ == "__main__":
	app.run()
```
Verify if it works by running 
```bash
python app.py
```
Begin the Gunicorn WSGI server to host the Flask application.
When Flask is executed, the Werkzeug's development WSGI server is essentially launched, which directs requests from a web server.
Since Werkzeug is intended solely for development purposes, Gunicorn, a production-ready WSGI server, is used to serve the application.

Install Gunicorn using the below command:
```bash
pip install gunicorn
```
Run Gunicorn:
```bash
gunicorn -b 0.0.0.0:8000 app:app 
```
Gunicorn is running (Ctrl + C to exit gunicorn)!

Manage Gunicorn using systemd.
Systemd serves as a boot manager for Linux. It is employed to automatically restart Gunicorn if the EC2 instance restarts or encounters a reboot.
A <projectname>.service file is crafted in the /etc/systemd/system directory, outlining the actions to be taken regarding Gunicorn upon system reboot.
Three components are appended to the systemd Unit file — Unit, Service, and Install.

Unit — This segment provides a description of the project and any dependencies.
Service — Specifies the user/group under which the service should run. Additionally, it details information about executables and commands.
Install — Informs systemd about the timing during the boot process when this service should initiate.

With these instructions, proceed to create a unit file in the /etc/systemd/system directory.
	
```bash
sudo nano /etc/systemd/system/helloworld.service
```
Then add this into the file.
```bash
[Unit]
Description=Gunicorn instance for a simple hello world app
After=network.target
[Service]
User=ubuntu
Group=www-data
WorkingDirectory=/home/ubuntu/helloworld
ExecStart=/home/ubuntu/helloworld/venv/bin/gunicorn -b localhost:8000 app:app
Restart=always
[Install]
WantedBy=multi-user.target
```
Then enable the service:
```bash
sudo systemctl daemon-reload
sudo systemctl start helloworld
sudo systemctl enable helloworld
```
Check if the app is running with 
```bash
curl localhost:8000
```
Finally, Nginx is configured as a reverse-proxy to accept requests from users and route them to Gunicorn.

Install Nginx 
```bash
sudo apt install nginx
```
Start the Nginx service and navigate to the Public IP address of the EC2 instance in the browser to view the default Nginx landing page.
```bash
sudo systemctl start nginx
sudo systemctl enable nginx
```
Edit the default file in the sites-available folder.
```bash
sudo nano /etc/nginx/sites-available/default
```
Add the following code at the top of the file (below the default comments)
```bash
upstream flaskhelloworld {
    server 127.0.0.1:8000;
}
```
Add a proxy_pass to flaskhelloworld atlocation bellow existing location
```bash
location / {
    proxy_pass http://flaskhelloworld;
}
```
Restart Nginx 
```bash
sudo systemctl restart nginx
```
Navigate to the Public IP address of the EC2 instance in the browser to view the default Nginx landing page.