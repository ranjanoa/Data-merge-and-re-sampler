import webbrowser
from waitress import serve
from samplev2 import app  # Import the app instance from your original file
import threading

# Define host and port
HOST = '127.0.0.1'
PORT = 5000

def open_browser():
    """Opens the default web browser to the application's URL."""
    webbrowser.open_new(f"http://{HOST}:{PORT}")

if __name__ == '__main__':
    print(f"Starting server at http://{HOST}:{PORT}")
    # Run the browser opening in a separate thread to avoid blocking the server start
    threading.Timer(1.25, open_browser).start()
    # Use a production-grade WSGI server like waitress
    serve(app, host=HOST, port=PORT)