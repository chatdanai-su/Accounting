import datetime
import io
import time
import random
import subprocess
from flask import Flask, request, render_template, redirect, url_for, flash, Response
from werkzeug.utils import secure_filename
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

# ================= CONFIG =================
SERVICE_ACCOUNT_FILE = "/home/dockeruser/account/credentials/power-query-467605-1e94c7e80abc.json"
ROOT_FOLDER_ID = "1lC28KVri3NzJ-XRuKcDYxl2-QocoUqS4"  # Shared Drive Root
SCOPES = ["https://www.googleapis.com/auth/drive"]

app = Flask(__name__)
app.secret_key = "supersecretkey"

# Auth Google Drive
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build("drive", "v3", credentials=creds)

# ================= HELPERS =================
def safe_execute(request, retries=3, delay=2):
    """ Wrapper for API calls with retry logic. """
    for attempt in range(retries):
        try:
            return request.execute()
        except (HttpError, BrokenPipeError, ConnectionResetError) as e:
            if attempt < retries - 1:
                wait = delay * (2 ** attempt) + random.random()
                time.sleep(wait)
                continue
            else:
                raise

def get_or_create_folder(name, parent_id):
    """ Gets a folder ID or creates it if it doesn't exist. """
    query = (
        f"'{parent_id}' in parents and trashed=false and "
        f"mimeType='application/vnd.google-apps.folder' and name='{name}'"
    )
    results = safe_execute(
        drive_service.files().list(
            q=query,
            fields="files(id, name)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        )
    )
    files = results.get("files", [])
    if files:
        return files[0]["id"]

    file_metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = safe_execute(
        drive_service.files().create(
            body=file_metadata,
            fields="id",
            supportsAllDrives=True
        )
    )
    return folder.get("id")

# ================= ROUTES =================
@app.route("/", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        companies = request.form.getlist("company[]")
        banks = request.form.getlist("bank[]")

        if not companies or not banks:
            flash("❌ Please select company and bank")
            return redirect(request.url)

        today = datetime.date.today()
        year_folder = get_or_create_folder(str(today.year), ROOT_FOLDER_ID)
        month_folder = get_or_create_folder(today.strftime("%m"), year_folder)

        # ✨ We will stream upload status to a dedicated status page
        # This part just handles the upload logic quickly.
        for idx, (company, bank) in enumerate(zip(companies, banks)):
            company_folder = get_or_create_folder(company, month_folder)
            bank_folder = get_or_create_folder(bank, company_folder)

            files = request.files.getlist(f"files{idx}")
            for file in files:
                if file:
                    filename = secure_filename(file.filename)
                    file_bytes = io.BytesIO(file.read())
                    media = MediaIoBaseUpload(file_bytes, mimetype=file.mimetype, resumable=True)
                    file_metadata = {"name": filename, "parents": [bank_folder]}
                    try:
                        safe_execute(
                            drive_service.files().create(
                                body=file_metadata,
                                media_body=media,
                                fields="id",
                                supportsAllDrives=True
                            )
                        )
                    except Exception as e:
                        flash(f"❌ Upload failed for {filename}: {e}")
                        return redirect(request.url)
        
        flash("✅ All files uploaded successfully. Now running the accounting script...")
        # ♻️ MODIFIED: Instead of running the script here, redirect to the status page.
        return redirect(url_for("status_page"))

    return render_template("index.html")

# ✨ NEW: A new page to show the status in real-time
@app.route("/status")
def status_page():
    """Renders the page that will display the streaming logs."""
    return render_template("status.html")

# ✨ NEW: The endpoint that streams the script's output
@app.route("/stream-logs")
def stream_logs():
    def generate_logs():
        """Runs the script and yields its output line by line."""
        try:
            # Use Popen to run the script as a background process
            process = subprocess.Popen(
                ["/home/dockeruser/sw_rate/.venv/bin/python", "/home/dockeruser/account/Accounting.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, # Combine stdout and stderr
                text=True, # Decode output as text
                bufsize=1  # Line-buffered
            )

            # Read the output line by line as it comes in
            for line in iter(process.stdout.readline, ''):
                # Format for Server-Sent Events (SSE)
                yield f"data: {line.strip()}\n\n"
                time.sleep(0.1) # Small delay to allow browser to render

            process.stdout.close()
            return_code = process.wait()
            
            if return_code == 0:
                yield "data: \n\n"
                yield "data: 🎉 Process complete! You can close this window.\n\n"
            else:
                yield "data: \n\n"
                yield f"data: ❌ Script failed with return code {return_code}.\n\n"

        except Exception as e:
            yield f"data: ⚠️ A critical error occurred: {str(e)}\n\n"
        
        # Send a special message to signal the end of the stream
        yield "data: [DONE]\n\n"

    # Return a streaming response
    return Response(generate_logs(), mimetype='text/event-stream')


# ================= MAIN =================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)