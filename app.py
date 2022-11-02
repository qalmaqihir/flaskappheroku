import os

from flask import Flask, request, render_template, redirect, url_for, send_from_directory
from werkzeug.utils import secure_filename
from datetime import datetime
from script import multiprocessing_requests_n_save



app = Flask(__name__)


ALLOWED_EXTENSIONS = set(['csv'])

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/")
def index():
    return redirect(url_for('upload'))

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        file = request.files['file']
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            # f'output/{datetime.now():%Y-%m-%d_%H-%M-%S}.csv'
            new_filename = f'{filename.split(".")[0]}_{datetime.now():%Y-%m-%d_%H-%M-%S}.csv'
            save_location = os.path.join('input', new_filename)
            file.save(save_location)

            output_file = multiprocessing_requests_n_save(save_location)
            # return send_from_directory('output', output_file)
            return redirect(url_for('download'))

    return render_template('upload.html')

@app.route('/download')
def download():
    return render_template('download.html', files=os.listdir('output'))

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory('output', filename)