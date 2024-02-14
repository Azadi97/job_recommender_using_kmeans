# REST API to expose data from www.marikhpay.com
# in order for SWAGGER UI to work, need to modify the openapi.yaml file in flask/static directory accordingly

import json
import math
from flask import Flask, jsonify, request
import pymysql
from config import DB_CONFIG
from flask_basicauth import BasicAuth 
from flask_swagger_ui import get_swaggerui_blueprint


app = Flask(__name__)
app.config.from_file("flask_config.json", load=json.load)
auth = BasicAuth(app)

swaggerui_blueprint = get_swaggerui_blueprint(
    base_url='/docs1',
    api_url='/static/openapi.yaml',
)
app.register_blueprint(swaggerui_blueprint)

@app.route('/jobs/<string:job_id>')
@auth.required

def profile(job_id):

    # conn = pymysql.connect(**DB_CONFIG)
    
    
    conn = pymysql.connect(
        host="198.38.84.178",
        user="kweekly_ironhack", 
        password="ironhack",
        database="kweekly_ironhack",
        cursorclass=pymysql.cursors.DictCursor
    )

    with conn.cursor() as cursor:
            cursor.execute("""SELECT ja.job_title, ja.location, ja.days_ago, ja.url
                            FROM job_announcement ja
                            WHERE ja.job_id = %s
                            ORDER BY ja.days_ago""", (job_id, ))

    job_announcements = cursor.fetchone()

    with conn.cursor() as cursor:
            cursor.execute("""SELECT js.*
                            FROM job_announcement ja
                            INNER JOIN job_skills js ON ja.job_id = js.job_id
                            WHERE ja.job_id = %s
                            ORDER BY ja.days_ago""", (job_id, ))

    job_skills = cursor.fetchone()
    job_announcements["skills"] = [s for s, v in job_skills.items() if v == 1]

    return jsonify(job_announcements)

MAX_PAGE_SIZE = 20




from flask import request

from flask import request, jsonify

@app.route('/jobs')
def all_jobs():
    # Parse query parameters for pagination
    page = request.args.get('page', default=1, type=int)
    items_per_page = request.args.get('items_per_page', default=10, type=int)

    conn = pymysql.connect(
        host="198.38.84.178",
        user="kweekly_ironhack", 
        password="ironhack",
        database="kweekly_ironhack",
        cursorclass=pymysql.cursors.DictCursor
    )

    # Calculate the starting index and ending index for pagination
    start_index = (page - 1) * items_per_page
    end_index = start_index + items_per_page

    with conn.cursor() as cursor:
        cursor.execute("""SELECT ja.job_id, ja.job_title, ja.location, ja.days_ago, ja.url
                        FROM job_announcement ja
                        ORDER BY ja.days_ago
                        LIMIT %s OFFSET %s""", (items_per_page, start_index))

    job_announcements = cursor.fetchall()

    # Fetch skills for each job in the job_announcements list
    for job in job_announcements:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT js.*
                            FROM job_announcement ja
                            INNER JOIN job_skills js ON ja.job_id = js.job_id
                            WHERE ja.job_id = %s
                            ORDER BY ja.days_ago""", (job['job_id'], ))

            job_skills = cursor.fetchone()

            # Check if job_skills is not None before iterating
            if job_skills:
                job["skills"] = [s for s, v in job_skills.items() if v == 1]
            else:
                job["skills"] = []

    # Calculate last page based on the total number of items and items per page
    total_items = len(job_announcements)
    last_page = (total_items + items_per_page - 1) // items_per_page

    # Build pagination details for the response
    next_page = f'/jobs?page={page+1}&items_per_page={items_per_page}' if page < last_page else None
    last_page_url = f'/jobs?page={last_page}&items_per_page={items_per_page}'

    return jsonify({
        'jobs': job_announcements,
        'next_page': next_page,
        'last_page': last_page_url,
    })















@app.route('/mpay')
@auth.required

def txn():
    userId = int(request.args.get('machine', 0))
    network = str(request.args.get('network', 'etisalat'))
    
    page = int(request.args.get('page', 0))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    
    page_size = min(page_size, MAX_PAGE_SIZE)

    conn = pymysql.connect(
        host="IP",
        user="DB_USER", 
        password="DB_PASS",
        database="DB_NAME",
        cursorclass=pymysql.cursors.DictCursor
    )

    with conn.cursor() as cursor:
        cursor.execute("""
                        SELECT * FROM kiosk_transaction
                        WHERE user_id = %s LIMIT %s OFFSET %s
                        """, (userId, page_size, page * page_size))
        transactions = cursor.fetchall()

        if network:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT * FROM kiosk_transaction
                WHERE telco_code = %s and user_id = %s ORDER BY id DESC LIMIT %s OFFSET %s
            """, (network, userId, page_size, page * page_size))
            transactions = cursor.fetchall()

    with conn.cursor() as total_count:
        total_count.execute("SELECT COUNT(*) AS total_count FROM kiosk_transaction")
        total = total_count.fetchone()
        last_page = math.ceil(total['total_count'] / page_size)

    return {
        'transactions' : transactions,
        'next_page': f'/mpay?machine={userId}&page={page+1}&page_size={page_size}',
        'last_page': f'/mpay?machine={userId}&page={last_page}&page_size={page_size}',
    }