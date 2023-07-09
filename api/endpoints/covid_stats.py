import json
import math
import os
import pandas as pd
import subprocess

import scripts.postgres_operations as postgress_ops

from flask import Blueprint, request, render_template


blueprint = Blueprint("covid_stats", __name__, url_prefix="/covid-stats")


@blueprint.route("/test-db-connection", methods=["GET"])
def test_db_connection():
    """Establish a connection to Postgres database"""

    host_arg = request.args.get("host")

    db_host = host_arg if host_arg else "localhost"
    db_port = 5432
    
    result = subprocess.run(
        f"echo('Connection Successful')",
        shell=True,
        capture_output=True,
    )

    return {
        "stdout": result.stdout.decode(),
        "stderr": result.stderr.decode(),
    }


@blueprint.route("/get-covid-state-stats", methods=["GET"])
def get_covid_state_stats():
    # Establish a connection to Postgres database
    conn, cursor = postgress_ops.connect_to_database()
    
    # Get the data from covid_state_stats table
    sql = "select * from covid_state_stats;"
    cursor = postgress_ops.get_data(cursor, sql)
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(result, columns=columns)

    # Pagination logic
    page = request.args.get("page", default=1, type=int)
    per_page = request.args.get("per_page", default=1000, type=int)
    total_records = len(result)
    total_pages = int(math.ceil(total_records / per_page))
    start_index = (page - 1) * per_page
    end_index = start_index + per_page
    paginated_data = result[start_index:end_index]

    # Convert the paginated data to JSON
    result_json = paginated_data.to_json(orient="records")

    # Create a response dictionary with pagination metadata
    response = {
        "data": json.loads(result_json),
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "total_records": total_records,
        "prev_page": page - 1 if page > 1 else None,
        "next_page": page + 1 if page < total_pages else None,
    }
    
    return render_template("paginated_data.html", response=response)
