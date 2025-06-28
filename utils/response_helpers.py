import json
import os
from utils.gcs import upload_file_to_gcs
from utils.logging_setup import log_event

def save_raw_response(response, data_date, prefix):
    global original_json_response
    original_json_response = response

    filename = f"{prefix}_raw_response_{data_date}.json"
    with open(filename, 'w') as f:
        json.dump(response, f, indent=4)

    log_event("info", "saved_raw_response", file=filename)

    bucket = os.getenv("GCS_BUCKET_NAME", "xtra_point")
    local_path = os.path.abspath(filename)

    try:
        gcs_path = upload_file_to_gcs(bucket, local_path)
        log_event("info", "backup_to_gcs_complete", gcs_path=gcs_path)
    except Exception as e:
        log_event("error", "gcs_backup_failed", error=str(e), file=local_path)
