from utils.logging_setup import log_event
from utils.gcs import upload_file_to_gcs
from runtime_config import load_runtime_config


RUNTIME_CONFIG = load_runtime_config()

def save_raw_response(response, data_date, prefix="raw"):
    import os
    import json

    # === Skip if empty ===
    if not response:
        log_event("warning", "empty_response_skipped", prefix=prefix, data_date=data_date)
        return

    try:
        # Save to local temp file
        filename = f"{prefix}_{data_date}.json"
        with open(filename, 'w') as f:
            json.dump(response, f, indent=4)

        # Upload to GCS
        gcs_path = upload_file_to_gcs(
            RUNTIME_CONFIG.raw_response_bucket,
            filename,
        )
        log_event("info", "backup_to_gcs_complete", gcs_path=gcs_path)

        # Optional: delete local temp file after upload
        os.remove(filename)

    except Exception as e:
        log_event("error", "gcs_upload_failed", error=str(e), data_date=data_date)
