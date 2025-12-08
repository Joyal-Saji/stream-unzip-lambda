
"""
Lambda 1: StreamUnzipFunction with IST millis
- Handles ZIP extraction, file upload to S3
- Records job details in MongoDB, stores created/updated times as IST millis
- Triggers Lambda 2 for validation
"""






import boto3
import zipfile
import io
import os
import json
from datetime import datetime, timedelta, timezone
import pymongo
from botocore.config import Config
import uuid

# --- IST time in millis ---
def get_ist_millis():
    utc_now = datetime.utcnow().replace(tzinfo=timezone.utc)
    ist_offset = timedelta(hours=5, minutes=30)
    ist_now = utc_now + ist_offset
    return int(ist_now.timestamp() * 1000)

# --- AWS Clients ---
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda', config=Config(max_pool_connections=100))
mongo_client = pymongo.MongoClient(os.environ['MONGO_URI'])
db = mongo_client['kjusys_db']
jobs_collection = db['processing_jobs']

VALIDATE_LAMBDA_NAME = os.environ.get('VALIDATE_LAMBDA_NAME', 'ValidateExcelFunction')
DELETE_ZIP_AFTER_SUCCESS = os.environ.get('DELETE_ZIP_AFTER_SUCCESS', 'true').lower() == 'true'

# --- Parse AWS Event ---
def parse_event(event):
    if 'Records' in event and len(event['Records']) > 0:
        record = event['Records'][0]
        if record.get('eventSource') == 'aws:s3':
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            try:
                from urllib.parse import unquote_plus
                key = unquote_plus(key)
            except:
                pass
            exam_code = 'UNKNOWN'
            course_code = 'UNKNOWN'
            if key.startswith('Answer_Scripts_Zip_Files/'):
                path = key[len('Answer_Scripts_Zip_Files/'):]
                parts = path.split('/')
                if len(parts) >= 3:
                    exam_code = parts[0]
                    course_code = parts[1]
            job_id = f"JOB-{str(uuid.uuid4())}"
            return {
                'jobId': job_id,
                's3Bucket': bucket,
                's3Key': key,
                'examCode': exam_code,
                'courseCode': course_code,
                'uploadedBy': 'S3_AUTO_TRIGGER',
                'source': 'S3_EVENT'
            }
    elif 'jobId' in event or 's3Key' in event:
        return {
            'jobId': event.get('jobId'),
            's3Bucket': event.get('s3Bucket') or event.get('bucket'),
            's3Key': event.get('s3Key'),
            'examCode': event.get('examCode', 'UNKNOWN'),
            'courseCode': event.get('courseCode', 'UNKNOWN'),
            'uploadedBy': event.get('uploadedBy', 'BACKEND'),
            'source': 'DIRECT_INVOCATION'
        }
    else:
        raise ValueError("Invalid event format")

# --- Junk File Detector ---
def is_junk_file(filename):
    if not filename:
        return True
    base_name = os.path.basename(filename)
    if not base_name:
        return True
    lower_name = base_name.lower()
    JUNK_NAMES = [
        '._', '.ds_store', '.spotlight-v100', '.trashes', '.fseventsd', '.temporaryitems', '.appledouble',
        '__macosx', 'thumbs.db', 'desktop.ini', 'iconcache.db', '~$', '.bak', '.lnk'
    ]
    if any(j in lower_name for j in JUNK_NAMES) or base_name.startswith('.') or base_name.endswith('~'):
        return True
    if '.Trash-' in filename or '/tmp/' in filename:
        return True
    if lower_name.endswith(('.swp', '.swo', '.log')) or 'cache' in lower_name:
        return True
    return False

def is_valid_pdf(filename, file_size):
    return filename.lower().endswith('.pdf') and file_size >= 1024

def is_valid_excel(filename, file_size):
    lower_name = filename.lower()
    return (lower_name.endswith('.xlsx') or lower_name.endswith('.xls')) and file_size >= 2048

# --- Delete ZIP file from S3 & log deleted time ---
def delete_zip_file(bucket, key, job_id):
    try:
        print(f"\n[LAMBDA 1] Deleting ZIP...")
        s3_client.delete_object(Bucket=bucket, Key=key)
        print(f"[LAMBDA 1] ✓ ZIP deleted")
        now_millis = get_ist_millis()
        jobs_collection.update_one(
            {"jobId": job_id},
            {"$set": {"originalZipDeleted": True, "zipDeletedAt_KJUSYSCommon_DateTime": now_millis}}
        )
        return True
    except Exception as e:
        print(f"[LAMBDA 1] ⚠ ZIP deletion failed: {str(e)}")
        return False

# --- Synchronously call Lambda 2 for validation ---
def invoke_lambda2_sync(payload):
    try:
        print(f"\n[LAMBDA 1] Invoking Lambda 2...")
        print(f"[LAMBDA 1] Payload: {json.dumps(payload)}")
        response = lambda_client.invoke(
            FunctionName=VALIDATE_LAMBDA_NAME,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        status_code = response.get('StatusCode', 0)
        if 'errorMessage' in response_payload:
            return {'success': False, 'error': response_payload.get('errorMessage')}
        if response_payload.get('statusCode', status_code) == 200:
            print(f"[LAMBDA 1] ✓ Lambda 2 completed")
            return {'success': True, 'response': response_payload}
        else:
            return {'success': False, 'error': response_payload.get('error', 'Unknown error')}
    except Exception as e:
        print(f"[LAMBDA 1] ✗ Exception: {str(e)}")
        return {'success': False, 'error': str(e)}

# --- Main Lambda 1 Handler ---
def lambda_handler(event, context):
    job_id = None
    bucket = None
    key = None
    lambda2_successful = False
    try:
        # 1. Parse event
        parsed = parse_event(event)
        job_id = parsed['jobId']
        bucket = parsed['s3Bucket']
        key = parsed['s3Key']
        exam_code = parsed['examCode']
        course_code = parsed['courseCode']
        uploaded_by = parsed['uploadedBy']
        event_source = parsed['source']

        now_millis = get_ist_millis()

        # 2. Create new job with createdAt (IST millis)
        jobs_collection.update_one(
            {"jobId": job_id},
            {"$setOnInsert": {
                "createdOn_KJUSYSCommon_DateTime": now_millis,
                "type": "ANSWER_SCRIPT_PROCESSING",
                "examCode_Examination_Text": exam_code,
                "courseCode_Examination_Text": course_code,
                "uploadedBy": uploaded_by,
                "eventSource": event_source,
                "status": "PENDING",
                "progress": 0
            }},
            upsert=True
        )

        # 3. Update job with unzipping started (update time IST millis)
        jobs_collection.update_one(
            {"jobId": job_id},
            {"$set": {
                "status": "UNZIPPING",
                "currentStep": "Extracting files",
                "progress": 10,
                "s3Bucket": bucket,
                "s3Key": key,
                "updatedOn_KJUSYSCommon_DateTime": now_millis
            }}
        )

        # 4. Download and extract ZIP from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        zip_bytes = response['Body'].read()
        zip_file = zipfile.ZipFile(io.BytesIO(zip_bytes))
        file_list = zip_file.namelist()
        total_entries = len(file_list)
        unzip_prefix = f"Answer_Scripts_Zip_Files/{exam_code}/{course_code}/unzipped/"
        pdf_files = []
        excel_file = None
        other_files = []
        skipped_files = []
        valid_files_processed = 0

        # 5. Iterate and upload extracted files
        for idx, file_name in enumerate(file_list):
            if file_name.endswith('/'): continue
            if is_junk_file(file_name):
                skipped_files.append({"fileName": file_name, "reason": "Junk"})
                continue
            try:
                file_data = zip_file.read(file_name)
            except Exception as e:
                skipped_files.append({"fileName": file_name, "reason": str(e)})
                continue
            base_name = os.path.basename(file_name)
            file_size = len(file_data)
            if file_size == 0:
                skipped_files.append({"fileName": base_name, "reason": "Empty"})
                continue
            dest_key = unzip_prefix + base_name
            lower_name = base_name.lower()

            # PDF files
            if lower_name.endswith('.pdf'):
                if not is_valid_pdf(base_name, file_size):
                    skipped_files.append({"fileName": base_name, "reason": "Too small"})
                    continue
                s3_client.put_object(
                    Bucket=bucket,
                    Key=dest_key,
                    Body=file_data,
                    ContentType='application/pdf',
                    ServerSideEncryption='AES256'
                )
                unique_code = os.path.splitext(base_name)[0]
                pdf_files.append({
                    "s3Key": dest_key,
                    "fileName": base_name,
                    "uniqueCode": unique_code,
                    "fileSize": file_size,
                    "s3Bucket": bucket
                })
                valid_files_processed += 1

            # Excel file (just one per job)
            elif lower_name.endswith(('.xlsx', '.xls')):
                if not is_valid_excel(base_name, file_size):
                    skipped_files.append({"fileName": base_name, "reason": "Too small"})
                    continue
                s3_client.put_object(
                    Bucket=bucket,
                    Key=dest_key,
                    Body=file_data,
                    ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    ServerSideEncryption='AES256'
                )
                if excel_file is None:
                    excel_file = {
                        "s3Key": dest_key,
                        "fileName": base_name,
                        "fileSize": file_size,
                        "s3Bucket": bucket
                    }
                    valid_files_processed += 1
                else:
                    skipped_files.append({"fileName": base_name, "reason": "Duplicate"})
            # Any other file
            else:
                s3_client.put_object(
                    Bucket=bucket,
                    Key=dest_key,
                    Body=file_data,
                    ServerSideEncryption='AES256'
                )
                other_files.append({
                    "s3Key": dest_key,
                    "fileName": base_name,
                    "fileSize": file_size
                })
                valid_files_processed += 1

            # Update progress every 10 files, set update timestamp
            if (idx + 1) % 10 == 0:
                now_millis = get_ist_millis()
                progress = 10 + int((idx + 1) / total_entries * 30)
                jobs_collection.update_one(
                    {"jobId": job_id}, 
                    {"$set": {"progress": progress, "updatedOn_KJUSYSCommon_DateTime": now_millis}}
                )

        # 6. Must find one Excel and one PDF
        if excel_file is None:
            raise Exception("No Excel file found")
        if len(pdf_files) == 0:
            raise Exception("No PDF files found")
        now_millis = get_ist_millis()
        jobs_collection.update_one(
            {"jobId": job_id},
            {"$set": {
                "progress": 40,
                "status": "UNZIPPED",
                "currentStep": "Starting validation",
                "updatedOn_KJUSYSCommon_DateTime": now_millis,
                "totalFiles": total_entries,
                "validFilesProcessed": valid_files_processed,
                "totalPDFs": len(pdf_files),
                "pdfFiles": pdf_files,
                "excelFile": excel_file,
                "unzipFolderPath": unzip_prefix
            }}
        )

        # 7. Call Lambda 2 for validation
        lambda2_result = invoke_lambda2_sync({"jobId": job_id})
        if not lambda2_result['success']:
            error_msg = lambda2_result.get('error', 'Unknown error')
            now_millis = get_ist_millis()
            jobs_collection.update_one(
                {"jobId": job_id},
                {"$set": {
                    "status": "VALIDATION_FAILED",
                    "error": f"Validation failed: {error_msg}",
                    "updatedOn_KJUSYSCommon_DateTime": now_millis
                }}
            )
            raise Exception(f"Lambda 2 failed: {error_msg}")

        # 8. Optionally, delete zip after success
        lambda2_successful = True
        if DELETE_ZIP_AFTER_SUCCESS and lambda2_successful:
            delete_zip_file(bucket, key, job_id)

        # 9. Mark completion
        now_millis = get_ist_millis()
        jobs_collection.update_one(
            {"jobId": job_id},
            {"$set": {
                "status": "COMPLETED",
                "updatedOn_KJUSYSCommon_DateTime": now_millis
            }}
        )

        return {
            "statusCode": 200,
            "jobId": job_id,
            "status": "SUCCESS"
        }
    except Exception as e:
        now_millis = get_ist_millis()
        jobs_collection.update_one(
            {"jobId": job_id},
            {"$set": {
                "status": "FAILED",
                "error": str(e),
                "updatedOn_KJUSYSCommon_DateTime": now_millis
            }}
        )
        return {
            "statusCode": 500,
            "jobId": job_id,
            "status": "FAILED",
            "error": str(e)
        }
