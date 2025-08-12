import boto3
import csv
import io
import logging

# Set up logging
logging.getLogger().setLevel(logging.INFO)
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Extract bucket and file key
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        logging.info(f"Processing file: s3://{bucket}/{key}")

        # Validate file is in raw/
        if not key.startswith('raw/'):
            logging.error(f"File {key} is not in raw/ folder")
            return {'statusCode': 400, 'body': f"Error: File {key} is not in raw/ folder"}

        # Read CSV from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8-sig', errors='ignore')
        logging.info(f"File size: {len(csv_content)} bytes")
        logging.info(f"Raw CSV content:\n{csv_content}")

        # Strip outer quotes from each line before parsing
        lines = csv_content.splitlines()
        cleaned_lines = [line.strip('"') for line in lines if line.strip() != '']
        input_file = io.StringIO('\n'.join(cleaned_lines))

        csv_reader = csv.reader(input_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)

        # Read header
        try:
            header = next(csv_reader)
            logging.info(f"Header: {header}")
        except StopIteration:
            logging.error("CSV is empty or malformed")
            return {'statusCode': 400, 'body': "Error: CSV is empty or malformed"}

        # Validate header (ensure at least one field)
        if len(header) < 1:
            logging.error(f"Invalid header: {header}. Expected at least one field")
            return {'statusCode': 400, 'body': "Error: Invalid header, expected at least one field"}

        # Clean rows
        cleaned_rows = []
        error_log = []
        for row in csv_reader:
            logging.info(f"Processing row: {row}")

            # Skip rows where row length doesn't match header length
            if len(row) != len(header):
                error_log.append(f"Skipped malformed row (wrong column count): {row}")
                continue

            # Skip rows where all fields are empty or whitespace (comma-only or empty rows)
            if all(not field.strip() for field in row):
                error_log.append(f"Skipped empty/comma-only row: {row}")
                continue

            # Skip rows with any missing/null fields (empty/whitespace-only)
            if any(not field.strip() for field in row):
                error_log.append(f"Skipped row with missing/null fields: {row}")
                continue

            # Append valid cleaned rows
            cleaned_rows.append(row)

        logging.info(f"Cleaned {len(cleaned_rows)} rows from {csv_reader.line_num} total rows (including header)")

        # Write cleaned CSV to memory
        output_file = io.StringIO()
        csv_writer = csv.writer(output_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        csv_writer.writerow(header)
        csv_writer.writerows(cleaned_rows)

        # Save cleaned CSV to cleaned/ folder
        cleaned_key = key.replace('raw/', 'cleaned/')
        logging.info(f"Output path: s3://{bucket}/{cleaned_key}")
        s3_client.put_object(
            Bucket=bucket,
            Key=cleaned_key,
            Body=output_file.getvalue().encode('utf-8')
        )
        logging.info(f"Cleaned CSV saved to s3://{bucket}/{cleaned_key}")

        # Save error log if any
        if error_log:
            log_key = f"logs/{key.split('/')[-1].replace('.csv', '_errors.log')}"
            s3_client.put_object(
                Bucket=bucket,
                Key=log_key,
                Body='\n'.join(error_log).encode('utf-8')
            )
            logging.info(f"Error log saved to s3://{bucket}/{log_key}")

        return {
            'statusCode': 200,
            'body': f"Cleaned CSV saved to {cleaned_key}"
        }

    except Exception as e:
        logging.error(f"Error processing file: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }
