import google.api_core
import psycopg2
import pandas as pd
import time
import pytz
from sqlalchemy import create_engine, exc, text
import time
import os
import json
import sys
import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText

# Load configuration from JSON file
f = open('./configurations.json')
config = json.load(f)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["GOOGLE_APPLICATION_CREDENTIALS"]
print(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

client = bigquery.Client()

# Define the BigQuery table ID
table_id = "your-project-id.dbt_your_table.orders"

# Get schema information for the BigQuery table
table = client.get_table(table_id)

# Create a schema DataFrame
schema = [{'Column': field.name, 'Data Type': field.field_type} 
          for field in table.schema]

df = pd.DataFrame(schema, columns=['Column', 'Data Type'])
print("Table schema: ", df)

# Define the order of columns for the BigQuery query
orderby = "id,user_id,order_number,invoice_number,note,status,wallet_id,created_by,coupon_id,previous_status,special_discount,stop_id,type,delivery_code,master_order_id,fulfilment_mode_id,is_fake,cart_id,seller_id,cancel_reason_id,delivery_package_id,is_exclusive,logistic_cost,vertical_id"

# Construct the BigQuery query
BQquery = "SELECT id,user_id,order_number,invoice_number,note,status,wallet_id,created_by,coupon_id,previous_status,special_discount,stop_id,type,delivery_code,master_order_id,fulfilment_mode_id,is_fake,cart_id,seller_id,cancel_reason_id,delivery_package_id,is_exclusive,logistic_cost,vertical_id FROM " + table_id + " ORDER BY "+ orderby + ""

# Execute the BigQuery query and convert the result to a DataFrame
bigquerytable = client.query(BQquery).to_dataframe()
print(bigquerytable)
print(type(bigquerytable))

# Save the DataFrame to a CSV file
bigquerytable.to_csv(r'C:\Users\Lenovo\OneDrive\Desktop\BQ.csv', index=None, header=True)

def fetch_aws_data(AWSTable, replication_key, min, limit):
    offset = 0
    limit = 1000
    incoming_data = True
    retries = 0
    where_clause = ""
    orderby = "id"

    if "on" in replication_key.lower() or "at" in replication_key.lower():
        min = str(min).split("+")[0]
        where_clause += replication_key + " > '" + str(min) + "'"
    elif "id" in replication_key.lower():
        where_clause += replication_key + " > " + str(min)

    records = pd.DataFrame()

    while incoming_data:
        AWSquery = "SELECT id,user_id,order_number,invoice_number,note,status,wallet_id,created_by,coupon_id,previous_status,special_discount,stop_id,type,delivery_code,master_order_id,fulfilment_mode_id,is_fake,cart_id,seller_id,cancel_reason_id,delivery_package_id,is_exclusive,logistic_cost,vertical_id FROM " + AWSTable +" WHERE " + where_clause + \
                   " ORDER BY "+ orderby + " ASC LIMIT " + str(limit) + " OFFSET " + str(offset) + ";"
        AWSquery = text(AWSquery)
        table_name = AWSTable.split(".")[-1]
        print("fetch_data::query:", AWSquery)

        try:
            print("fetch_data::Fetching " + str(limit) + " records from " + AWSTable + ". Row:" + str(offset+1) + " to Row: " + str(offset+limit) )
            start = time.time()
            print("start read")
            aws_data = pd.read_sql_query(AWSquery, conn)
            print(aws_data)
            end = time.time()
            print("fetch_data::Time taken for fetching " + str(records.shape[0]) + " records: ", end - start, "s")

            if not aws_data.empty:
                incoming_data = False
            else:
                records = records.append(aws_data)
            offset += limit
            retries = 0

        except exc.OperationalError as e:
            print("fetch_data::exception", e)
            if retries == 6:
                print("fetch_data::Max retries attempted. Terminating program")
                conn.close()
                sys.exit()
            retries += 1
            print("fetch_data::Retrying ...")

        except Exception as e:
            print("fetch_data::exception", e)
            conn.close()
            sys.exit
            
    records.to_csv(r'C:\Users\Lenovo\OneDrive\Desktop\AWS.csv', index=None, header=True)
    return records

def compare_data(BQtable,AWStable):
    merged_df = pd.concat([BQtable, AWStable], ignore_index=True)
    print("BQ DF")
    print(BQtable)
    print("AWS DF")
    print(AWStable)
    print("Merged DF")
    print(merged_df)
    diff_df = merged_df.drop_duplicates(subset=['id','description','is_report','is_active'], keep=False)
    print("Diff DF")
    print(diff_df)
    diff_df.to_csv(r'C:\Users\Lenovo\OneDrive\Desktop\DIFF.csv', index = None, header=True)

engine = create_engine(config["connection_string"])
conn = engine.connect()
f = open('./schema.json')
schema = json.load(f)
# fetch_aws_data("areas","id",0,100000)
compare_data(bigquerytable,fetch_aws_data("orders","id",0,100000))
conn.close()

 f=open('./user_login_details.json')
 user_login_details = json.load(f)
 emailfrom = "abdul.ahad@dastgyr.com"
 emailto = "abdul.ahad@dastgyr.com"
 fileToSend = "C:/Users/Lenovo/OneDrive/Desktop/DIFF.csv"
 username = user_login_details['username']
 password = user_login_details['password']

 msg = MIMEMultipart()
 msg["From"] = emailfrom
 msg["To"] = emailto
 msg["Subject"] = "DISCREPANCY ALERT"
 msg.preamble = "DISCREPANCY ALERT"
 ctype, encoding = mimetypes.guess_type(fileToSend)
 if ctype is None or encoding is not None:
     ctype = "application/octet-stream"

 maintype, subtype = ctype.split("/", 1)

 if maintype == "text":
     fp = open(fileToSend)
     # Note: we should handle calculating the charset
     attachment = MIMEText(fp.read(), _subtype=subtype)
     fp.close()
 elif maintype == "image":
     fp = open(fileToSend, "rb")
     attachment = MIMEImage(fp.read(), _subtype=subtype)
     fp.close()
 elif maintype == "audio":
     fp = open(fileToSend, "rb")
     attachment = MIMEAudio(fp.read(), _subtype=subtype)
     fp.close()
 else:
     fp = open(fileToSend, "rb")
     attachment = MIMEBase(maintype, subtype)
     attachment.set_payload(fp.read())
     fp.close()
     encoders.encode_base64(attachment)
 attachment.add_header("Content-Disposition", "attachment", filename=fileToSend)
 msg.attach(attachment)

 server = smtplib.SMTP("smtp.gmail.com:587")
 server.starttls()
 server.login(username,password)
 server.sendmail(emailfrom, emailto, msg.as_string())
 server.quit()