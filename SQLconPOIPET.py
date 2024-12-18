import mysql.connector
from mysql.connector import Error
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

max_rows_per_file = 400000  # Maximum rows allowed per Excel file

def connect_and_extract_to_excel():
    conn = None
    try:
        # Establish the connection
        conn = mysql.connector.connect(
            host="103.173.187.2",
            user="uj_prak_sovanmony",
            password="WDj6jjE67EWfT1KSoqy8",
            database="bi_takeaway",
            port=29334   
        )   
        
        if conn.is_connected():
            print("\nConnected to data warehouse === Mony access")
            cursor = conn.cursor()

            # SQL query to fetch data
            query = """WITH poipet_order AS (
                SELECT 
                    order_no,
                    order_time,
                    order_hour,
                    customer_no,
                    gmv,
                    net_gmv,
                    city,
                    lang,
                    receiver_gender,
                    receiver_name,
                    store_no,
                    store,
                    salesman,
                    level2,
                    level3,
                    store_type,
                    first_order_type,
                    delivery_distance,
                    sop_no,
                    receiver_lat,
                    receiver_lng,
                    order_type,
                    order_source,
                    goods_qty,
                    store_score,
                    delivery_content,
                    deliverfee_discount_title,
                    promocode_no,
                    coupon_no,
                    coupon_title,
                    success,
                    CONCAT(receiver_lat, ',', receiver_lng) AS geo_code
                FROM 
                    wownow_order_detail_unique 
                WHERE 
                    order_time BETWEEN '2024-07-01 00:00:00' AND '2024-09-30 00:00:00'
                    AND city = 'POIPET'
                ORDER BY 
                    order_time DESC
            ),
            dms AS (
                SELECT 
                    operator_no,
                    MAX(app_id) as app_id, 
                    MAX(phone_mode) as phone_model,
                    MAX(reg_method) as last_login_method
                FROM 
                    dms_user_label_unique
                GROUP BY 
                    operator_no
            )

            SELECT 
                po.order_no,
                po.order_time,
                po.order_hour,
                po.customer_no,
                po.gmv,
                po.net_gmv,
                po.city,
                po.lang,
                po.receiver_gender,
                po.receiver_name,
                po.store_no,
                po.store,
                po.salesman,
                po.level2,
                po.level3,
                po.store_type,
                po.first_order_type,
                po.delivery_distance,
                po.sop_no,
                po.receiver_lat,
                po.receiver_lng,
                po.geo_code,
                po.order_type,
                po.order_source,
                po.goods_qty,
                po.store_score,
                po.delivery_content,
                po.deliverfee_discount_title,
                po.promocode_no,
                po.coupon_no,
                po.coupon_title,
                po.success,
                dms.operator_no,
                dms.app_id,
                dms.phone_model,
                dms.last_login_method
            FROM 
                poipet_order po
            LEFT JOIN 
                dms ON po.customer_no = dms.operator_no
            LEFT JOIN 
                dms_user_session_info dmsv2 ON po.customer_no = dmsv2.operator_no
            GROUP BY 
                po.order_no,
                po.order_time,
                po.order_hour,
                po.customer_no,
                po.gmv,
                po.net_gmv,
                po.city,
                po.lang,
                po.receiver_gender,
                po.receiver_name,
                po.store_no,
                po.store,
                po.salesman,
                po.level2,
                po.level3,
                po.store_type,
                po.first_order_type,
                po.delivery_distance,
                po.sop_no,
                po.receiver_lat,
                po.receiver_lng,
                po.geo_code,
                po.order_type,
                po.order_source,
                po.goods_qty,
                po.store_score,
                po.delivery_content,
                po.deliverfee_discount_title,
                po.promocode_no,
                po.coupon_no,
                po.coupon_title,
                po.success,
                dms.operator_no,
                dms.app_id,
                dms.phone_model,
                dms.last_login_method
            ORDER BY order_time ASC"""

            cursor.execute(query)

            # Fetch all rows from the executed query
            data = cursor.fetchall()

            # Get column names
            columns = [i[0] for i in cursor.description]

            # Create a DataFrame from the fetched data
            df = pd.DataFrame(data, columns=columns)
            return df  # Return DataFrame for further processing

    except Error as e:
        print(f"\nError: {e}")

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection is closed")

def transform_data(df):

    
    # Replace any remaining NaN values in the entire DataFrame with zero
    df.fillna(0, inplace=True)

    # Ensure 'order_time' is a datetime type and format it to string format
    if 'order_time' in df.columns:
        df['order_time'] = pd.to_datetime(df['order_time'], errors='coerce')
        df['order_time'] = df['order_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Replace spaces with empty strings, strip whitespace, and fill NaN/None with 0 for all columns except 'order_time'
    for col in df.columns:
        if col != 'order_time':
            # Convert to string and replace spaces and None values with '0'
            df[col] = df[col].astype(str).replace(' ', '').str.strip().replace(['', 'None', None, pd.NA, pd.NaT], '0').fillna('0')



    # Print only the head (first 5 rows) of the DataFrame in JSON format
    print("JSON Data (First 5 Rows):")
    print(df.head().to_json(date_format='iso', orient='records'))

    return df

def transform_data_v2(df):
    # Replace any remaining NaN values in the entire DataFrame with zero, except for 'order_time'
    df.loc[:, df.columns != 'order_time'] = df.loc[:, df.columns != 'order_time'].fillna(0)

    # Replace 'nan' and 'none' strings with zero in 'store_score' and 'order_type' columns
    df['store_score'] = df['store_score'].replace(['nan', 'none'], 0).fillna(0).astype(str)
    df['order_type'] = df['order_type'].replace(['nan', 'none'], 0).fillna(0).astype(str)  # Keep as string
    
    return df

def store_data(df, file_index):
    # Save the transformed DataFrame to an Excel file
    df.to_excel(f'cleaned_poipet_{file_index}.xlsx', index=False)

    # Load the Excel file
    df_to_gspread = pd.read_excel(r'C:\Users\kong.sambathreach\Desktop\Pylearning\excel_to_gspread\cleaned_takhmau_1.xlsx', sheet_name='Sheet1')

    # Convert 'order_no' to string format
    df_to_gspread['order_no'] = df_to_gspread['order_no'].astype(str)

    # Replace NaN and None with zero
    df_to_gspread.fillna(0, inplace=True)

    # Ensure 'order_time' is a datetime type
    df_to_gspread['order_time'] = pd.to_datetime(df_to_gspread['order_time'], errors='coerce')

    # Format 'order_time' to string format
    df_to_gspread['order_time'] = df_to_gspread['order_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Convert the entire DataFrame to JSON format
    json_data = df_to_gspread.to_json(date_format='iso', orient='records')

    # Output the JSON data
    print("JSON Data:")
    print(json_data)

def upload_to_google_sheets(df):
    # Define the scope for Google Sheets and Google Drive API
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
    ]

    # Authenticate using the service account key
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name("key.json", scope)
        client = gspread.authorize(creds)
    except FileNotFoundError:
        print("Error: key.json file not found.")
        return
    except Exception as e:
        print(f"Authentication failed: {e}")
        return

    # Open an existing Google Sheets document or create a new one
    try:
        sheet = client.open('gsheet_poipet')  # Open an existing Google Sheet by name
    except gspread.exceptions.SpreadsheetNotFound:
        print("Google Sheet not found, creating a new one.")
        sheet = client.create('gsheet_poipet')  # Create a new Google Sheet if it doesn't exist
        sheet.share('ksambathireach001@gmail.com', perm_type='user', role='writer')  # Share with the specified user

    # Select the first worksheet
    worksheet = sheet.get_worksheet(0)

    # Check if the worksheet is empty (i.e., no data in the first row)
    if not worksheet.get_all_values():  # If the worksheet is empty
        worksheet.append_row(df.columns.tolist(), value_input_option='RAW')  # Add the header (columns from the DataFrame)
        print("Added headers to the Google Sheet.")

    # Fetch existing order_no values from the first column
    existing_orders = worksheet.col_values(1)  # Assuming 'order_no' is in the first column

    # Filter the DataFrame to find new orders not present in the Google Sheet
    new_orders = df[~df['order_no'].isin(existing_orders)]

    # Start appending new rows from the row below the last filled row
    if not new_orders.empty:
        try:
            # Prefix order_no with apostrophe to treat as text
            new_orders['order_no'] = "'" + new_orders['order_no']

            # Append new orders to the Google Sheet
            worksheet.append_rows(new_orders.values.tolist(), value_input_option='USER_ENTERED')
            print(f"Uploaded {new_orders.shape[0]} new rows to Google Sheets.")

            # Format the order_no column as plain text to avoid scientific notation
            worksheet.format('A:A', {'numberFormat': {'type': 'TEXT'}})  # Assuming order_no is in column A
        except Exception as e:
            print(f"Failed to upload new data to Google Sheets: {e}")
    else:
        print("No new orders to upload.")

    # Print the URL of the Google Sheet
    print(f"Data upload process complete! You can access the Google Sheet here: {sheet.url}")

# Call the function to connect and export data
df = connect_and_extract_to_excel()

if df is not None:
    # Transform the data
    transformed_df = transform_data(df)

    # Store the cleaned data
    file_index = 1  # Change this as needed for each file
    store_data(transformed_df, file_index)

    # Call the function to upload data
    upload_to_google_sheets(transformed_df)

print("Script completed.")
