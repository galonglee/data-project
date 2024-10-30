import pandas as pd
from datetime import datetime as dt, timedelta
import os
from sqlalchemy import create_engine
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# create today date
today1 = dt.today().strftime('%Y%m%d')
folder = f'C:/Users/User/Insync/Drivers/Google Drive/數據資料/SofinX-PROD/'
output_path = f'C:/Users/User/Insync/Drivers/Google Drive/黑森科技/數據專案/20240813_game4_referral_robot_count/output/'
if not os.path.exists(output_path):
    os.makedirs(output_path)

conn_info = {
    "database": os.getenv('DB_NAME'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "host": os.getenv('DB_HOST'),
    "port": os.getenv('DB_PORT')
}
sql_command1 = """
SELECT 
    cp.id AS competition_id, 
    cp.title AS competition_title, 
    cp.status,
    cp.time_start AT TIME ZONE 'UTC' AT TIME ZONE 'GMT+8' AS time_start, 
    cp.time_end AT TIME ZONE 'UTC' AT TIME ZONE 'GMT+8' AS time_end
FROM public.competitions cp
WHERE broker_id = 1
ORDER BY cp.id
"""
# Create the database engine
engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(conn_info['user'], conn_info['password'], conn_info['host'], conn_info['port'], conn_info['database']))

# Connect to the database and execute the SQL command
with engine.connect() as conn:
    cp = pd.read_sql(sql_command1, conn)

cp['competition_id'] = cp['competition_id'].astype(str)

def get_report_start_end_date():
    # Report times are at 10am every Wednesday and 5pm every Friday.
    # First report for each competition is the nearest time (either at 10am of the first Wednesday or 5pm) after the competition start date.
    # Last report for each competition is the nearest time (either at 5pm of the last Friday before the competition end date or 10am of the last Wednesday) before the competition end date.
    # time_start and time_end are in GMT+8, and we need to keep two new columns to GMT+8
    
    def calculate_report_dates(start_date, end_date):
        # Calculate the first report date
        first_wednesday = start_date + timedelta(days=(2 - start_date.weekday()) % 7)
        first_friday = start_date + timedelta(days=(4 - start_date.weekday()) % 7)
        first_wednesday1 = first_wednesday + timedelta(days=7)
        first_friday1 = first_friday + timedelta(days=7)
        
        first_report_dates = sorted([
            first_wednesday.replace(hour=10, minute=0),
            first_wednesday1.replace(hour=10, minute=0),
            first_friday.replace(hour=17, minute=0),
            first_friday1.replace(hour=17, minute=0)
        ])
        first_report_date = next(x for x in first_report_dates if x >= start_date)
        
        # Calculate the last report date
        last_wednesday = end_date + timedelta(days=(2 - end_date.weekday()) % 7)
        last_friday = end_date + timedelta(days=(4 - end_date.weekday()) % 7)
        last_wednesday1 = last_wednesday + timedelta(days=7)
        last_friday1 = last_friday + timedelta(days=7)
        
        last_report_dates = sorted([
            last_wednesday.replace(hour=10, minute=0),
            last_wednesday1.replace(hour=10, minute=0),
            last_friday.replace(hour=17, minute=0),
            last_friday1.replace(hour=17, minute=0)
        ])
        last_report_date = next(x for x in last_report_dates if x >= end_date)
        
        return first_report_date, last_report_date
    
    report_start_dates = []
    report_end_dates = []
    
    for competition_id in cp['competition_id'].unique():
        competition = cp[cp['competition_id'] == competition_id]
        start_date = pd.to_datetime(competition['time_start'].values[0])
        end_date = pd.to_datetime(competition['time_end'].values[0])
        
        first_report_date, last_report_date = calculate_report_dates(start_date, end_date)
        
        report_start_dates.append(first_report_date)
        report_end_dates.append(last_report_date)
    
    cp['report_start_date'] = report_start_dates
    cp['report_end_date'] = report_end_dates
    
    return cp

cp = get_report_start_end_date()

# Ensure today is a datetime object
today = dt.today()
# Find the list of competition_id which today is between report_start_date and report_end_date
competition_id_list = cp[(cp['report_start_date'] <= today) & (cp['report_end_date'] >= today)]['competition_id'].tolist()
competition_id_list_str = str(tuple(competition_id_list))  # Convert list to SQL-compatible string
competition_name_list = cp[cp['competition_id'].isin(competition_id_list)]['competition_title'].tolist()
# Handle single-element tuple case
if not competition_id_list:
    refer = pd.DataFrame()
elif len(competition_id_list) == 1:
    competition_id_list_str = f"({competition_id_list[0]})"

# Only execute the query if competition_id_list_str is not empty
if competition_id_list:
    sql_command2 = f"""
    WITH all_users AS (
        SELECT sfu.id AS user_id, sfu.username, sfu.email
        FROM public.competitions cp
        JOIN public.competitions_users cpu ON cpu.competition_id = cp.id
        JOIN public.sub_front_users sfu ON sfu.id = cpu.user_id
        WHERE cp.id IN {competition_id_list_str}
    ),
    referrer AS (
        SELECT sfu.id, sfu.referrer AS referral_id
        FROM public.sub_front_users sfu
    ),
    referrer_name AS (
        SELECT sfu.id, sfu.username AS referrer_name
        FROM public.sub_front_users sfu
    ),
    robot_count AS (
        SELECT DISTINCT r.user_id, COUNT(r.id) AS robot_count
        FROM public.robots r
        LEFT JOIN public.competitions_robots cpr ON r.id = cpr.robot_id
        WHERE cpr.competition_id IN {competition_id_list_str}
        GROUP BY r.user_id
    ),
    verified_users AS (
        SELECT 
        cp.id AS competition_id,
        cp.title,
        cpr.user_id,
        cpr.data::json ->> 'name'::text AS real_name,
        cpr.data::json ->> 'countryCode'::text AS country_code,
        cpr.data::json ->> 'phone'::text AS phone,
        cpr.data::json ->> 'referralCode'::text AS referralCode,
        cpr.created_at
        FROM competitions_registrations cpr
        JOIN competitions cp ON cp.id IN {competition_id_list_str} AND cp.id = cpr.competition_id
        WHERE cpr.data::json ->> 'referralCode'::text IS NOT null
        ORDER BY cp.id, cpr.created_at
    ),
    referrer_robot_count AS (
        SELECT 
            rfn.referrer_name,
            COALESCE(COUNT(rfn.id), 0) AS create_robot_user_count
        FROM referrer_name rfn
        JOIN referrer rfr ON rfn.id = rfr.referral_id
        JOIN robot_count rc ON rfr.id = rc.user_id
        GROUP BY rfn.referrer_name
    ),
    total_user_count AS (
        SELECT 
            COUNT(au.user_id) AS total_user_count,
            rfr.referral_id
        FROM all_users au
        JOIN referrer rfr ON rfr.id = au.user_id
        GROUP BY rfr.referral_id
    )
    SELECT 
        vu.title,
        au.user_id, 
        au.username,
        COALESCE(rc.robot_count, 0) AS robot_count, -- Use COALESCE to handle NULL values
        au.email, 
        vu.real_name, 
        vu.country_code, 
        vu.phone,
        vu.referralCode AS referral_code,
        rfr.referral_id,
        rfn.referrer_name,
        COALESCE(rrc.create_robot_user_count, 0) AS create_robot_user_count, -- Corrected table alias
        tuc.total_user_count AS total_user_count -- Use COALESCE to handle NULL values
    FROM all_users au
    LEFT JOIN verified_users vu ON au.user_id = vu.user_id
    LEFT JOIN referrer rfr ON au.user_id = rfr.id
    LEFT JOIN referrer_name rfn ON rfr.referral_id = rfn.id
    LEFT JOIN robot_count rc ON rc.user_id = au.user_id
    LEFT JOIN referrer_robot_count rrc ON rfn.referrer_name = rrc.referrer_name
    LEFT JOIN total_user_count tuc ON rfr.referral_id = tuc.referral_id
    ORDER BY rfn.referrer_name, au.user_id ASC;
    """

    with engine.connect() as conn:
        refer = pd.read_sql(sql_command2, conn)
    refer[['user_id','phone','referral_id']] = refer[['user_id','phone','referral_id']].astype(str)
else:
    refer = pd.DataFrame()
# if refer is not an empty data frame, save refer to xlsx with each sheet name as its competition_title
if not refer.empty:
    # Ensure competition_name_list is a single string if there are multiple competition titles
    sheet_name = ', '.join(competition_name_list) if len(competition_name_list) > 1 else competition_name_list[0]
    # Truncate sheet name to 31 characters
    sheet_name = sheet_name[:31]
    refer.to_excel(f'{output_path}SoFinQ_all_referrers_IV_KYC_games_{today1}.xlsx', sheet_name=sheet_name, index=False)
else:
    print('No competitions found')

# select the competitions of the refer, which today is the last_report_date of the competitions
# convert cp['report_end_date'] and today to date
cp['report_end_date1'] = pd.to_datetime(cp['report_end_date']).dt.date
today_date = pd.to_datetime(today).date().strftime('%Y-%m-%d')
competition_name_list_last_report_date = cp[(cp['report_end_date1'] == today_date)]['competition_title'].tolist()
# if competition_name_list_last_report_date is not empty, select the refer of the competitions
if competition_name_list_last_report_date:
    refer_last_report_date = refer[refer['competition_title'].isin(competition_name_list_last_report_date)]
    # save each competition in refer_last_report_date to a xlsx with file name as its competition_title
    for competition_name in competition_name_list_last_report_date:
        # Truncate sheet name to 25 characters
        title_name = competition_name[:25]
        refer_last_report_date[refer_last_report_date['competition_title'] == competition_name].to_excel(f'{output_path}{title_name}_final.xlsx', sheet_name=title_name, index=False)
else:
    print('No final report for competitions found')

# upload_files to zoho workdrive
def get_access_token(url, params):
    response = requests.post(url, params=params)
    response.raise_for_status()  # Add error handling
    return response.json().get('access_token')

def upload_file(url, headers, payload, file_path, file_type):
    with open(file_path, 'rb') as f:
        files = {'content': (file_path, f, file_type)}
        response = requests.post(url, headers=headers, data=payload, files=files)
        print(response.text)
# Function to upload multiple files
def upload_files(file_paths, parent_id, access_token):
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
    }
    payload = {
        'parent_id': parent_id,
        'override-name-exist': 'false'
    }
    for file_path, file_type in file_paths:
        upload_file("https://workdrive.zoho.com/api/v1/upload", headers, payload, file_path, file_type)

# Use environment variables for sensitive data
CLIENT_ID = os.getenv('ZOHO_CLIENT_ID')
CLIENT_SECRET = os.getenv('ZOHO_CLIENT_SECRET')
SCOPE = os.getenv('ZOHO_SCOPE')
SOID = os.getenv('ZOHO_SOID')

url = os.getenv('ZOHO_TOKEN_URL')
params = {
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "grant_type": "client_credentials",
    "scope": SCOPE,
    "soid": SOID
}
ACCESS_TOKEN = get_access_token(url, params)
parent_id = os.getenv('ZOHO_PARENT_FOLDER_ID')  # ID of the parent folder where you want to create the new folder
file_paths = [
    (f'{output_path}SoFinQ_all_referrers_IV_KYC_games_{today1}.xlsx', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
]
# upload_files for xlsx file
upload_files(file_paths, parent_id, ACCESS_TOKEN)
# upload_files for xlsx file if file names ending with final_{today1}.xlsx
if competition_name_list_last_report_date:
    for competition_name in competition_name_list_last_report_date:
        title_name = competition_name[:16]
        file_paths = [
            (f'{output_path}{title_name}_final_{today1}.xlsx', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        ]
        upload_files(file_paths, parent_id, ACCESS_TOKEN)