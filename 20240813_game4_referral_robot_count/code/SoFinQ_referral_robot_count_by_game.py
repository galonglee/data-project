# Import necessary libraries
import pandas as pd
from datetime import datetime as dt, timedelta
import os
from sqlalchemy import create_engine
import requests
import sys
import logging
from dotenv import load_dotenv

# Set up date variables
today1 = dt.today().strftime('%Y%m%d')  # Format today's date as YYYYMMDD
today = dt.today()  # Get today's date as a datetime object

# Define file paths
output_path = 'C:/Users/User/Insync/Drivers/Google Drive/黑森科技/數據專案/20240813_game4_referral_robot_count/output/'
logs_path = 'C:/Users/User/Insync/Drivers/Google Drive/黑森科技/數據專案/20240813_game4_referral_robot_count/logs/'

# Create necessary directories
if not os.path.exists(output_path):
    os.makedirs(output_path)
if not os.path.exists('logs'):
    os.makedirs('logs')

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/robot_count_{dt.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)

# Initialize logger
logger = logging.getLogger(__name__)

# Define function to validate competition data
def validate_competition_data(cp_data):
    required_columns = ['competition_id', 'time_start', 'time_end']
    if not all(col in cp_data.columns for col in required_columns):
        raise ValueError("Missing required columns in competition data")
    return True

# Define functions for Zoho WorkDrive file upload
def get_access_token(url, params):
    response = requests.post(url, params=params)
    response.raise_for_status()  # Add error handling
    return response.json().get('access_token')

def upload_file(url, headers, payload, file_path, file_type):
    try:
        with open(file_path, 'rb') as f:
            files = {'content': (file_path, f, file_type)}
            response = requests.post(url, headers=headers, data=payload, files=files)
            response.raise_for_status()
            logging.info(f"Successfully uploaded {file_path}")
    except Exception as e:
        logging.error(f"Failed to upload {file_path}: {e}")

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

# Function to calculate report start and end dates for competitions
def get_report_start_end_date():
    """
    Calculate report start and end dates for each competition.

    This function determines the reporting schedule for competitions based on the following rules:
    - Reports are generated at 10am every Wednesday and 5pm every Friday.
    - The first report for each competition is the nearest reporting time after the competition start date.
    - The last report for each competition is the nearest reporting time before the competition end date.

    The function modifies the global 'cp' DataFrame by adding two new columns:
    - 'report_start_date': The date and time of the first report for each competition.
    - 'report_end_date': The date and time of the last report for each competition.

    Returns:
    --------
    pandas.DataFrame
        The modified 'cp' DataFrame with added report start and end date columns.

    Note:
    -----
    - All dates and times are in GMT+8 timezone.
    - The function assumes that the global 'cp' DataFrame exists and contains 'competition_id', 'time_start', and 'time_end' columns.
    """

    def calculate_report_dates(start_date, end_date):
        """
        Calculate the first and last report dates for a given competition period.

        Parameters:
        -----------
        start_date : datetime
            The start date of the competition.
        end_date : datetime
            The end date of the competition.

        Returns:
        --------
        tuple
            A tuple containing the first and last report dates (datetime objects).
        """
        # Calculate potential first report dates
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
        
        # Calculate potential last report dates
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
    
    # Iterate through each unique competition to calculate report dates
    for competition_id in cp['competition_id'].unique():
        competition = cp[cp['competition_id'] == competition_id]
        start_date = pd.to_datetime(competition['time_start'].values[0])
        end_date = pd.to_datetime(competition['time_end'].values[0])
        
        first_report_date, last_report_date = calculate_report_dates(start_date, end_date)
        
        report_start_dates.append(first_report_date)
        report_end_dates.append(last_report_date)
    
    # Add new columns to the cp DataFrame
    cp['report_start_date'] = report_start_dates
    cp['report_end_date'] = report_end_dates
    
    return cp

# Set up database connection information
conn_info = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

logging.info(f"Database: {conn_info['database']}, User: {conn_info['user']}, Host: {conn_info['host']}, Port: {conn_info['port']}")

# SQL query to fetch competition data
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

# Create database engine and connect to fetch competition data
try:
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(
        conn_info['user'], 
        conn_info['password'], 
        conn_info['host'], 
        conn_info['port'], 
        conn_info['database']
    ))
except Exception as e:
    logging.error(f"Failed to create database engine: {e}")
    sys.exit(1)

# Execute SQL query and load data into DataFrame
with engine.connect() as conn:
    cp = pd.read_sql(sql_command1, conn)
    validate_competition_data(cp)

# Process competition data
cp['competition_id'] = cp['competition_id'].astype(str)
cp = get_report_start_end_date()

# Find competitions that are currently active
competition_id_list = cp[(cp['report_start_date'] <= today) & (cp['report_end_date'] >= today)]['competition_id'].tolist()
competition_id_list_str = str(tuple(competition_id_list))  # Convert list to SQL-compatible string
competition_name_list = cp[cp['competition_id'].isin(competition_id_list)]['competition_title'].tolist()

# Handle single-element tuple case
if not competition_id_list:
    refer = pd.DataFrame()
elif len(competition_id_list) == 1:
    competition_id_list_str = f"({competition_id_list[0]})"

# SQL query to fetch detailed referral data for active competitions
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
        COALESCE(rc.robot_count, 0) AS robot_count,
        au.email, 
        vu.real_name, 
        vu.country_code, 
        vu.phone,
        vu.referralCode AS referral_code,
        rfr.referral_id,
        rfn.referrer_name,
        COALESCE(rrc.create_robot_user_count, 0) AS create_robot_user_count,
        tuc.total_user_count AS total_user_count
    FROM all_users au
    LEFT JOIN verified_users vu ON au.user_id = vu.user_id
    LEFT JOIN referrer rfr ON au.user_id = rfr.id
    LEFT JOIN referrer_name rfn ON rfr.referral_id = rfn.id
    LEFT JOIN robot_count rc ON rc.user_id = au.user_id
    LEFT JOIN referrer_robot_count rrc ON rfn.referrer_name = rrc.referrer_name
    LEFT JOIN total_user_count tuc ON rfr.referral_id = tuc.referral_id
    ORDER BY rfn.referrer_name, au.user_id ASC;
    """

    # Execute SQL query and load referral data into DataFrame
    with engine.connect() as conn:
        refer = pd.read_sql(sql_command2, conn)
    refer[['user_id','phone','referral_id']] = refer[['user_id','phone','referral_id']].astype(str)
else:
    refer = pd.DataFrame()

# Set up Zoho API credentials and parameters
CLIENT_ID = os.getenv('ZOHO_CLIENT_ID')
CLIENT_SECRET = os.getenv('ZOHO_CLIENT_SECRET')
SCOPE = os.getenv('ZOHO_SCOPE', "WorkDrive.files.CREATE")
SOID = os.getenv('ZOHO_SOID')

# API endpoint for getting access token
url = os.getenv('ZOHO_TOKEN_URL', "https://accounts.zoho.com/oauth/v2/token")

# Parameters for access token request
params = {
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET, 
    "grant_type": "client_credentials",
    "scope": SCOPE,
    "soid": SOID
}

# Get access token for Zoho API
ACCESS_TOKEN = get_access_token(url, params)

# ID of parent folder in Zoho WorkDrive where files will be uploaded
parent_id = os.getenv('ZOHO_PARENT_FOLDER_ID')

# Create and upload Excel file for all referrers if data exists
if not refer.empty:
    try:
        all_referrers_file = f'{output_path}SoFinQ_all_referrers_IV_KYC_games_{today1}.xlsx'
        with pd.ExcelWriter(all_referrers_file, engine='openpyxl') as writer:
            for competition_name in competition_name_list:
                sheet_name = competition_name[:31]
                competition_data = refer[refer['title'] == competition_name]
                competition_data.to_excel(writer, sheet_name=sheet_name, index=False)
        logging.info("Successfully created all referrers Excel file")
        
        # Initialize file_paths list with the all referrers file
        file_paths = [(all_referrers_file, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')]
        
        # Upload the file to Zoho WorkDrive
        upload_files(file_paths, parent_id, ACCESS_TOKEN)
        logging.info("Successfully uploaded all referrers file to Zoho WorkDrive")
    except Exception as e:
        logging.error(f"Failed to create or upload all referrers Excel file: {e}")

# Prepare data for final reports
cp['report_end_date1'] = pd.to_datetime(cp['report_end_date']).dt.date
today_date = pd.to_datetime(today).date().strftime('%Y-%m-%d')
competition_name_list_last_report_date = cp[(cp['report_end_date1'] == today_date)]['competition_title'].tolist()

# Create and upload final reports if any competitions end today
if competition_name_list_last_report_date:
    try:
        refer_last_report_date = refer[refer['competition_title'].isin(competition_name_list_last_report_date)]
        final_report_file = f'{output_path}final_report_{today1}.xlsx'
        
        logging.info(f"Creating final reports for {len(competition_name_list_last_report_date)} competitions")
        
        # Initialize list to store all file paths for upload
        all_file_paths = []
        
        # Create combined final report
        with pd.ExcelWriter(final_report_file, engine='openpyxl') as writer:
            for competition_name in competition_name_list_last_report_date:
                title_name = competition_name[:31]
                competition_data = refer_last_report_date[refer_last_report_date['competition_title'] == competition_name]
                competition_data.to_excel(writer, sheet_name=title_name, index=False)
        
        # Add combined final report to upload list
        all_file_paths.append((final_report_file, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
        logging.info(f"Created combined final report: {final_report_file}")
        
        # Create individual final reports
        for competition_name in competition_name_list_last_report_date:
            try:
                title_name = competition_name[:31]
                individual_final_file = f'{output_path}{title_name}_final.xlsx'
                
                competition_data = refer_last_report_date[refer_last_report_date['competition_title'] == competition_name]
                competition_data.to_excel(individual_final_file, sheet_name=title_name, index=False)
                
                all_file_paths.append((individual_final_file, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'))
                logging.info(f"Created individual final report for {competition_name}")
            except Exception as e:
                logging.error(f"Failed to create individual final report for {competition_name}: {e}")
        
        # Get fresh access token for upload
        ACCESS_TOKEN = get_access_token(url, params)
        
        # Upload all files
        logging.info(f"Attempting to upload {len(all_file_paths)} files to Zoho WorkDrive")
        upload_files(all_file_paths, parent_id, ACCESS_TOKEN)
        
        # Log successful uploads
        for file_path, _ in all_file_paths:
            logging.info(f"Successfully uploaded: {os.path.basename(file_path)}")
        
        logging.info("All final reports have been uploaded to Zoho WorkDrive")
        
    except Exception as e:
        logging.error(f"Error in final report processing: {e}")
else:
    logging.info('No final report for competitions found')