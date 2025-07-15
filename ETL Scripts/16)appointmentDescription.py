import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

# Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE appointment_descriptions ADD COLUMN IF NOT EXISTS PPM_ApptDesc_Id VARCHAR(100) DEFAULT NULL;"
query_3 = "ALTER TABLE procedures ADD COLUMN IF NOT EXISTS PPM_ApptDesc_Id VARCHAR(100) DEFAULT NULL;"
query_4 = "ALTER TABLE appointment_description_procedures ADD COLUMN IF NOT EXISTS PPM_ApptDesc_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
target_cursor.execute(query_3)
target_cursor.execute(query_4)
myconnection.commit()

src_appt_desc = 'SELECT * FROM DiaryEx'

try:
    src_appt_desc_df = pd.read_sql(src_appt_desc, get_src_accessdb2_connection())
except:
    src_appt_desc_df = pd.read_sql(src_appt_desc, get_src_accessdb_connection())

appt_type_desc_df = src_appt_desc_df[src_appt_desc_df['AppointmentType'].str.upper().str.strip() != 'THEATRE']
surgery_type_desc_df = src_appt_desc_df[src_appt_desc_df['AppointmentType'].str.upper().str.strip() == 'THEATRE']

appt_type_desc_df['AppointmentType'] = appt_type_desc_df['AppointmentType'].fillna('Not defined AppointmentType').str.strip()
appt_type_desc_df['AppointmentType'] = appt_type_desc_df['AppointmentType'].replace('', 'Not Defined AppointmentType').str.strip()
appt_type_desc_df['AppointmentType_Upper'] = appt_type_desc_df['AppointmentType'].str.upper().str.strip()
appt_type_desc_df1 = appt_type_desc_df.drop_duplicates(subset=['AppointmentType_Upper'], keep='first').reset_index(drop=True)
appt_type_desc_df1 = appt_type_desc_df1.drop(columns=['AppointmentType_Upper'])
appt_type_desc_df2 = appt_type_desc_df1.rename(columns={'AppointmentType': 'Description'})
appt_type_desc_df2['type'] = 'appointment'
appt_type_desc_df3 = appt_type_desc_df2[['Description', 'type']]

surgery_type_desc_df['Text'] = surgery_type_desc_df['Text'].fillna('Not defined SurgeryType').str.strip()
surgery_type_desc_df['Text'] = surgery_type_desc_df['Text'].replace('', 'Not Defined SurgeryType').str.strip()
surgery_type_desc_df['Text_Upper'] = surgery_type_desc_df['Text'].str.upper().str.strip()
surgery_type_desc_df1 = surgery_type_desc_df.drop_duplicates(subset=['Text_Upper'], keep='first').reset_index(drop=True)
surgery_type_desc_df1 = surgery_type_desc_df1.drop(columns=['Text_Upper'])
surgery_type_desc_df2 = surgery_type_desc_df1.rename(columns={'Text': 'Description'})
surgery_type_desc_df2['type'] = 'surgery'
surgery_type_desc_df3 = surgery_type_desc_df2[['Description', 'type']]

landing_desc_df = pd.concat([appt_type_desc_df3, surgery_type_desc_df3], ignore_index=True)
landing_desc_df['Description'] = landing_desc_df['Description'].str.strip()

tgt_app_desc = pd.read_sql('SELECT DISTINCT name as procedure_name FROM appointment_descriptions WHERE PPM_ApptDesc_Id IS NOT NULL',myconnection)
tgt_app_desc['procedure_name'] = tgt_app_desc['procedure_name'].astype(str)

landing_desc_df = landing_desc_df[~landing_desc_df['Description'].isin(tgt_app_desc['procedure_name'])].reset_index(drop=True)

# appt_desc_id generation
app_desc_max = 'SELECT MAX(id) FROM appointment_descriptions'
app_desc_max_df = pd.read_sql(app_desc_max, myconnection)
if app_desc_max_df is None or app_desc_max_df.iloc[0, 0] is None:
    app_desc_max_id = 1
else:
    app_desc_max_id = app_desc_max_df.iloc[0, 0] + 1
landing_desc_df.insert(0, 'appt_desc_id', range(app_desc_max_id, app_desc_max_id + len(landing_desc_df)))

#procedure_id generation
procedure_max = 'SELECT MAX(id) FROM procedures'
procedure_max_df = pd.read_sql(procedure_max, myconnection)
if procedure_max_df is None or procedure_max_df.iloc[0, 0] is None:
    procedure_max_id = 1
else:
    procedure_max_id = procedure_max_df.iloc[0, 0] + 1
landing_desc_df.insert(0, 'procedure_id', range(procedure_max_id, procedure_max_id + len(landing_desc_df)))

#appointment description_procedures_id generation
app_desc_proc_max = 'SELECT MAX(id) FROM appointment_description_procedures'
app_desc_proc_max_df = pd.read_sql(app_desc_proc_max, myconnection)
if app_desc_proc_max_df is None or app_desc_proc_max_df.iloc[0, 0] is None:
    app_desc_proc_max_id = 1
else:
    app_desc_proc_max_id = app_desc_proc_max_df.iloc[0, 0] + 1
landing_desc_df.insert(0, 'appt_desc_proc_id', range(app_desc_proc_max_id, app_desc_proc_max_id + len(landing_desc_df)))

landing_desc_df.loc[landing_desc_df['type'] == 'surgery', 'appt_desc_id'] = None
landing_desc_df.loc[landing_desc_df['type'] == 'surgery', 'appt_desc_proc_id'] = None

landing_appt_desc_df = landing_desc_df[landing_desc_df['type']== 'appointment'].reset_index(drop=True)
landing_surgery_desc_df = landing_desc_df

appointment_bar = tqdm(total=len(landing_appt_desc_df), desc='Processing landing_appt_desc_df')

#-----------------if some records ins present in target make the datafarme empty --------------
tgt_appt_desc = pd.read_sql('SELECT DISTINCT PPM_ApptDesc_Id FROM appointment_descriptions WHERE PPM_ApptDesc_Id IS NOT NULL', myconnection)
#---------------------------convert the column to string type for comparison---------------------------
tgt_appt_desc['PPM_ApptDesc_Id'] = tgt_appt_desc['PPM_ApptDesc_Id'].astype(int)
landing_appt_desc_df['check'] = 1
#---------------------------filter out records already present in target---------------------------
landing_appt_desc_df = landing_appt_desc_df[~landing_appt_desc_df['check'].isin(tgt_appt_desc['PPM_ApptDesc_Id'])].reset_index(drop=True)    

#-----------------if some records ins present in target make the datafarme empty --------------
tgt_procedure = pd.read_sql('SELECT DISTINCT PPM_ApptDesc_Id FROM procedures WHERE PPM_ApptDesc_Id IS NOT NULL', myconnection)
#---------------------------convert the column to string type for comparison---------------------------
tgt_procedure['PPM_ApptDesc_Id'] = tgt_procedure['PPM_ApptDesc_Id'].astype(int)
landing_surgery_desc_df['check'] = 1
#---------------------------filter out records already present in target---------------------------
landing_surgery_desc_df = landing_surgery_desc_df[~landing_surgery_desc_df['check'].isin(tgt_procedure['PPM_ApptDesc_Id'])].reset_index(drop=True)

for index, row in landing_appt_desc_df.iterrows():
    appointment_bar.update(1)
    try:
        appointment_desc_insert = f"""
        INSERT INTO appointment_descriptions (id,name,code,short_name,procedure_id,doctor_id,template_id,sms_template_id,created_at,updated_at,created_user_id,updated_user_id,PPM_ApptDesc_Id)
        VALUES (
        {safe_value(row['appt_desc_id'])},
        {safe_value(row['Description'])},
        {safe_value(row['Description'])},
        {safe_value(row['Description'])},
        {safe_value(row['procedure_id'])},
        1,
        0,
        0,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        1,
        1,
        1
        )
        """
        target_cursor.execute(appointment_desc_insert)

        app_desc_proc_insert = f"""
        INSERT INTO appointment_description_procedures (id,appointment_description_id,procedure_id,doctor_id,created_at,updated_at,PPM_ApptDesc_Id)
        VALUES (
        {safe_value(row['appt_desc_proc_id'])},
        {safe_value(row['appt_desc_id'])},
        {safe_value(row['procedure_id'])},
        1,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        1
        )
        """
        target_cursor.execute(app_desc_proc_insert)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
appointment_bar.close()

# Process surgery descriptions
surgery_bar = tqdm(total=len(landing_surgery_desc_df), desc='Processing landing_surgery_desc_df')

for index, row in landing_surgery_desc_df.iterrows():
    surgery_bar.update(1)
    try:
        procedure_insert = f"""
        INSERT INTO procedures (id,name,code,short_name,rate,is_archive,appointment_description_id,created_user_id,updated_user_id,created_at,updated_at,PPM_ApptDesc_Id)
        VALUES(
        {safe_value(row['procedure_id'])},
        {safe_value(row['Description'])},
        {safe_value(row['Description'])},
        {safe_value(row['Description'])},
        0.0,
        0,
        {safe_value(row['appt_desc_id'])},
        1,1,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        1
        )
        """
        target_cursor.execute(procedure_insert)
    except Exception as e:
        logging.error(f"Error inserting surgery row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
surgery_bar.close()
print('Appointment and Surgery Descriptions processed successfully.')



