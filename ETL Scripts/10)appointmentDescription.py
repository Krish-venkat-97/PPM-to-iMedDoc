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

src_appt_desc = 'SELECT * FROM CodeConsultation'
src_appt_desc_df = pd.read_sql(src_appt_desc, get_src_accessdb_connection())

src_consultation = 'SELECT ConsultationCode FROM Consultations'
src_consultation_df = pd.read_sql(src_consultation, get_src_accessdb_connection())

src_consultation_df = src_consultation_df.drop_duplicates(subset=['ConsultationCode']).reset_index(drop=True)

landing_appt_desc = """
SELECT ConsultationCode, ConsultationDescription, DefaultCharge,EDIConsultationCode FROM src_appt_desc_df
UNION
SELECT 'From Consultation'||'-'||ConsultationCode AS ConsultationCode, ConsultationCode as ConsultationDescription,NULL,NULL FROM src_consultation_df
"""
landing_appt_desc_df = ps.sqldf(landing_appt_desc)

landing_appt_desc_df['ConsultationDescription'] = landing_appt_desc_df['ConsultationDescription'].str.strip()
landing_appt_desc_df['ConsultationDescription_Upper'] = landing_appt_desc_df['ConsultationDescription'].str.upper()
landing_appt_desc_df = landing_appt_desc_df.drop_duplicates(subset=['ConsultationDescription_Upper'],keep='first').reset_index(drop=True)
landing_appt_desc_df = landing_appt_desc_df.drop(columns=['ConsultationDescription_Upper'])
landing_appt_desc_df = landing_appt_desc_df.dropna(subset=['ConsultationDescription'])

src_appt_desc_df1 = landing_appt_desc_df[['ConsultationCode', 'ConsultationDescription', 'DefaultCharge','EDIConsultationCode']]

src_appt_desc_df1['is_archive'] = src_appt_desc_df1.apply(lambda x: 0 if pd.notnull(x['EDIConsultationCode']) else 1, axis=1)

# appt_desc_id generation
app_desc_max = 'SELECT MAX(id) FROM appointment_descriptions'
app_desc_max_df = pd.read_sql(app_desc_max, myconnection)
if app_desc_max_df is None or app_desc_max_df.iloc[0, 0] is None:
    app_desc_max_id = 1
else:
    app_desc_max_id = app_desc_max_df.iloc[0, 0] + 1
src_appt_desc_df1.insert(0, 'appt_desc_id', range(app_desc_max_id, app_desc_max_id + len(src_appt_desc_df1)))

#procedure_id generation
procedure_max = 'SELECT MAX(id) FROM procedures'
procedure_max_df = pd.read_sql(procedure_max, myconnection)
if procedure_max_df is None or procedure_max_df.iloc[0, 0] is None:
    procedure_max_id = 1
else:
    procedure_max_id = procedure_max_df.iloc[0, 0] + 1
src_appt_desc_df1.insert(0, 'procedure_id', range(procedure_max_id, procedure_max_id + len(src_appt_desc_df1)))

#appointment description_procedures_id generation
app_desc_proc_max = 'SELECT MAX(id) FROM appointment_description_procedures'
app_desc_proc_max_df = pd.read_sql(app_desc_proc_max, myconnection)
if app_desc_proc_max_df is None or app_desc_proc_max_df.iloc[0, 0] is None:
    app_desc_proc_max_id = 1
else:
    app_desc_proc_max_id = app_desc_proc_max_df.iloc[0, 0] + 1
src_appt_desc_df1.insert(0, 'appt_desc_proc_id', range(app_desc_proc_max_id, app_desc_proc_max_id + len(src_appt_desc_df1)))

bar = tqdm(total=len(src_appt_desc_df1), desc='Processing appointment descriptions', unit='record')

tgt_app_desc = pd.read_sql('SELECT DISTINCT PPM_ApptDesc_Id, id FROM appointment_descriptions',myconnection)
tgt_app_desc['PPM_ApptDesc_Id'] = tgt_app_desc['PPM_ApptDesc_Id'].astype(int)

src_appt_desc_df2 = src_appt_desc_df1[~src_appt_desc_df1['ConsultationCode'].isin(tgt_app_desc['PPM_ApptDesc_Id'])].reset_index(drop=True)

for index,row in src_appt_desc_df2.iterrows():
    bar.update(1)
    try:
        appointment_desc_insert = f"""
        INSERT INTO appointment_descriptions (id,name,code,short_name,procedure_id,doctor_id,template_id,sms_template_id,created_at,updated_at,created_user_id,updated_user_id,PPM_ApptDesc_Id)
        VALUES (
            {safe_value(row['appt_desc_id'])},
            {safe_value(row['ConsultationDescription'])},
            {safe_value(row['EDIConsultationCode'] if pd.notnull(row['EDIConsultationCode']) else row['ConsultationDescription'])},
            {safe_value(row['ConsultationDescription'])},
            {safe_value(row['procedure_id'])},
            1,
            0,
            0,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            1,
            1,
           {safe_value(row['ConsultationCode'])}
        )
        """
        target_cursor.execute(appointment_desc_insert)

        procedure_insert = f"""
        INSERT INTO procedures (id,name,code,short_name,rate,is_archive,appointment_description_id,created_user_id,updated_user_id,created_at,updated_at,PPM_ApptDesc_Id)
        VALUES(
            {safe_value(row['procedure_id'])},
            {safe_value(row['ConsultationDescription'])},
            {safe_value(row['EDIConsultationCode'] if pd.notnull(row['EDIConsultationCode']) else row['ConsultationDescription'])},
            {safe_value(row['ConsultationDescription'])},
            {safe_value(row['DefaultCharge']) if pd.notnull(row['DefaultCharge']) else 0},
            {safe_value(row['is_archive'])},
            {safe_value(row['appt_desc_id'])},
            1,1,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP(),
            {safe_value(row['ConsultationCode'])}
            )
            """
        target_cursor.execute(procedure_insert)

        app_desc_proc_insert = f"""
        INSERT INTO appointment_description_procedures (id,appointment_description_id,procedure_id,created_at,updated_at,PPM_ApptDesc_Id)
        VALUES (
        {safe_value(row['appt_desc_proc_id'])},
        {safe_value(row['appt_desc_id'])},
        {safe_value(row['procedure_id'])},
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        1
        )
        """
        target_cursor.execute(app_desc_proc_insert)
    except Exception as e:
        print(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print( "Appointment descriptions processed successfully.")

