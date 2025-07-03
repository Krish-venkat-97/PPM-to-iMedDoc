from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_appointment = 'SELECT * FROM DiaryEx'
src_appointment_df = pd.read_sql(src_appointment, get_src_accessdb_connection())

src_appointment_df1 = src_appointment_df[src_appointment_df['AppointmentType'].str.upper().str.strip() != 'THEATRE']
src_appointment_df1['AppointmentType'] = src_appointment_df1['AppointmentType'].fillna('Not defined AppointmentType').str.strip()
src_appointment_df1['AppointmentType'] = src_appointment_df1['AppointmentType'].replace('', 'Not Defined AppointmentType').str.strip()

# Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE appointments ADD COLUMN IF NOT EXISTS PPM_Appointment_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#get AppointmentDate
def get_appointment_date(row):
    if pd.isna(row['StartDate']):
        return None
    else:
        return row['StartDate'].strftime('%Y-%m-%d')
    
src_appointment_df1['AppointmentDate'] = src_appointment_df1.apply(get_appointment_date, axis=1)

def getStartTime(row):
    if pd.isna(row['StartDate']):
        return None
    else:
        a = row['StartDate'].strftime('%H:%M:%S')
        b = '1970-02-01 ' + a
        return b

src_appointment_df1['StartTime'] = src_appointment_df1.apply(getStartTime, axis=1)

#add duration to the starttime 
def getEndTime(row):
    if pd.isna(row['StartDate']):
        return None
    else:
        a = row['StartDate'] + pd.Timedelta(minutes=row['Duration'])
        b = a.strftime('%H:%M:%S')
        c = '1970-02-01 ' + b
        return c
    
src_appointment_df1['EndTime'] = src_appointment_df1.apply(getEndTime, axis=1)

#---------------------hospital mapping---------------------
src_appointment_df1['LocationCode'] = src_appointment_df1['LocationCode'].astype(int)
tgt_hospital_df = pd.read_sql("SELECT id as hospital_id,PPM_Hospital_Id FROM hospitals WHERE PPM_Hospital_Id IS NOT NULL", myconnection)
tgt_hospital_df['PPM_Hospital_Id'] = tgt_hospital_df['PPM_Hospital_Id'].astype(int)
landing_appointment_df1 = dd.merge(src_appointment_df1,tgt_hospital_df, left_on='LocationCode', right_on='PPM_Hospital_Id', how='left')

#---------------------appointment type mapping---------------------
landing_appointment_df1['AppointmentType_Upper'] = landing_appointment_df1['AppointmentType'].str.upper().str.strip()
tgt_appointment_description_df = pd.read_sql("SELECT id as appointment_type_id,UPPER(LTRIM(RTRIM(name))) as AppointmentType_Upper FROM appointment_descriptions WHERE PPM_ApptDesc_Id IS NOT NULL", myconnection)
tgt_appointment_description_df['AppointmentType_Upper'] = tgt_appointment_description_df['AppointmentType_Upper'].astype(str)
landing_appointment_df2 = dd.merge(landing_appointment_df1, tgt_appointment_description_df, on='AppointmentType_Upper', how='left')

#---------------------doctor mapping---------------------
landing_appointment_df2['ResourceCode'] = landing_appointment_df2['ResourceCode'].astype(int)
tgt_doctor_df = pd.read_sql("SELECT id as doctor_id,PPM_Doctor_Id FROM doctors WHERE PPM_Doctor_Id IS NOT NULL", myconnection)
tgt_doctor_df['PPM_Doctor_Id'] = tgt_doctor_df['PPM_Doctor_Id'].astype(int)
landing_appointment_df3 = dd.merge(landing_appointment_df2, tgt_doctor_df, left_on='ResourceCode', right_on='PPM_Doctor_Id', how='left')

#---------------------patient mapping---------------------
landing_appointment_df3['PatientCode'] = landing_appointment_df3['PatientCode'].astype(int)
tgt_patient_df = pd.read_sql("SELECT id as patient_id,PPM_Patient_Id FROM patients WHERE PPM_Patient_Id IS NOT NULL", myconnection)
tgt_patient_df['PPM_Patient_Id'] = tgt_patient_df['PPM_Patient_Id'].astype(int)
landing_appointment_df4 = dd.merge(landing_appointment_df3, tgt_patient_df, left_on='PatientCode', right_on='PPM_Patient_Id', how='left')

#---------------------appointment status mapping---------------------
landing_appointment_df4['appointment_status_id'] = landing_appointment_df4.apply(
    lambda x: 1 if x['AppointmentDate'] is not None and pd.to_datetime(x['AppointmentDate']).date() >= datetime.now().date() else 4,
    axis=1
)

#--------------------Filtering out rows with null patient_id---------------------
landing_appointment_df5 = landing_appointment_df4[landing_appointment_df4['patient_id'].notnull()]

#---------------------episode mapping---------------------
landing_appointment_df5['patient_id'] = landing_appointment_df5['patient_id'].astype(int)
tgt_episode_df = pd.read_sql("SELECT id as episode_id,patient_id FROM episodes WHERE name = 'General'", myconnection)
tgt_episode_df['patient_id'] = tgt_episode_df['patient_id'].astype(int)
landing_appointment_df5 = dd.merge(landing_appointment_df5, tgt_episode_df, on='patient_id', how='left')

#---------------------Missing values handling---------------------
landing_appointment_df5['doctor_id'] = landing_appointment_df5['doctor_id'].fillna(1).astype(int)
landing_appointment_df5['hospital_id'] = landing_appointment_df5['hospital_id'].fillna(1).astype(int)

#---------------------Dropping unnecessary columns---------------------
landing_appointment_df5 = landing_appointment_df5.loc[:, ~landing_appointment_df5.columns.str.contains('ppm', case=False)]

#---------------------Keeping only required columns---------------------
landing_appointment_df6 = landing_appointment_df5[['ID', 'patient_id', 'doctor_id', 'hospital_id', 'appointment_type_id', 'episode_id','appointment_status_id', 'Text', 'AppointmentDate','StartTime','Duration','EndTime']]

#----------------------Generating id for appointments---------------------
appointment_max = 'SELECT MAX(id) FROM appointments'
appointment_max_df = pd.read_sql(appointment_max,myconnection)
if appointment_max_df is None or appointment_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = appointment_max_df.iloc[0, 0] + 1
landing_appointment_df6.insert(0,'appointment_id',range(max_id,max_id+len(landing_appointment_df6)))

#---------------------filtering out rows already present in target database ---------------------
landing_appointment_df7 = landing_appointment_df6[~landing_appointment_df6['ID'].isin(pd.read_sql("SELECT PPM_Appointment_Id FROM appointments WHERE PPM_Appointment_Id IS NOT NULL", myconnection)['PPM_Appointment_Id'])]

#---------------------Inserting appointments into target database---------------------
appointment_bar = tqdm(total = len(landing_appointment_df6), desc='Inserting appointments')

for index,row in landing_appointment_df7.iterrows():
    appointment_bar.update(1)
    try:
        appointment_insert = f"""
        INSERT INTO `appointments` (`id`, `patient_id`, `doctor_id`, `service_location_id`, `waitinglist_id`, `appointment_description_id`, `dictation_id`, `appointment_date`, `start_time`, `end_time`, `templates_id`, `letter_id`, `referral_id`, `appointment_notes`, `invoice_id`, `sms_flag`, `mail_flag`, `fax_flag`, `reappointment`, `reappointment_date`, `reappointment_start_time`, `reappointment_end_time`, `appointment_status_id`, `episode_id`, `meeting_id`, `reminders`, `patient_portal_id`, `is_rescheduled`, `sync_treatment_id`, `sync_treatment_review_det_id`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`, PPM_Appointment_Id) 
        VALUES (
        {safe_value(row['appointment_id'])}, 
        {safe_value(row['patient_id'])}, 
        {safe_value(row['doctor_id'])}, 
        {safe_value(row['hospital_id'])}, 
        NULL, 
        {safe_value(row['appointment_type_id'])}, 
        NULL, 
        {safe_value(row['AppointmentDate'])}, 
        {safe_value(row['StartTime'])}, 
        {safe_value(row['EndTime'])}, 
        NULL, NULL, NULL, 
        {safe_value(row['Text'])}, 
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
        {safe_value(row['appointment_status_id'])}, 
        {safe_value(row['episode_id'])}, 
        NULL, 0, NULL, NULL, NULL, NULL, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,
        {safe_value(row['ID'])}
        );
        """
        target_cursor.execute(appointment_insert)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
appointment_bar.close()
print("Appointments inserted successfully.")