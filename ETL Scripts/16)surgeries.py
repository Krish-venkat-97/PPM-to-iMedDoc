from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_surgery= 'SELECT * FROM DiaryEx'
src_surgery_df = pd.read_sql(src_surgery, get_src_accessdb_connection())

src_surgery_df1 = src_surgery_df[src_surgery_df['AppointmentType'].str.upper().str.strip() == 'THEATRE']
src_surgery_df1['AppointmentType'] = src_surgery_df1['AppointmentType'].fillna('Not defined SurgeryType').str.strip()
src_surgery_df1['AppointmentType'] = src_surgery_df1['AppointmentType'].replace('', 'Not defined SurgeryType').str.strip()

# Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE surgeries ADD COLUMN IF NOT EXISTS PPM_Surgery_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#get AppointmentDate
def get_appointment_date(row):
    if pd.isna(row['StartDate']):
        return None
    else:
        return row['StartDate'].strftime('%Y-%m-%d')
    
src_surgery_df1['SurgeryDate'] = src_surgery_df1.apply(get_appointment_date, axis=1)

def getStartTime(row):
    if pd.isna(row['StartDate']):
        return None
    else:
        a = row['StartDate'].strftime('%H:%M:%S')
        b = '1970-02-01 ' + a
        return b

src_surgery_df1['StartTime'] = src_surgery_df1.apply(getStartTime, axis=1)

#add duration to the starttime 
def getEndTime(row):
    if pd.isna(row['StartDate']):
        return None
    else:
        a = row['StartDate'] + pd.Timedelta(minutes=row['Duration'])
        b = a.strftime('%H:%M:%S')
        c = '1970-02-01 ' + b
        return c
    
src_surgery_df1['EndTime'] = src_surgery_df1.apply(getEndTime, axis=1)

#---------------------hospital mapping---------------------
src_surgery_df1['LocationCode'] = src_surgery_df1['LocationCode'].astype(int)
tgt_hospital_df = pd.read_sql("SELECT id as hospital_id,PPM_Hospital_Id FROM hospitals WHERE PPM_Hospital_Id IS NOT NULL", myconnection)
tgt_hospital_df['PPM_Hospital_Id'] = tgt_hospital_df['PPM_Hospital_Id'].astype(int)
landing_surgery_df1 = dd.merge(src_surgery_df1,tgt_hospital_df, left_on='LocationCode', right_on='PPM_Hospital_Id', how='left')

#---------------------appointment type mapping---------------------
landing_surgery_df1['AppointmentType_Upper'] = landing_surgery_df1['Text'].str.upper().str.strip()
tgt_procedure_df = pd.read_sql("SELECT id as procedure_id,UPPER(LTRIM(RTRIM(name))) as AppointmentType_Upper FROM procedures WHERE PPM_ApptDesc_Id IS NOT NULL", myconnection)
tgt_procedure_df['AppointmentType_Upper'] = tgt_procedure_df['AppointmentType_Upper'].astype(str)
landing_surgery_df2 = dd.merge(landing_surgery_df1, tgt_procedure_df, on='AppointmentType_Upper', how='left')

#---------------------doctor mapping---------------------
landing_surgery_df2['ResourceCode'] = landing_surgery_df2['ResourceCode'].astype(int)
tgt_doctor_df = pd.read_sql("SELECT id as doctor_id,PPM_Doctor_Id FROM doctors WHERE PPM_Doctor_Id IS NOT NULL", myconnection)
tgt_doctor_df['PPM_Doctor_Id'] = tgt_doctor_df['PPM_Doctor_Id'].astype(int)
landing_surgery_df3 = dd.merge(landing_surgery_df2, tgt_doctor_df, left_on='ResourceCode', right_on='PPM_Doctor_Id', how='left')

#---------------------patient mapping---------------------
landing_surgery_df3['PatientCode'] = landing_surgery_df3['PatientCode'].astype(int)
tgt_patient_df = pd.read_sql("SELECT id as patient_id,PPM_Patient_Id FROM patients WHERE PPM_Patient_Id IS NOT NULL", myconnection)
tgt_patient_df['PPM_Patient_Id'] = tgt_patient_df['PPM_Patient_Id'].astype(int)
landing_surgery_df4 = dd.merge(landing_surgery_df3, tgt_patient_df, left_on='PatientCode', right_on='PPM_Patient_Id', how='left')

#---------------------appointment status mapping---------------------
landing_surgery_df4['surgery_status_id'] = landing_surgery_df4.apply(
    lambda x: 1 if x['SurgeryDate'] is not None and pd.to_datetime(x['SurgeryDate']).date() >= datetime.now().date() else 4,
    axis=1
)

#--------------------Filtering out rows with null patient_id---------------------
landing_surgery_df5 = landing_surgery_df4[landing_surgery_df4['patient_id'].notnull()]

#---------------------episode mapping---------------------
landing_surgery_df5['patient_id'] = landing_surgery_df5['patient_id'].astype(int)
tgt_episode_df = pd.read_sql("SELECT id as episode_id,patient_id FROM episodes WHERE name = 'General'", myconnection)
tgt_episode_df['patient_id'] = tgt_episode_df['patient_id'].astype(int)
landing_surgery_df5 = dd.merge(landing_surgery_df5, tgt_episode_df, on='patient_id', how='left')

#---------------------Missing values handling---------------------
landing_surgery_df5['doctor_id'] = landing_surgery_df5['doctor_id'].fillna(1).astype(int)
landing_surgery_df5['hospital_id'] = landing_surgery_df5['hospital_id'].fillna(1).astype(int)

#---------------------Dropping unnecessary columns---------------------
landing_surgery_df5 = landing_surgery_df5.loc[:, ~landing_surgery_df5.columns.str.contains('ppm', case=False)]

#---------------------Keeping only required columns---------------------
landing_surgery_df6 = landing_surgery_df5[['ID', 'patient_id', 'doctor_id', 'hospital_id', 'procedure_id', 'episode_id','surgery_status_id', 'Text', 'SurgeryDate','StartTime','Duration','EndTime']]

#----------------------Generating id for appointments---------------------
surgery_max = 'SELECT MAX(id) FROM surgeries'
surgery_max_df = pd.read_sql(surgery_max,myconnection)
if surgery_max_df is None or surgery_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = surgery_max_df.iloc[0, 0] + 1
landing_surgery_df6.insert(0,'surgery_id',range(max_id,max_id+len(landing_surgery_df6)))

#---------------------filtering out rows already present in target database ---------------------
landing_surgery_df7 = landing_surgery_df6[~landing_surgery_df6['ID'].isin(pd.read_sql("SELECT PPM_Surgery_Id FROM surgeries WHERE PPM_Surgery_Id IS NOT NULL", myconnection)['PPM_Surgery_Id'])]

#---------------------Inserting appointments into target database---------------------
surgery_bar = tqdm(total = len(landing_surgery_df7), desc='Inserting surgeries')

for index, row in landing_surgery_df7.iterrows():
    surgery_bar.update(1)
    try:
        surgery_insert = f"""
        INSERT INTO `surgeries` (`id`, `patient_id`, `doctor_id`, `dictation_id`, `appointment_id`, `service_hospital_id`, `waitinglist_id`, `procedure_id`, `procedure2_id`, `procedure3_id`, `procedure4_id`, `procedure5_id`, `procedure6_id`, `procedure7_id`, `procedure8_id`, `admission_date`, `admission_time`, `surgery_date`, `start_time`, `end_time`, `discharge_date`, `discharge_time`, `templates_id`, `letter_id`, `referral_id`, `surgery_notes`, `side_list`, `invoice_id`, `sms_flag`, `mail_flag`, `fax_flag`, `readmission`, `readmission_date`, `readmission_time`, `resurgery_date`, `resurgery_start_time`, `resurgery_end_time`, `surgery_status_id`, `episode_id`, `contact_id`, `staff_nurse_id`, `claim_status`, `hospital_episode_no`, `ssc_claim_status`, `episode_ref_no`, `episode_updated_date`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`, PPM_Surgery_Id) 
        VALUES (
        {safe_value(row['surgery_id'])}, 
        {safe_value(row['patient_id'])}, 
        {safe_value(row['doctor_id'])}, 
        NULL, 
        NULL, 
        {safe_value(row['hospital_id'])}, 
        NULL, 
        {safe_value(row['procedure_id'])}, 
        NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        {safe_value(row['SurgeryDate'])}, 
        {safe_value(row['StartTime'])}, 
        {safe_value(row['SurgeryDate'])}, 
        {safe_value(row['StartTime'])}, 
        {safe_value(row['EndTime'])}, 
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 
        {safe_value(row['surgery_status_id'])}, 
        {safe_value(row['episode_id'])}, 
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,
        {safe_value(row['ID'])}
        );
        """
        target_cursor.execute(surgery_insert)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
surgery_bar.close()
print('Surgeries inserted successfully.')