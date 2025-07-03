from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

# Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE patient_contact_details ADD COLUMN IF NOT EXISTS PPM_PatCon VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

src_patient = 'SELECT * FROM CodePatients'
src_patient_df = pd.read_sql(src_patient, get_src_accessdb_connection())

src_patient_df1 = src_patient_df[['PatientCode','GPCode','GP1Code','Solicitor1Code']]
src_patient_df2 = src_patient_df1.dropna(subset=['GPCode','GP1Code','Solicitor1Code'], how='all').reset_index(drop=True)

src_patient_df2 = src_patient_df2.apply(pd.to_numeric, errors='coerce').astype('Int64')

tgt_patient = 'SELECT id as patient_id, PPM_Patient_Id FROM patients'
tgt_patient_df = pd.read_sql(tgt_patient, myconnection)

tgt_patient_df['PPM_Patient_Id'] = tgt_patient_df['PPM_Patient_Id'].astype(int)

tgt_contact_gp1 = 'SELECT id as gp_contact_id1, 1 as prim, PPM_GP_Id as PPM_GP_Id1 FROM contacts WHERE contact_type_id = 3'
tgt_contact_gp_df1 = pd.read_sql(tgt_contact_gp1, myconnection)

tgt_contact_gp_df1['PPM_GP_Id1'] = tgt_contact_gp_df1['PPM_GP_Id1'].astype(int)

tgt_contact_gp2 = 'SELECT id as gp_contact_id2, 0 as prim, PPM_GP_Id as PPM_GP_Id2 FROM contacts WHERE contact_type_id = 3'
tgt_contact_gp_df2 = pd.read_sql(tgt_contact_gp2, myconnection)

tgt_contact_gp_df2['PPM_GP_Id2'] = tgt_contact_gp_df2['PPM_GP_Id2'].astype(int)

tgt_solicitor_df = 'SELECT id as solicitor_contact_id, PPM_solicitor_Id FROM contacts WHERE contact_type_id = 4 AND PPM_solicitor_Id != "From_InvoiceHeadSummary"'
tgt_solicitor_df = pd.read_sql(tgt_solicitor_df, myconnection)

tgt_solicitor_df['PPM_solicitor_Id'] = tgt_solicitor_df['PPM_solicitor_Id'].astype(int)

src_patient_df3 = pd.merge(src_patient_df2, tgt_patient_df, left_on='PatientCode', right_on='PPM_Patient_Id', how='left')
src_patient_df4 = pd.merge(src_patient_df3, tgt_contact_gp_df1, left_on='GPCode', right_on='PPM_GP_Id1', how='left')
src_patient_df5 = pd.merge(src_patient_df4, tgt_contact_gp_df2, left_on='GP1Code', right_on='PPM_GP_Id2', how='left')
src_patient_df6 = pd.merge(src_patient_df5, tgt_solicitor_df, left_on='Solicitor1Code', right_on='PPM_solicitor_Id', how='left')

src_patient_df7 = src_patient_df6.drop(columns=['PPM_Patient_Id', 'PPM_GP_Id1', 'PPM_GP_Id2', 'PPM_solicitor_Id', 'GPCode', 'GP1Code', 'Solicitor1Code'])

pat_con_df = pd.melt(src_patient_df7, id_vars=['patient_id'], value_vars=['gp_contact_id1', 'gp_contact_id2', 'solicitor_contact_id'], var_name='contact_type', value_name='contact_id')

pat_con_df1 = pat_con_df.dropna(subset=['contact_id']).reset_index(drop=True)

def get_contact_type(row):
    if row['contact_type'] == 'gp_contact_id1':
        return pd.Series([3, 1])  # Primary GP
    elif row['contact_type'] == 'gp_contact_id2':
        return pd.Series([3, 0])  # Secondary GP
    elif row['contact_type'] == 'solicitor_contact_id':
        return pd.Series([4, 0])  # Solicitor
    else:
        return pd.Series([None, None])  # Default case

pat_con_df1[['contact_type_id', 'prim']] = pat_con_df1.apply(get_contact_type, axis=1)

pat_con_df1 = pat_con_df1.drop(columns=['contact_type'])
pat_con_df1['contact_id'] = pat_con_df1['contact_id'].astype('Int64')

bar = tqdm(total=len(pat_con_df1), desc='Inserting Patient Contact Details', position=0)

for index, row in pat_con_df1.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `patient_contact_details` (`patient_id`, `contact_id`, `contact_type_id`, `referred_on_date`, `expiry_on_date`, `primary`, `auto_sendmail_letter`, `created_at`, `updated_at`, `deleted_at`,PPM_PatCon) 
        VALUES (
        {safe_value(row['patient_id'])}, 
        {safe_value(row['contact_id'])}, 
        {safe_value(row['contact_type_id'])}, 
        NULL, NULL,
        {safe_value(row['prim'])}, 
        NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,
        1
        );
        """
        target_cursor.execute(query)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print("Patient contact details inserted successfully.")