import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_documents = 'SELECT * FROM ExternalDocuments'
src_documents_df = pd.read_sql(src_documents, get_src_accessdb_connection())

src_documents_df = src_documents_df[~src_documents_df['DocFolder'].isna()]

#--------------------------patient mapping---------------------
tgt_patient = 'SELECT id as patient_id,PPM_Patient_Id FROM patients WHERE PPM_Patient_Id IS NOT NULL'
tgt_patient_df = pd.read_sql(tgt_patient, myconnection)
tgt_patient_df['PPM_Patient_Id'] = tgt_patient_df['PPM_Patient_Id'].astype(int)
src_documents_df['PatientCode'] = src_documents_df['PatientCode'].astype(int)
landing_documents_df = dd.merge(src_documents_df, tgt_patient_df, left_on='PatientCode', right_on='PPM_Patient_Id', how='left')

#--------------------------dropping None patients rows-----------------
landing_documents_df = landing_documents_df[~landing_documents_df['patient_id'].isna()]

#---------------------------document date---------------------
def documentDate(row):
    if pd.isna(row['DocDate']):
        return None
    else:
        return row['DocDate'].strftime('%Y-%m-%d')
landing_documents_df['DocDate'] = landing_documents_df.apply(documentDate, axis=1)

#------------------------------needed columns---------------------
landing_documents_df1 = landing_documents_df[['ID', 'DocDate', 'patient_id', 'DocDescription']]

#----------------------scan_id generation-----------------
scan_max = 'SELECT MAX(id) FROM scan_documents'
scan_max_df = pd.read_sql(scan_max,myconnection)
if scan_max_df is None or scan_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = scan_max_df.iloc[0, 0] + 1
landing_documents_df1.insert(0,'scan_id',range(max_id,max_id+len(landing_documents_df1)))

#----------------------episode mapping-----------------
landing_documents_df1['patient_id'] = landing_documents_df1['patient_id'].astype(int)
tgt_episode_df = pd.read_sql("SELECT id as episode_id,patient_id FROM episodes WHERE episodes.name = 'General'", myconnection)
tgt_episode_df['patient_id'] = tgt_episode_df['patient_id'].astype(int)
landing_scan_df2 = dd.merge(landing_documents_df1, tgt_episode_df, on='patient_id', how='left')

#----------------------adding source identifier column in target-----------------
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE scan_documents ADD COLUMN IF NOT EXISTS PPM_External_Scan_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#----------------------filtering out rows already present in target database -----------------
# Convert ID to string for comparison
landing_scan_df2['ID'] = landing_scan_df2['ID'].astype(str)
tgt_scan_df = pd.read_sql("SELECT PPM_External_Scan_Id FROM scan_documents WHERE PPM_External_Scan_Id IS NOT NULL", myconnection) 
tgt_scan_df['PPM_External_Scan_Id'] = tgt_scan_df['PPM_External_Scan_Id'].astype(str)
# Filtering out rows already present in target database
landing_scan_df2 = landing_scan_df2[~landing_scan_df2['ID'].isin(tgt_scan_df['PPM_External_Scan_Id'].to_list())]

landing_scan_df3 = landing_scan_df2

#----------------------Inserting letters into target database-----------------
scan_bar = tqdm(total=len(landing_scan_df3), desc='Inserting scans')

for index, row in landing_scan_df3.iterrows():
    scan_bar.update(1)
    try:
        scan_query = f"""
        INSERT INTO `scan_documents` (`id`, `patient_id`, `description`, `doctor_id`, `document_date`, `scan_category_id`, `file_extension`, `notes`, `file_path`, `status`, `mail_flag`, `sms_flag`, `fax_flag`, `letter_flag`, `patient_communication`, `episode_id`, `visible`, `ngupload`, `email_sent_date`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`,PPM_External_Scan_Id) 
        VALUES (
        {safe_value(row['scan_id'])}, 
        {safe_value(row['patient_id'])},
        {safe_value(row['DocDescription'])}, 
        1, 
        {safe_value(row['DocDate'])}, 
        1, '.pdf', 
        NULL, 
        NULL, 4, NULL, NULL, NULL, NULL, NULL, 
        {safe_value(row['episode_id'])}, 
        1, NULL, NULL, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 
        NULL,
        {safe_value(row['ID'])}
        );
        """
        target_cursor.execute(scan_query)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
scan_bar.close()
print('Scans inserted successfully.')

