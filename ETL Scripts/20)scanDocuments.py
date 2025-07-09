from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_documents = 'SELECT * FROM PatientDocHistory'
src_documents_df = pd.read_sql(src_documents, get_src_accessdb_connection())

def getFileExtension(filename):
    if pd.isna(filename):
        return None
    else:
        return os.path.splitext(filename)[1].lower()
    
src_documents_df['FileExtension'] = src_documents_df['DocFileName'].apply(getFileExtension)

src_scan_df = src_documents_df[~src_documents_df['FileExtension'].isin(['.doc', '.docx', '.rtf'])]

#--------------------filtering out the rows if DocFileName is empty----------------
src_scan_df = src_scan_df[~src_scan_df['DocFileName'].isna()]

def letterDate(row):
    if pd.isna(row['DocDate']):
        return None
    else:
        return row['DocDate'].strftime('%Y-%m-%d')

src_scan_df['LetterDate'] = src_scan_df.apply(letterDate, axis=1)

#----------------------patient mapping---------------------
src_scan_df['PatientCode'] = src_scan_df['PatientCode'].astype(int)
tgt_patient_df = pd.read_sql("SELECT id as patient_id, PPM_Patient_Id FROM patients WHERE PPM_Patient_Id IS NOT NULL", myconnection)
tgt_patient_df['PPM_Patient_Id'] = tgt_patient_df['PPM_Patient_Id'].astype(int)
landing_scan_df = dd.merge(src_scan_df, tgt_patient_df, left_on='PatientCode', right_on='PPM_Patient_Id', how='left')

#----------------------dropping None patients rows-----------------
landing_scan_df = landing_scan_df[~landing_scan_df['patient_id'].isna()]

#----------------------document description-----------------
def getLetterDescription(row):
    if pd.isna(row['DocDescription']):
        return None
    else:
        return ('DocType: '+row['DocType'].strip()+'; DocDescription: '+row['DocDescription'].strip())

landing_scan_df['LetterDescription'] = landing_scan_df.apply(getLetterDescription, axis=1)

#----------------------needed columns-----------------
landing_scan_df1 = landing_scan_df[['ID','LetterDate', 'patient_id','LetterDescription']]

#----------------------adding source identifier column in target-----------------
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE scan_documents ADD COLUMN IF NOT EXISTS PPM_Scan_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#----------------------scan_id generation-----------------
scan_max = 'SELECT MAX(id) FROM scan_documents'
scan_max_df = pd.read_sql(scan_max,myconnection)
if scan_max_df is None or scan_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = scan_max_df.iloc[0, 0] + 1
landing_scan_df1.insert(0,'scan_id',range(max_id,max_id+len(landing_scan_df1)))

#----------------------episode mapping-----------------
landing_scan_df1['patient_id'] = landing_scan_df1['patient_id'].astype(int)
tgt_episode_df = pd.read_sql("SELECT id as episode_id,patient_id FROM episodes WHERE episodes.name = 'General'", myconnection)
tgt_episode_df['patient_id'] = tgt_episode_df['patient_id'].astype(int)
landing_scan_df2 = dd.merge(landing_scan_df1, tgt_episode_df, on='patient_id', how='left')

#----------------------filtering out rows already present in target database -----------------
landing_scan_df3 = landing_scan_df2[~landing_scan_df2['ID'].isin(pd.read_sql("SELECT PPM_Scan_Id FROM scan_documents WHERE PPM_Scan_Id IS NOT NULL", myconnection)['PPM_Scan_Id'])]

#----------------------Inserting letters into target database-----------------
scan_bar = tqdm(total=len(landing_scan_df3), desc='Inserting scans')

for index, row in landing_scan_df3.iterrows():
    scan_bar.update(1)
    try:
        scan_query = f"""
        INSERT INTO `scan_documents` (`id`, `patient_id`, `description`, `doctor_id`, `document_date`, `scan_category_id`, `file_extension`, `notes`, `file_path`, `status`, `mail_flag`, `sms_flag`, `fax_flag`, `letter_flag`, `patient_communication`, `episode_id`, `visible`, `ngupload`, `email_sent_date`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`,PPM_Scan_Id) 
        VALUES (
        {safe_value(row['scan_id'])}, 
        {safe_value(row['patient_id'])},
        {safe_value(row['LetterDescription'])}, 
        1, 
        {safe_value(row['LetterDate'])}, 
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

