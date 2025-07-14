import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_documents = 'SELECT * FROM PatientDocHistory'
src_documents_df = pd.read_sql(src_documents, get_src_accessdb_connection())

#-----------------------------filterinig out the rows with SubDirectory = None--------------
src_documents_df = src_documents_df[~src_documents_df['SubDirectory'].isna()] 

def getFileExtension(filename):
    if pd.isna(filename):
        return None
    else:
        return os.path.splitext(filename)[1].lower()
    
src_documents_df['FileExtension'] = src_documents_df['DocFileName'].apply(getFileExtension)

src_letter_df = src_documents_df[src_documents_df['FileExtension'].isin(['.doc', '.docx', '.rtf'])]

def letterDate(row):
    if pd.isna(row['DocDate']):
        return None
    else:
        return row['DocDate'].strftime('%Y-%m-%d')

src_letter_df['LetterDate'] = src_letter_df.apply(letterDate, axis=1)

#----------------------patient mapping---------------------
src_letter_df['PatientCode'] = src_letter_df['PatientCode'].astype(int)
tgt_patient_df = pd.read_sql("SELECT id as patient_id, PPM_Patient_Id FROM patients WHERE PPM_Patient_Id IS NOT NULL", myconnection)
tgt_patient_df['PPM_Patient_Id'] = tgt_patient_df['PPM_Patient_Id'].astype(int)
landing_letter_df = dd.merge(src_letter_df, tgt_patient_df, left_on='PatientCode', right_on='PPM_Patient_Id', how='left')

#----------------------dropping None patients rows-----------------
landing_letter_df = landing_letter_df[~landing_letter_df['patient_id'].isna()]

#----------------------document description-----------------
def getLetterDescription(row):
    if pd.isna(row['DocDescription']):
        return None
    else:
        return ('DocType: '+row['DocType'].strip()+'; DocDescription: '+row['DocDescription'].strip())

landing_letter_df['LetterDescription'] = landing_letter_df.apply(getLetterDescription, axis=1)

#----------------------needed columns-----------------
landing_letter_df1 = landing_letter_df[['ID','LetterDate', 'patient_id','LetterDescription']]

#----------------------adding source identifier column in target-----------------
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE letters ADD COLUMN IF NOT EXISTS PPM_Letter_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#----------------------letter_id generation-----------------
letter_max = 'SELECT MAX(id) FROM letters'
letter_max_df = pd.read_sql(letter_max,myconnection)
if letter_max_df is None or letter_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = letter_max_df.iloc[0, 0] + 1
landing_letter_df1.insert(0,'letter_id',range(max_id,max_id+len(landing_letter_df1)))

#----------------------episode mapping-----------------
landing_letter_df1['patient_id'] = landing_letter_df1['patient_id'].astype(int)
tgt_episode_df = pd.read_sql("SELECT id as episode_id,patient_id FROM episodes WHERE episodes.name = 'General'", myconnection)
tgt_episode_df['patient_id'] = tgt_episode_df['patient_id'].astype(int)
landing_letter_df2 = dd.merge(landing_letter_df1, tgt_episode_df, on='patient_id', how='left')

#----------------------filtering out rows already present in target database -----------------
# Convert ID to string for comparison
landing_letter_df2['ID'] = landing_letter_df2['ID'].astype(str)
tgt_letter_df = pd.read_sql("SELECT DISTINCT PPM_Letter_Id FROM letters WHERE PPM_Letter_Id IS NOT NULL", myconnection)
tgt_letter_df['PPM_Letter_Id'] = tgt_letter_df['PPM_Letter_Id'].astype(str)
# Filtering out rows already present in target database
landing_letter_df3 = landing_letter_df2[~landing_letter_df2['ID'].isin(tgt_letter_df['PPM_Letter_Id'])]

#----------------------Inserting letters into target database-----------------
letter_bar = tqdm(total=len(landing_letter_df3), desc='Inserting letters')

for index, row in landing_letter_df3.iterrows():
    letter_bar.update(1)
    try:
        letter_insert = f"""
        INSERT INTO `letters` (`id`, `template_id`, `patient_id`, `doctor_id`, `description`, `letter_category_id`, `episode_id`, `extension`, `mail_flag`, `sms_flag`, `fax_flag`, `letter_flag`, `patient_communication`, `is_word_close`, `new`, `verified`, `printed`, `removed`, `completed`, `emailed`, `finalize`, `from_where`, `external_id`, `document_date`, `type`, `phyalert`, `active`, `workflow_state_id`, `clinic_workflow_id`, `available_editor_id`, `visible`, `ngupload`, `add_from_letter`, `status`, `temp_status`, `contact_type_id`, `contact_id`, `notes`, `dictate_it_key`, `followup`, `dictate_it_followup`, `from_dictate_it`, `skip_printtray`, `verified_date`, `email_sent_date`, `dictation_id`, `speech_to_text_flag`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`, PPM_Letter_Id) 
        VALUES (
        {safe_value(row['letter_id'])}, 
        NULL, 
        {safe_value(row['patient_id'])}, 
        1, 
        {safe_value(row['LetterDescription'])}, 
        1, 
        {safe_value(row['episode_id'])}, 
        'doc', 0, NULL, 0, NULL, NULL, 0, 0, 1, 1, 0, NULL, 0, 0, NULL, 0, 
        {safe_value(row['LetterDate'])}, 
        NULL, NULL, 1, 0, NULL, 2, 0, NULL, NULL, 3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,
        {safe_value(row['ID'])}
        );
        """
        target_cursor.execute(letter_insert)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break
        
myconnection.commit()
myconnection.close()
letter_bar.close()
print('Letters inserted successfully.')