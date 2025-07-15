import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_solicitor = 'SELECT * FROM CodeSolicitors'

try:
    src_solicitor_df = pd.read_sql(src_solicitor, get_src_accessdb2_connection())
except:
    src_solicitor_df = pd.read_sql(src_solicitor, get_src_accessdb_connection())

#Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE contacts ADD COLUMN IF NOT EXISTS PPM_solicitor_Id VARCHAR(100) DEFAULT NULL;"
query_3 = "ALTER TABLE contacts ADD COLUMN IF NOT EXISTS PPM_solicitor VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
target_cursor.execute(query_3)
myconnection.commit()

#solicitor_id generation
solicitor_max = 'SELECT MAX(id) FROM contacts'
solicitor_max_df = pd.read_sql(solicitor_max,myconnection)
if solicitor_max_df is None or solicitor_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = solicitor_max_df.iloc[0, 0] + 1
src_solicitor_df.insert(0,'solicitor_id',range(max_id,max_id+len(src_solicitor_df)))

tgt_title = 'SELECT id as title_id, name as title_name FROM titles'
tgt_title_df = pd.read_sql(tgt_title, myconnection)

solicitor_df = pd.merge(src_solicitor_df, tgt_title_df, left_on='SolicitorsTitle', right_on='title_name', how='left')
solicitor_df['title_id'] = solicitor_df['title_id'].fillna(0).astype(int)

bar = tqdm(total=len(src_solicitor_df),desc='Inserting solicitors',position=-1)

tgt_solicitor_df = pd.read_sql('SELECT DISTINCT PPM_solicitor_Id FROM contacts', myconnection)

#solicitor_df['display_name'] =  solicitor_df['SolicitorsInitials'].fillna('') + ' ' + solicitor_df['SolicitorsName'].fillna('') 

def getDisplayName(row):
    if pd.isna(row['SolicitorsInitials']):
        return row['SolicitorPracticeName']
    else:
        return (row['SolicitorsInitials'] if pd.notna(row['SolicitorsInitials']) else '') + ' ' + (row['SolicitorsName'] if pd.notna(row['SolicitorsName']) else '')
    
solicitor_df['display_name'] = solicitor_df.apply(getDisplayName, axis=1)

if solicitor_df.empty:
    # Just add empty columns with None (or np.nan)
    solicitor_df['first_name'] = None
    solicitor_df['sur_name'] = None
else:
    def firstNameAndSurname(row):
        if pd.isna(row['SolicitorsInitials']):
            if pd.notna(row['SolicitorPracticeName']):
                parts = row['SolicitorPracticeName'].split()
                first_name = parts[0] if parts else ''
                sur_name = ' '.join(parts[1:]) if len(parts) > 1 else ''
            else:
                first_name = ''
                sur_name = ''
        else:
            first_name = row['SolicitorsInitials'] if pd.notna(row['SolicitorsInitials']) else ''
            sur_name = row['SolicitorsName'] if pd.notna(row['SolicitorsName']) else ''
        return pd.Series([first_name, sur_name])

    solicitor_df[['first_name', 'sur_name']] = solicitor_df.apply(firstNameAndSurname, axis=1)

#--------------------------filtering out solicitors already present in target--------------------------
# Convert both columns to the same data type before filtering
solicitor_df['SolicitorCode'] = solicitor_df['SolicitorCode'].astype(str)
tgt_solicitor_df['PPM_solicitor_Id'] = tgt_solicitor_df['PPM_solicitor_Id'].astype(str)
solicitor_df = solicitor_df[~solicitor_df['SolicitorCode'].isin(tgt_solicitor_df['PPM_solicitor_Id'])]

for index, row in solicitor_df.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `contacts` (id,`contact_type_id`, `title_id`, `first_name`, `sur_name`, `display_name`, `professional_title`, `entity_name`, `address1`, `address2`, `address3`, `town`, `county`, `postcode`, `work_phone`, `home_phone`, `mobile`, `email`, `website`, `fax`, `is_archive`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`,PPM_solicitor_Id, PPM_solicitor) 
        VALUES (
        {row['solicitor_id']},
        4, 
        {safe_value(row['title_id'])}, 
        {safe_value(row['first_name'])}, 
        {safe_value(row['sur_name'])}, 
        {safe_value(row['display_name'])}, 
        NULL, 
        {safe_value(row['SolicitorPracticeName'])}, 
        {safe_value(row['SolicitorAddress1'])}, 
        {safe_value(row['SolicitorAddress2'])}, 
        {safe_value(row['SolicitorAddress3'])}, 
        {safe_value(row['SolicitorAddress4'])}, 
        {safe_value(row['SolicitorAddress5'])}, 
        {safe_value(row['SolicitorPostCode'])}, 
        {safe_value(row['SolicitorTelNo1'])},
        {safe_value(row['SolicitorTelNo2'])},
        {safe_value(row['SolicitorMobile'])}, 
        NULL, NULL, 
        {safe_value(row['SolicitorFaxNumber'])}, 
        0, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,
        {safe_value(row['SolicitorCode'])},
        {safe_value(row['SolicitorPracticeName'] if row['SolicitorPracticeName'] == row['display_name'] else None)}
        );
        """
        target_cursor.execute(query)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break


myconnection.commit()
myconnection.close()
bar.close()
print("Solicitors inserted successfully.")
