from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_solicitor = 'SELECT DISTINCT AccountName,AccountAddress1,AccountAddress2,AccountAddress3,AccountAddress4,AccountAddress5,AccountPostCode,AccountContactTelNo FROM InvoiceHeadSummary WHERE InvoiceTo = 6'
src_solicitor_df = pd.read_sql(src_solicitor, get_src_accessdb_connection())

src_solicitor_df = src_solicitor_df.drop_duplicates(subset=['AccountName'])

def firstNameAndSurname(row):
    first_name = row['AccountName'].split(' ')[0] if pd.notna(row['AccountName']) else ''
    sur_name = ' '.join(row['AccountName'].split(' ')[1:]) if pd.notna(row['AccountName']) and len(row['AccountName'].split(' ')) > 1 else ''
    return pd.Series([first_name, sur_name])

src_solicitor_df[['first_name', 'sur_name']] = src_solicitor_df.apply(firstNameAndSurname, axis=1)

#tgt_solicitor_df = pd.read_sql('SELECT DISTINCT UPPER(LTRIM(RTRIM(PPM_solicitor))) as PPM_solicitor FROM contacts', myconnection)

#solicitor_id generation
solicitor_max = 'SELECT MAX(id) FROM contacts'
solicitor_max_df = pd.read_sql(solicitor_max,myconnection)
if solicitor_max_df is None or solicitor_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = solicitor_max_df.iloc[0, 0] + 1
src_solicitor_df.insert(0,'solicitor_id',range(max_id,max_id+len(src_solicitor_df)))

#------------------------filtering out solicitors already present in target--------------------------
# Convert both columns to the same data type before filtering
src_solicitor_df['AccountName'] = src_solicitor_df['AccountName'].astype(str)
tgt_solicitor_df = pd.read_sql('SELECT DISTINCT PPM_solicitor FROM contacts', myconnection)
tgt_solicitor_df['PPM_solicitor'] = tgt_solicitor_df['PPM_solicitor'].astype(str)
src_solicitor_df = src_solicitor_df[~src_solicitor_df['AccountName'].isin(tgt_solicitor_df['PPM_solicitor'])] 

bar = tqdm(total=len(src_solicitor_df), desc='Inserting Solicitors from InvoiceHeadSummary', position=0)

for index, row in src_solicitor_df.iterrows():
    bar.update(1)
    #if row['AccountName'].upper().strip() not in tgt_solicitor_df['PPM_solicitor'].values:
    try:
        query = f"""
        INSERT INTO `contacts` (id,`contact_type_id`, `title_id`, `first_name`, `sur_name`, `display_name`, `professional_title`, `entity_name`, `address1`, `address2`, `address3`, `town`, `county`, `postcode`, `work_phone`, `home_phone`, `mobile`, `email`, `website`, `fax`, `is_archive`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`,PPM_solicitor_Id,PPM_solicitor) 
        VALUES (
        {row['solicitor_id']},
        4, 
        0, 
        {safe_value(row['first_name'])}, 
        {safe_value(row['sur_name'])}, 
        {safe_value(row['AccountName'])}, 
        NULL, 
        {safe_value(row['AccountName'])}, 
        {safe_value(row['AccountAddress1'])}, 
        {safe_value(row['AccountAddress2'])}, 
        {safe_value(row['AccountAddress3'])}, 
        {safe_value(row['AccountAddress4'])}, 
        {safe_value(row['AccountAddress5'])}, 
        {safe_value(row['AccountPostCode'])}, 
        {safe_value(row['AccountContactTelNo'])},
        NULL, NULL, NULL, NULL, NULL, 
        0, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,
        'From_InvoiceHeadSummary',
        {safe_value(row['AccountName'])}
        );
        """
        target_cursor.execute(query)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break
    #else:
        #continue

myconnection.commit()
myconnection.close()
bar.close()
print("Solicitors inserted successfully from InvoiceHeadSummary.")
