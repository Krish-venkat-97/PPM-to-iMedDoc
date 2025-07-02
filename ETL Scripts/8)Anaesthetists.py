from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_anaesth = 'SELECT * FROM CodeAnaesthetists'
src_anaesth_df = pd.read_sql(src_anaesth, get_src_accessdb2_connection())

#Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE contacts ADD COLUMN IF NOT EXISTS PPM_Anaesth_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#solicitor_id generation
anaesth_max = 'SELECT MAX(id) FROM contacts'
anaesth_max_df = pd.read_sql(anaesth_max,myconnection)
if anaesth_max_df is None or anaesth_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = anaesth_max_df.iloc[0, 0] + 1
src_anaesth_df.insert(0,'anaesth_id',range(max_id,max_id+len(src_anaesth_df)))

tgt_title = 'SELECT id as title_id, name as title_name FROM titles'
tgt_title_df = pd.read_sql(tgt_title, myconnection)

anaesth_df = dd.merge(src_anaesth_df, tgt_title_df, left_on='AnaesthetistTitle', right_on='title_name', how='left')
anaesth_df['title_id'] = anaesth_df['title_id'].fillna(0).astype(int)

anaesth_df['display_name'] =  (anaesth_df['AnaesthetistForename'].fillna('') + ' ' + anaesth_df['AnaesthetistName']).str.strip()

bar = tqdm(total=len(src_anaesth_df),desc='Inserting anaesth',position=0)

tgt_solicitor_df = pd.read_sql('SELECT DISTINCT PPM_Anaesth_Id FROM contacts', myconnection)

for index, row in anaesth_df.iterrows():
    bar.update(1)
    if row['AnaesthetistCode'] not in tgt_solicitor_df['PPM_Anaesth_Id'].values:
        try:
            query = f"""
            INSERT INTO `contacts` (id,`contact_type_id`, `title_id`, `first_name`, `sur_name`, `display_name`, `professional_title`, `entity_name`, `address1`, `address2`, `address3`, `town`, `county`, `postcode`, `work_phone`, `home_phone`, `mobile`, `email`, `website`, `fax`, `is_archive`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`,PPM_GP_Id) 
            VALUES (
            {row['anaesth_id']},
            5, 
            {safe_value(row['title_id'])}, 
            {safe_value(row['AnaesthetistForename'] if pd.notna(row['AnaesthetistForename']) else '')}, 
            {safe_value(row['AnaesthetistName'])}, 
            {safe_value(row['display_name'])}, 
            NULL, 
            {safe_value(row['AnaesthetistPractice'])}, 
            {safe_value(row['AnaesthetistAddress1'])}, 
            {safe_value(row['AnaesthetistAddress2'])}, 
            {safe_value(row['AnaesthetistAddress3'])}, 
            {safe_value(row['AnaesthetistAddress4'])}, 
            {safe_value(row['AnaesthetistAddress5'])}, 
            {safe_value(row['AnaesthetistPostCode'])}, 
            {safe_value(row['AnaesthetistTelNo1'])},
            {safe_value(row['AnaesthetistTelNo2'])},
            {safe_value(row['AnaesthetistMobile'])}, 
            NULL, NULL, 
            {safe_value(row['AnaesthetistFaxNo'])}, 
            0, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,
            {safe_value(row['AnaesthetistCode'])}
            );
            """
            target_cursor.execute(query)
        except Exception as e:
            print(f"Error inserting row {index}: {e}")
            break
    else:
        continue

myconnection.commit()
myconnection.close()
bar.close()
print("Anaesthetists inserted successfully.")
