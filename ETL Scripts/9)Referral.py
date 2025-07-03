from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_referral = 'SELECT * FROM CodeSpecialists'
src_referral_df = pd.read_sql(src_referral, get_src_accessdb2_connection())

print(src_referral_df.columns)

#Adding Source identifier column in target
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE contacts ADD COLUMN IF NOT EXISTS PPM_referral_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#solicitor_id generation
referral_max = 'SELECT MAX(id) FROM contacts'
referral_max_df = pd.read_sql(referral_max,myconnection)
if referral_max_df is None or referral_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = referral_max_df.iloc[0, 0] + 1
src_referral_df.insert(0,'referral_id',range(max_id,max_id+len(src_referral_df)))

tgt_title = 'SELECT id as title_id, name as title_name FROM titles'
tgt_title_df = pd.read_sql(tgt_title, myconnection)

referral_df = dd.merge(src_referral_df, tgt_title_df, left_on='SpecialistTitle', right_on='title_name', how='left')
referral_df['title_id'] = referral_df['title_id'].fillna(0).astype(int)

referral_df['display_name'] =  (referral_df['SpecialistForeName'].fillna('') + ' ' + referral_df['SpecialistName']).str.strip()

bar = tqdm(total=len(src_referral_df),desc='Inserting referral',position=0)

tgt_referral_df = pd.read_sql('SELECT DISTINCT PPM_referral_Id FROM contacts', myconnection)

for index, row in referral_df.iterrows():
    bar.update(1)
    if row['SpecialistCode'] not in tgt_referral_df['PPM_referral_Id'].values:
        try:
            query = f"""
            INSERT INTO `contacts` (id,`contact_type_id`, `title_id`, `first_name`, `sur_name`, `display_name`, `professional_title`, `entity_name`, `address1`, `address2`, `address3`, `town`, `county`, `postcode`, `work_phone`, `home_phone`, `mobile`, `email`, `website`, `fax`, `is_archive`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`,PPM_referral_Id) 
            VALUES (
            {row['referral_id']},
            5, 
            {safe_value(row['title_id'])}, 
            {safe_value(row['SpecialistForeName'] if pd.notna(row['SpecialistForeName']) else '')}, 
            {safe_value(row['SpecialistName'])}, 
            {safe_value(row['display_name'])}, 
            NULL, 
            {safe_value(row['SpecialistSpeciality'])}, 
            {safe_value(row['SpecialistAddress1'])}, 
            {safe_value(row['SpecialistAddress2'])}, 
            {safe_value(row['SpecialistAddress3'])}, 
            {safe_value(row['SpecialistAddress4'])}, 
            {safe_value(row['SpecialistAddress5'])}, 
            {safe_value(row['SpecialistPostCode'])}, 
            {safe_value(row['SpecialistTelNo1'])},
            {safe_value(row['SpecialistTelNo2'])},
            {safe_value(row['SpecialistMobile'])}, 
            NULL, NULL, 
            {safe_value(row['SpecialistFaxNo'])}, 
            0, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,
            {safe_value(row['SpecialistCode'])}
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
print("referral inserted successfully.")
