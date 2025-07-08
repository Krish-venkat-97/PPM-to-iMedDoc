from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_billTo_df = pd.DataFrame(
    ('GP','Hospital','Specalist'),
    columns=['billTo']
)

#---------------------id generation-------------------
billTo_max = 'SELECT MAX(id) FROM bill_to'
billTo_max_df = pd.read_sql(billTo_max, myconnection)
if billTo_max_df is None or billTo_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = billTo_max_df.iloc[0, 0] + 1
src_billTo_df.insert(0, 'bill_to_id', range(max_id, max_id + len(src_billTo_df)))

#------------------------filtering out existing billTo-------------------
tgt_billTo_df = pd.read_sql("SELECT id as bill_to_id, name as billTo FROM bill_to", myconnection)
src_billTo_df['billTo_Upper'] = src_billTo_df['billTo'].str.upper().str.strip()
src_billTo_df = src_billTo_df[~src_billTo_df['billTo_Upper'].isin(tgt_billTo_df['billTo'].str.upper().str.strip())]
src_billTo_df = src_billTo_df.drop(columns=['billTo_Upper']).reset_index(drop=True)

#-----------------------inserting billTo into target database---------------------
bar = tqdm(total=len(src_billTo_df), desc='Inserting Bill To')

for index,row in src_billTo_df.iterrows():
    bar.update(1)
    try:
        billTo_query = f"""
        INSERT INTO `bill_to` (`id`, `name`, `order`, `is_default`, `created_at`, `updated_at`, `deleted_at`) 
        VALUES ({safe_value(row['bill_to_id'])}, {safe_value(row['billTo'])}, {safe_value(row['bill_to_id'])}, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL); 
        """
        target_cursor.execute(billTo_query)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print("Bill To data inserted successfully.")