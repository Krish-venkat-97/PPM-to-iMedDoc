import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_patient_titles = 'SELECT DISTINCT Title FROM CodePatients'
src_patient_titles_df = pd.read_sql(src_patient_titles, get_src_accessdb_connection())

src_gp_titles = 'SELECT DISTINCT GPTitle FROM CodeGPs'
src_gp_titles_df = pd.read_sql(src_gp_titles, get_src_accessdb2_connection())

sr_solicitor_titles = 'SELECT DISTINCT SolicitorsTitle FROM CodeSolicitors'
src_solicitor_titles_df = pd.read_sql(sr_solicitor_titles, get_src_accessdb2_connection())

sr_anesthetist_titles = 'SELECT DISTINCT AnaesthetistTitle FROM CodeAnaesthetists'
sr_anesthetist_titles_df = pd.read_sql(sr_anesthetist_titles, get_src_accessdb2_connection())

# Combining all distinct titles into a single DataFrame
titles_df = pd.DataFrame({
    'title': pd.concat([
        src_patient_titles_df['Title'],
        src_gp_titles_df['GPTitle'],
        src_solicitor_titles_df['SolicitorsTitle'],
        sr_anesthetist_titles_df['AnaesthetistTitle']
    ]).drop_duplicates().dropna().reset_index(drop=True)
})

titles_df['title_Upper'] = titles_df['title'].str.upper().str.strip()

tgt_titles = 'SELECT DISTINCT name,UPPER(LTRIM(RTRIM(name))) AS title_Upper FROM titles'
tgt_titles_df = pd.read_sql(tgt_titles, myconnection)

# Filtering out titles that already exist in the target
titles_df1 = titles_df[~titles_df['title_Upper'].isin(tgt_titles_df['title_Upper'])]

bar = tqdm(total=len(titles_df1), desc='Inserting Titles', position=0)

for index, row in titles_df1.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `titles` (`name`,order_by, `created_at`, `updated_at`)
        VALUES (
        {safe_value(row['title'])},
        100,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
        )
        """
        target_cursor.execute(query)
        
    except Exception as e:
        logging.error(f"Error inserting title {row['title']}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()