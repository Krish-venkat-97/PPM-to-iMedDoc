import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value,getSourceFilePath, getTargetFilePath, getLogFilePath

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_documents = 'SELECT * FROM ExternalDocuments'
src_documents_df = pd.read_sql(src_documents, get_src_accessdb_connection())

src_documents_df = src_documents_df[~src_documents_df['DocFolder'].isna()]

#-----------------------------Combining source and target data----------------------
tgt_scan_df = pd.read_sql("SELECT id AS scan_id,patient_id,PPM_External_Scan_Id FROM scan_documents WHERE PPM_External_Scan_Id IS NOT NULL", myconnection)
tgt_scan_df['PPM_External_Scan_Id'] = tgt_scan_df['PPM_External_Scan_Id'].astype(int)
src_documents_df['ID'] = src_documents_df['ID'].astype(int)
landing_documents_df = dd.merge(src_documents_df, tgt_scan_df, left_on='ID', right_on='PPM_External_Scan_Id', how='inner')

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
landing_documents_df1 = landing_documents_df[['ID','DocDate','patient_id','DocFolder','DocFileName']]

#------------------------------finiding extension--------------------
def getFileExtension(row):
    matches = glob.glob(os.path.join(getSourceFilePath(),row['DocFolder'], f"{row['DocFileName']}.*"))
    if matches:
        extension = os.path.splitext(matches[0])[1]
    else:
        None
    return extension
landing_documents_df1['FileExtension'] = landing_documents_df1.apply(getFileExtension, axis=1)

#----------------------sourceFileLocation-------------------
def getSourceFileLocation(row):
    if pd.isna(row['DocFileName']):
        return None
    else:
        return os.path.join(getSourceFilePath(), row['DocFolder'], (row['DocFileName'] + row['FileExtension']))
landing_documents_df1['SourceFileLocation'] = landing_documents_df1.apply(getSourceFileLocation, axis=1)

#----------------------------getting target file directory-----------
def getTargetFileDirectory(row):
    if pd.isna(row['DocFileName']):
        return None
    else:
        return os.path.join(getTargetFilePath(), 'patients', str(row['patient_id']), 'scans','verified')
landing_documents_df1['TargetFileDirectory'] = landing_documents_df1.apply(getTargetFileDirectory, axis=1)

#--------------------------------getting target file location-------------------
def getTargetFileLocation(row):
    if pd.isna(row['DocFileName']):
        return None
    else:
        return os.path.join(row['TargetFileDirectory'], (str(row['scan_id']) + row['FileExtension']))
landing_documents_df1['TargetFileLocation'] = landing_documents_df1.apply(getTargetFileLocation, axis=1)

#----------------------------checking the file exist-------------------
def fileCheck(row):
    if os.path.exists(row['SourceFileLocation']):
        return 1
    else:
        return 0
landing_documents_df1['file_check'] = landing_documents_df1.apply(fileCheck, axis=1)

#-------------------------------dropping unneeded columns----------------------
landing_documents_df1 = landing_documents_df1[['ID', 'scan_id', 'DocDate', 'patient_id', 'DocFolder', 'DocFileName', 'FileExtension', 'SourceFileLocation', 'TargetFileDirectory', 'TargetFileLocation', 'file_check']]
landing_documents_df1 = landing_documents_df1[landing_documents_df1['file_check'] == 1]

#--------------------------------Migrating the documents---------------------------
bar = tqdm(total=len(landing_documents_df1), desc='Migrating documents')
missing_source_document = []
missing_source_id = []

for index, row in landing_documents_df1.iterrows():
    bar.update(1)
    try:
        is_file_dffolder_exist = os.path.exists(row['TargetFileDirectory'])
        if not is_file_dffolder_exist:
            os.makedirs(row['TargetFileDirectory'])
            is_file_exist = os.path.exists(row['TargetFileLocation'])
            if not is_file_exist:
                shutil.copy(row['SourceFileLocation'], row['TargetFileLocation'])
            else:
                pass
        else:
            is_file_exist = os.path.exists(row['TargetFileLocation'])
            if not is_file_exist:
                shutil.copy(row['SourceFileLocation'], row['TargetFileLocation'])
            else:
                pass
    except Exception as e:
        logging.error(f"Error copying file from {row['SourceFileLocation']} to {row['TargetFileLocation']}: {e}")
        missing_source_document.append(row['SourceFileLocation'])
        missing_source_id.append(row['ID'])
        
#-----------------------Making a excel file which has missing documents-----------------
if missing_source_document:
    missing_df = pd.DataFrame(zip(missing_source_id, missing_source_document), columns=['Missing Source Document ID', 'Missing Source Document'])
    missing_df.to_excel(os.path.join(getLogFilePath(), 'missing_source_documents.xlsx'), index=False)
    print(f"Missing source documents logged in {getLogFilePath()}/missing_source_documents.xlsx")


print(landing_documents_df1.columns)