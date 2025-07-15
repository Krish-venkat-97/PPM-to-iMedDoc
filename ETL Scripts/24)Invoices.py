import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from libs import *
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value

myconnection = get_tgt_myconnection()
target_cursor = myconnection.cursor()

warnings.filterwarnings("ignore")

src_invoices = 'SELECT * FROM InvoiceHeadSummary'

try:
    src_invoices_df = pd.read_sql(src_invoices, get_src_accessdb_connection())
except:
    src_invoices_df = pd.read_sql(src_invoices, get_src_accessdb2_connection())

src_invoices_df = src_invoices_df[src_invoices_df['Invoice Number'] != 0]
src_invoices_df = src_invoices_df[src_invoices_df['TotalValue'] != 0]

src_invoice_print_summary = 'SELECT * FROM InvoicePrintSummary'

try:
    src_invoice_print_summary_df = pd.read_sql(src_invoice_print_summary, get_src_accessdb2_connection())
except:
    src_invoice_print_summary_df = pd.read_sql(src_invoice_print_summary, get_src_accessdb_connection())

src_invoice_print_summary_df = src_invoice_print_summary_df[['InvoiceNo','Balance']]
src_invoice_print_summary_df = src_invoice_print_summary_df.rename(columns={'InvoiceNo': 'Invoice Number'})

src_invoice_used = 'SELECT * FROM InvNoUsed'

try:
    src_invoice_used_df = pd.read_sql(src_invoice_used, get_src_accessdb2_connection())
except:
    src_invoice_used_df = pd.read_sql(src_invoice_used, get_src_accessdb_connection())
    
src_invoice_used_list = src_invoice_used_df['Invoice Number'].unique().tolist()

#----------------------filtering invoices that are not printed---------------------
src_invoices_df = src_invoices_df[src_invoices_df['Invoice Number'].isin(src_invoice_used_list)]

#----------------------patient mapping---------------------
src_invoices_df['PatientCode'] = src_invoices_df['PatientCode'].astype(int)
tgt_patient_df = pd.read_sql("SELECT id as patient_id, PPM_Patient_Id FROM patients WHERE PPM_Patient_Id IS NOT NULL", myconnection)
tgt_patient_df['PPM_Patient_Id'] = tgt_patient_df['PPM_Patient_Id'].astype(int)
landing_invoice_df = pd.merge(src_invoices_df, tgt_patient_df, left_on='PatientCode', right_on='PPM_Patient_Id', how='left')

#----------------------dropping None patients rows----------------- 
landing_invoice_df = landing_invoice_df[~landing_invoice_df['patient_id'].isna()]  

#----------------------invoice date-----------------
def invoiceDate(row):
    if pd.isna(row['Date Created']) or row['Date Created'] == '':
        return None
    else:
        return row['Date Created'].strftime('%Y-%m-%d') 
    
landing_invoice_df['InvoiceDate'] = landing_invoice_df.apply(invoiceDate, axis=1)

#--------------------------only needed columns-------------------
landing_invoice_df1 = landing_invoice_df[['Invoice Number','InvoiceDate','patient_id','TotalValue','VATRate', 'VATAmount','InvoiceTo','AccountName','EDIClaim','Hospital','EDIHospitalNumber','InsuranceCompany','InsuranceCompany1']]

#--------------------------Mapping tax-------------------
tgt_tax_df = pd.read_sql("SELECT id as tax_id, name as tax_name, perc as tax_perc FROM taxes", myconnection)
tgt_tax_df['tax_perc'] = tgt_tax_df['tax_perc'].astype(float)
landing_invoice_df1['VATRate'] = landing_invoice_df1['VATRate'].fillna(0) # Fill NaN values with 0
landing_invoice_df1['VATRate'] = (landing_invoice_df1['VATRate']*100).astype(float)
landing_invoice_df1 = landing_invoice_df1.merge(tgt_tax_df, left_on='VATRate', right_on='tax_perc', how='left')
landing_invoice_df1.drop(columns=['tax_perc','tax_name'], inplace=True)

#--------------------------Mapping billTo-------------------
def billTo(row):
    if row['InvoiceTo'] == 1:
        return 'Patient'
    elif row['InvoiceTo'] == 2:
        return 'alt billing'
    elif row['InvoiceTo'] == 3:
        return 'Hospital'
    elif row['InvoiceTo'] == 4:
        return 'GP'
    elif row['InvoiceTo'] == 6:
        return 'Third Party'
    elif row['InvoiceTo'] == 7:
        return 'Specialist'
    elif row['InvoiceTo'] == 9:
        return 'Insurance Company'
    elif row['InvoiceTo'] == 11:
        return 'Insurance Company'
    elif row['InvoiceTo'] == 20:
        return 'Patient'
    elif row['InvoiceTo'] == 21:
        return 'Insurance Company'
    else:
        return 'Patient'

landing_invoice_df1['billTo'] = landing_invoice_df1.apply(billTo, axis=1)

#--------------------------Patient BillTO------------------
landing_invoice_patient_df = landing_invoice_df1[landing_invoice_df1['billTo'].isin(['Patient', 'alt billing'])]

#--------------------------Hospital BillTO-------------------
landing_invoice_hospital_df0 =landing_invoice_df1[landing_invoice_df1['billTo'] == 'Hospital']

edi_hospitals_df = landing_invoice_hospital_df0.dropna(subset=['EDIHospitalNumber']).drop_duplicates(subset=['EDIHospitalNumber']).reset_index(drop=True).drop(columns=['Hospital'])
edi_hospitals_df = edi_hospitals_df[['AccountName','EDIHospitalNumber']]

src_invoice_hospitals_df2 = landing_invoice_hospital_df0.merge(edi_hospitals_df, on='EDIHospitalNumber', how='left')

def changeHospital(row):
    account_name = str(row['AccountName_x']).lower()
    hospital_name = str(row['Hospital']).lower()
    secondary_account_name = str(row['AccountName_y']).lower()

    if 'hospital' not in account_name and 'hospital' in secondary_account_name:
        return secondary_account_name
    elif 'hospital' not in account_name and 'hospital' not in secondary_account_name and 'hospital' in hospital_name:
        return hospital_name
    else:
        return account_name
    
src_invoice_hospitals_df2['AccountName_original'] = src_invoice_hospitals_df2.apply(changeHospital, axis=1)
src_invoice_hospitals_df2 = src_invoice_hospitals_df2.drop(columns=['AccountName_x', 'AccountName_y', 'Hospital','EDIHospitalNumber'])
src_invoice_hospitals_df2 = src_invoice_hospitals_df2.rename(columns={'AccountName_original': 'AccountName'})

tgt_hospital_df = pd.read_sql('SELECT DISTINCT id as hospital_id,UPPER(LTRIM(RTRIM(name))) as AccountName_Upper FROM hospitals GROUP BY UPPER(LTRIM(RTRIM(name)))', myconnection)
src_invoice_hospitals_df2['AccountName_Upper'] = src_invoice_hospitals_df2['AccountName'].astype(str).str.upper().str.strip()
landing_invoice_hospital_df = src_invoice_hospitals_df2.merge(tgt_hospital_df, on='AccountName_Upper', how='left')
landing_invoice_hospital_df = landing_invoice_hospital_df.drop(columns=['AccountName_Upper'])
landing_invoice_hospital_df['Notes'] = landing_invoice_hospital_df.apply(lambda x: f"paid by {x['AccountName']}" if pd.notna(x['AccountName']) else '', axis=1)

#-------------------------Mapping insurance---------------------------
landing_invoice_insurance_df0 = landing_invoice_df1[landing_invoice_df1['billTo'] == 'Insurance Company']
landing_invoice_insurance_df0['AccountName_x'] = landing_invoice_insurance_df0['InsuranceCompany'].fillna(landing_invoice_insurance_df0['InsuranceCompany1'])

tgt_insurance_company_df =  pd.read_sql("SELECT id as insurance_company_id,UPPER(LTRIM(RTRIM(name))) as AccountName_Upper FROM insurance_companies GROUP BY UPPER(LTRIM(RTRIM(name)))", myconnection)
landing_invoice_insurance_df0['AccountName_Upper'] = landing_invoice_insurance_df0['AccountName_x'].str.upper().str.strip()
landing_invoice_insurance_df = landing_invoice_insurance_df0.merge(tgt_insurance_company_df, on='AccountName_Upper', how='left')
landing_invoice_insurance_df = landing_invoice_insurance_df.drop(columns=['AccountName_Upper','AccountName_x'])

#-------------------------Mapping GP---------------------------
landing_invoice_gp_df0 = landing_invoice_df1[landing_invoice_df1['billTo'] == 'GP']
tgt_gp_df = pd.read_sql("SELECT c.id AS contact_id, UPPER(LTRIM(RTRIM(CONCAT(t.name,' ',c.display_name)))) AS AccountName_Upper FROM contacts c INNER JOIN titles t ON c.title_id = t.id WHERE c.PPM_GP_Id IS NOT NULL GROUP BY 2;", myconnection)
landing_invoice_gp_df0['AccountName_Upper'] = landing_invoice_gp_df0['AccountName'].str.upper().str.strip()
landing_invoice_gp_df = landing_invoice_gp_df0.merge(tgt_gp_df, on='AccountName_Upper', how='left')
landing_invoice_gp_df = landing_invoice_gp_df.drop(columns=['AccountName_Upper'])

#-------------------------Mapping Specialist---------------------------
landing_invoice_specialist_df0 = landing_invoice_df1[landing_invoice_df1['billTo'] == 'Specialist']
tgt_specialist_df = pd.read_sql("SELECT c.id AS contact_id, UPPER(LTRIM(RTRIM(CONCAT(t.name,' ',c.display_name)))) AS AccountName_Upper FROM contacts c INNER JOIN titles t ON c.title_id = t.id WHERE c.PPM_referral_Id IS NOT NULL GROUP BY 2;", myconnection)
landing_invoice_specialist_df0['AccountName_Upper'] = landing_invoice_specialist_df0['AccountName'].str.upper().str.strip()
landing_invoice_specialist_df = landing_invoice_specialist_df0.merge(tgt_specialist_df, on='AccountName_Upper', how='left')
landing_invoice_specialist_df = landing_invoice_specialist_df.drop(columns=['AccountName_Upper'])

#-------------------------Mapping Third Party---------------------------
landing_invoice_third_party_df0 = landing_invoice_df1[landing_invoice_df1['billTo'] == 'Third Party']
tgt_third_party_df = pd.read_sql("SELECT id as contact_id,UPPER(LTRIM(RTRIM(PPM_solicitor))) as AccountName_Upper FROM contacts WHERE PPM_solicitor_Id = 'From_InvoiceHeadSummary' GROUP BY UPPER(LTRIM(RTRIM(PPM_solicitor)))", myconnection)
landing_invoice_third_party_df0['AccountName_Upper'] = landing_invoice_third_party_df0['AccountName'].str.upper().str.strip()
landing_invoice_third_party_df = landing_invoice_third_party_df0.merge(tgt_third_party_df, on='AccountName_Upper', how='left')
landing_invoice_third_party_df = landing_invoice_third_party_df.drop(columns=['AccountName_Upper'])

#-------------------------Unoin all the dataframes---------------------------
landing_invoice_df1 = pd.concat([
    landing_invoice_patient_df,
    landing_invoice_hospital_df,
    landing_invoice_insurance_df,
    landing_invoice_gp_df,
    landing_invoice_specialist_df,
    landing_invoice_third_party_df
], ignore_index=True)

landing_invoice_df2 = landing_invoice_df1[['Invoice Number', 'InvoiceDate', 'patient_id', 'TotalValue', 'VATRate','VATAmount','tax_id', 'InvoiceTo', 'AccountName', 'EDIClaim','insurance_company_id', 'contact_id','Notes','billTo']]
landing_invoice_df2 = landing_invoice_df2.sort_values(by='InvoiceDate')

#--------------------------Mapping InvoiceTo---------------------
tgt_bill_to_df = pd.read_sql("SELECT id as bill_to_id, UPPER(LTRIM(RTRIM(name))) as bill_to_name FROM bill_to", myconnection)
landing_invoice_df2['billTo'] = landing_invoice_df2['billTo'].str.upper().str.strip()
landing_invoice_df2 = landing_invoice_df2.merge(tgt_bill_to_df, left_on='billTo', right_on='bill_to_name', how='left')
landing_invoice_df2 = landing_invoice_df2.drop(columns=['bill_to_name','billTo'])

#-----------------------------EDI Claims-----------------------------
landing_invoice_df2['EDIClaim_Status'] = landing_invoice_df2['EDIClaim'].apply(lambda x:1 if x else 0)

#-----------------------------Mapping tax_type-----------------------------
landing_invoice_df2['tax_type'] = landing_invoice_df2['tax_id'].apply(lambda x:1 if x ==1 else 3)

#-----------------------------invoice id generation-----------------------------
invoice_max = 'SELECT MAX(id) FROM invoices'
invoice_max_df = pd.read_sql(invoice_max, myconnection)
if invoice_max_df is None or invoice_max_df.iloc[0, 0] is None:
    max_id = 1
else:
    max_id = invoice_max_df.iloc[0, 0] + 1
landing_invoice_df2.insert(0, 'invoice_id', range(max_id, max_id + len(landing_invoice_df2)))

#------------------------------joining invoice print summary to get the belance---------------
landing_invoice_df3 = landing_invoice_df2.merge(src_invoice_print_summary_df, left_on='Invoice Number', right_on='Invoice Number', how='left')

#------------------------------dropping unnecessary columns-----------------------------
invoice_df = landing_invoice_df3[['invoice_id','Invoice Number', 'InvoiceDate', 'patient_id', 'TotalValue','Balance', 'VATRate','VATAmount', 'tax_id','insurance_company_id', 'contact_id', 'Notes', 'bill_to_id','EDIClaim_Status', 'tax_type']]

#------------------------------Adding source identifier-----------------------------
query_1 = "SET sql_mode = ''"
target_cursor.execute(query_1)
query_2 = "ALTER TABLE invoices ADD COLUMN IF NOT EXISTS PPM_Invoice_Id VARCHAR(100) DEFAULT NULL;"
target_cursor.execute(query_2)
myconnection.commit()

#-------------------------------grand total---------------------------
invoice_df['GrandTotal'] = invoice_df['TotalValue'] - invoice_df['VATAmount']

#------------------------filtering out invoices already present in target---------------------------
tgt_invoices_df = pd.read_sql("SELECT PPM_Invoice_Id FROM invoices WHERE PPM_Invoice_Id IS NOT NULL", myconnection)
tgt_invoices_df['PPM_Invoice_Id'] = tgt_invoices_df['PPM_Invoice_Id'].astype(str)
invoice_df['Invoice Number'] = invoice_df['Invoice Number'].astype(str)
invoice_df = invoice_df[~invoice_df['Invoice Number'].isin(tgt_invoices_df['PPM_Invoice_Id'])].reset_index(drop=True)

#-------------------------------inserting into target-----------------------------
bar = tqdm(total=len(invoice_df), desc='Inserting Invoices from InvoiceHeadSummary')


for index, row in invoice_df.iterrows():
    bar.update(1)
    try:
        query = f"""
        INSERT INTO `invoices` (id,`invoice_no`, `invoice_no_ref`, `invoice_date`, `service_date`, `requested_date`, `reported_date`, `discharge_date`, `los`, `billto_id`, `doctor_id`, `patient_id`, `patient_address`, `contact_id`, `contact_address`, `tax_id`, `income_category_id`, `insurance_company_id`, `insurance_number`, `discount`, `discount_notes`, `tax_perc`, `tax_amount`, `waived_amount`, `grand_total`, `net_total`, `balance`, `invoice_status`, `appointment_id`, `surgery_id`, `void_invoice`, `void_invoice_date`, `void_invoice_reason`, `bad_debts_invoice`, `bad_debts_invoice_date`, `bad_debts_invoice_reason`, `is_deleted`, `notes`, `due_date`, `band_id`, `invoice_credit_note`, `invoice_credit_amount`, `invoice_credit_status`, `invoice_writeoff_status`, `eclaim_status`, `eclaim_error_message`, `save_status`, `is_split_invoice`, `split_invoice_amount`, `split_invoice_date`, `split_invoice_bill_to`, `split_invoice_description`, `ins_share`, `ins_balance`, `other_share`, `other_balance`, `tax_type`, `paradox_invoice_number`, `reminders`, `patient_alt_billing`, `created_user_id`, `updated_user_id`, `deleted_user_id`, `created_at`, `updated_at`, `deleted_at`,PPM_Invoice_Id) 
        VALUES (
        {safe_value(row['invoice_id'])},
        {safe_value(row['invoice_id'])},
        {safe_value(row['invoice_id'])}, 
        {safe_value(row['InvoiceDate'])}, 
        NULL, NULL, NULL, NULL, NULL, 
        {safe_value(row['bill_to_id'])}, 
        1, 
        {safe_value(row['patient_id'])}, 
        NULL, 
        {safe_value(row['contact_id'])  }, 
        NULL, 
        {safe_value(row['tax_id'])}, 
        1, 
        {safe_value(row['insurance_company_id'])}, 
        NULL, NULL, '', 
        {safe_value(row['VATRate'])}, 
        {safe_value(row['VATAmount'])}, 
        NULL, 
        {safe_value(row['GrandTotal'])}, 
        {safe_value(row['TotalValue'])}, 
        {safe_value(row['Balance'])}, 
        {safe_value(row['EDIClaim_Status'])}, 
        NULL, NULL, 0, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, NULL, NULL, NULL, NULL, 0.00, 0.00, 0.00, 0.00, 
        {safe_value(row['tax_type'])}, 
        {safe_value(row['Invoice Number'])},
        0, NULL, 1, 1, 0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL,
        {safe_value(row['Invoice Number'])}
        );
        """
        target_cursor.execute(query)
    except Exception as e:
        logging.error(f"Error inserting row {index}: {e}")
        break

myconnection.commit()
myconnection.close()
bar.close()
print('Invoices from InvoiceHeadSummary inserted successfully.')
