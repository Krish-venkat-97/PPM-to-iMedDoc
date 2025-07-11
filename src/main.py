from libs import *
import platform
from src.utils import get_tgt_myconnection, get_src_accessdb_connection, get_src_accessdb2_connection, safe_value,getSourceFilePath, getTargetFilePath, getLogFilePath

warnings.filterwarnings("ignore")

print(f"Running in: {platform.architecture()[0]} Python")

# Function to set up logging for each script
def setup_script_logging(script_name):
    #a = str(script_name).replace(".py","")
    a = str(script_name).replace("", "").replace(".py", "")
    log_dir = getLogFilePath()
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{a}.log")

    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


checkpoint_file = os.path.join(getLogFilePath(), "checkpoint.txt")

#list of scripts to run
scripts = [
    '1)insuranceCompanies.py',
    '2)insuranceCompaniesPatient.py',
    '3)insuranceCompaniesInvoice.py',
    '4)titles.py',
    '5)doctors.py',
    '6)hospitals.py',
    '7)hospitalFromInvoices.py',
    '8)GP.py',
    '9)solicitors.py',
    '10)solicitorsInvoice.py',
    '11)anaesthetists.py',
    '12)referralTo.py',
    '13)patients.py',
    '14)patients_AltBilling.py',
    '15)patientContactDetails.py',
    '16)appointmentDescription.py',
    '17)appointments.py',
    '18)surgeries.py',
    '19)letters.py',
    '20)scanDocuments.py',
    '21)externalDocuments.py',
    '22)taxes.py',
    '23)invoiceTo.py',
    '24)Invoices.py',
    '25)invoiceDetails_Consultation.py',
    '26)invoiceDetails_Procedure.py',
    '27)invoiceDetails_MedicalReport.py',
    '28)invoiceDetail_Arbitary.py',
    '29)invoiceDetail_Other.py',
    '30)writeoff.py',
    '31)credit.py',
    '32)Receipt.py',
    '33)lettersMigration.py',
    '34)externalDocumentMigration.py'
]

# Function to get the last completed script
def get_last_completed_script():
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            last_script = f.read().strip()
            if last_script in scripts:
                return scripts.index(last_script) + 1  # Start from next script
    return 0  # Start from the beginning

# Function to save the last successfully completed script
def save_checkpoint(script_name):
    with open(checkpoint_file, "w") as f:
        f.write(script_name)

# Start execution from the last successful script
start_index = get_last_completed_script()

print(f"Starting from script index: {start_index}")
print(f"Total number of scripts: {len(scripts)}")

if start_index >= len(scripts):
    print("Error: Start index is out of range.")
    logging.error("Start index is out of range.")
else:
    for script in scripts[start_index:]:
        # Set up logging for the current script only if an error occurs
        try:
            #logging.info(f"Starting execution: {script}")
            # Remove the old log file before running the script
            log_file = os.path.join(getLogFilePath(), f"{os.path.splitext(script)[0]}.log")
            if os.path.exists(log_file):
                os.remove(log_file)
            print(f"Running {script}...")

            # Run the script and ensure the progress bar is displayed
            script_path = os.path.join("ETL Scripts", script)
            python_exe = r"D:\Sakthi\Py 32-bit\python.exe"  # Adjust the path to your Python executable if needed
            # Use subprocess to run the script
            result = subprocess.run([python_exe, script_path], capture_output=True, text=True, check=True, encoding='utf-8')

            # Log standard output
            #logging.info(f"Output of {script}: {result.stdout}")
            print(f"Output of {script}: {result.stdout}")

            # Save checkpoint
            save_checkpoint(script)

        except subprocess.CalledProcessError as e:
            setup_script_logging(script)
            logging.error(f"Error in {script}: {e.stderr}")
            print(f"❌ {script} failed! Check {script}.log for details.")
            break  # Stop execution on failure

print("✅ Pipeline execution complete. Check the individual log files for details.")