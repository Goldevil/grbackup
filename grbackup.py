# GRBackup - GET REMOTE BACKUP
# karim.elfounas@gmail.com (KEF)
# Licence: GPL-3.0-or-later
# Purpose:
#   SFTP download of files from a remote server.
#   Designed as scheduled task: downloads only file changed since last execution.
#   Appropriate for syncing backup files, generated on a remote server, with a local folder
# Technical details:
#   SFTP implemented using paramiko library: https://docs.paramiko.org/en/stable/
#   YAML loader with PyYaml: https://pyyaml.org/

# V 0.1.0 02/01/2020 KEF: Base implementation (all hardcoded)
# V 0.2.0 07/01/2020 KEF: Parametrisation in param file + manage date last backup
# V 0.3.0 12/01/2020 KEF: Support of debug mode and simulation mode
# V 0.4.0 15/01/2020 KEF: Avoid overwritting of previous version (Rename previous version of a file and delete it only after successful download)
#                    KEF: Error handling
#                    KEF: Display file size in human readable format (KB, MB...)
# V 0.5.0 18/01/2020 KEF: New sync mode: download not only updated files but also files missing in local folder
# V 0.6.0 25/01/2020 KEF: Move parameter for file storing last execution date into parameter files. 
# V 0.7.0 08/02/2020 KEF: create log function
#                    KEF: send log in email (supports Gmail SMTP with SSL)
#                    KEF: log into text file
#                    KEF: parameter to control log mode (console output and/or logfile and/or email report)
# V 0.7.1 09/02/2020 KEF: BUG syntax error in display message when cannot connect to server
#                    KEF: BUG missing closing of log file
# V 0.8.0 16/02/2020 KEF: parameters in text format (YAML instead of python module)
#                    KEF: Supports multiple configurations in a single conf file (multiple settings for multiple remote servers)
#                    KEF: Support default parameters (applicable to multiple configuration
#                    KEF: Command line argument to select the configuration to execute
# V 0.8.1 16/02/2020 KEF: Error handling when loading parameters
# V 0.8.2 16/02/2020 KEF: Corrected subject for email report
# V 0.8.3 29/02/2020 KEF: Use standard python logging library instead of custom file write.
#                         --> Benefits: flush log file during execution (not at end of execution), include libraries logs
#                    KEF: Rename class param into gr_param. Allows support of python 2.
# V 0.9.0 07/03/2020 KEF: Calculate and display duration
# V 0.9.1 13/03/2020 KEF: subject of mail report updated
# V 0.10.0 28/11/2020 KEF: Better usage of python 3 logging package
#                          Bug about temporary backup file solved

# Planned features:
# [MEDIUM] Restructure code to put everything in classes
# [MEDIUM] Improve exception handling (in particular in email sending part)
# [MEDIUM] Manage folder structure, allowing to download subfolders. Should be able to avoid links (controlled by parameters)
# [MEDIUM] Keep status last exec for each file, not relies on a single date for a complete sync (SQLite DB?)
# [MEDIUM] Keeping older version of files with retention policy (nb of versions, max size, dates...)
# [MEDIUM] support keystore of credentials to avoid password displayed in clear in config files
# [LOW] calculate and display transfert rate in logs
# [LOW] Display status bar in console
# [LOW] Create root target directory if not exists (controlled by parameters)
# [LOW] supports parameter file in /etc folder on Unix based system
# [LOW] extended to support local-to-local and local-to-remote, not only remote-to-local. More universal sync tool
# [LOW] support encryption of copies
# [LOW] support compresion of copies


VERSION = "0.10.0"
log_output = ""

import os
import datetime
import time
import smtplib
import ssl
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import yaml 
import copy
import sys
import logging
import shutil
import glob
import stat
import uuid
import paramiko
from pprint import pprint

#def dump(obj):
#  for attr in dir(obj):
#    print("obj.%s = %r" % (attr, getattr(obj, attr)))

class gr_param:
    def __init__(self,name):
        self.name = name
        self.label = name
        self.last_exec_filename=os.getcwd() + "/grbackup_"+self.name+"_last.txt"
        self.log_filename = os.getcwd() + "/grbackup_"+self.name+".log"
        self.log_console = True                                      #Output on console
        self.log_email = False                                       #Output on email report (email report parameters are mandatory)
        self.log_file = False                                        #Output on log file (parameter log_filename is mandatory)
        self.debug = False                                           #Display additional debug information
        self.host = "undefined_server" 
        self.port = 22
        self.password = "undefined_password"
        self.username = "undefined_login"
        self.sourcefolder = ""                                       #Remote folder from where files will be downloaded
        self.targetfolder = os.getcwd() + "/grbackup_"+self.name+"/" #Folder where files will be stored. Don't forget the trailing slash
        self.sync = True                                             #download not only updated files but also new files (not existing in target folder)
        self.simulation = False                                      #Do not download any file
        self.ssl_port = 465  # For SSL
        self.smtp_server = "undefined_server"
        self.smtp_login = "undefined_login"
        self.smtp_password = "undefined_password"
        self.source_email_address = "undefined_source_email"
        self.dest_email_address = "undefined_dest_email"
        self.process_folders = False 

    def load(self,data):
        if 'name' in data:
            self.name = data['name']
        if 'label' in data:
            self.label = data['label']
        if 'last_exec_filename' in data:
             self.last_exec_filename = data['last_exec_filename']
        if 'files' in data:
            files = data['files']
            if 'process_subfolders' in files:
                self.process_folders = files['process_subfolders']
        if 'log' in data:
            log = data['log']
            if 'filename' in log:
                self.log_filename = log['filename']
            if 'console' in log:
                self.log_console = log['console']
            if 'email' in log:
                self.log_email = log['email']
            if 'file' in log:
                self.log_file = log['file']
            if 'debug' in log:
                self.debug= log['debug']
        if 'source' in data:
            source = data['source']
            if 'host' in source:
                self.host = source['host']
            if 'port' in source:
                self.port = source['port']
            if 'username' in source:
                self.username = source['username']
            if 'password' in source:
                self.password = source['password']              
            if 'folder' in source:
                self.sourcefolder = source['folder']
        if 'target' in data:
            target = data['target']
            if 'folder' in target:
                self.targetfolder = target['folder']
        if 'execmode' in data:
            execmode = data['execmode']
            if 'sync' in execmode:
                self.sync = execmode['sync']
            if 'simulation' in execmode:
                self.simulation = execmode['simulation']
        if 'emailreport' in data:
            emailreport = data['emailreport']
            if 'ssl_port' in emailreport:
                self.ssl_port = emailreport['ssl_port']
            if 'server' in emailreport:
                self.smtp_server = emailreport['server']
            if 'username' in emailreport:
                self.smtp_login = emailreport['username']
            if 'password' in emailreport:
                self.smtp_password = emailreport['password']
            if 'source_address' in emailreport:
                self.source_email_address = emailreport['source_address']
            if 'dest_address' in emailreport:
                self.dest_email_address = emailreport['dest_address']
        return self

def copyfilter(source, destination,AllowedPattern,MinSize):
    totalsize= 0
    logger.info("CopyFilter from "+source+ " to "+destination)
    
    for pattern in AllowedPattern:
        #print("Pattern: "+pattern)
        for sfile in glob.glob(source+pattern,recursive=False):
            sfilesize= os.path.getsize(sfile)
            if sfilesize >= MinSize:
                dfile=destination+"/"+os.path.basename(os.path.normpath(sfile))
                if os.path.exists(dfile):
                    logger.info(dfile+ " exists. Skip")
                else:
                    logger.info("Copy "+sfile+ " to "+dfile+ " ("+str(sfilesize)+" bytes)")
                    try:
                        shutil.copyfile(sfile, dfile)
                        totalsize=totalsize+sfilesize
                    except:
                        logger.info("Error. Skip")
    for sfolder in glob.glob(source+"*/",recursive=False):
        #print("Subfolder: "+os.path.basename(os.path.normpath(sfolder)))
        dfolder=destination+"/"+os.path.basename(os.path.normpath(sfolder))
        logger.info("Create destination subfolder "+dfolder)
        os.makedirs(dfolder,exist_ok = True)
        totalsize=totalsize+copyfilter(sfolder,dfolder,AllowedPattern,MinSize)
        logger.info("Total size: "+humanbytes(totalsize))
    return totalsize

def humanbytes(B):
   'Return the given bytes as a human friendly KB, MB, GB, or TB string'
   # Function written by https://stackoverflow.com/users/322384/whereisalext
   B = float(B)
   KB = float(1024)
   MB = float(KB ** 2) # 1,048,576
   GB = float(KB ** 3) # 1,073,741,824
   TB = float(KB ** 4) # 1,099,511,627,776
   if B < KB:
      return '{0} {1}'.format(B,'Bytes' if 0 == B > 1 else 'Byte')
   elif KB <= B < MB:
      return '{0:.2f} KB'.format(B/KB)
   elif MB <= B < GB:
      return '{0:.2f} MB'.format(B/MB)
   elif GB <= B < TB:
      return '{0:.2f} GB'.format(B/GB)
   elif TB <= B:
      return '{0:.2f} TB'.format(B/TB)

def humanduration(start_time, end_time):
    'Return a duration in human readable format'
    duration = end_time - start_time #returns seconds
    duration_text=""
    days = int(duration // 86400)
    hours = int(duration // 3600 % 24)
    minutes = int(duration // 60 % 60)
    seconds = int(duration % 60)
    if days > 0:
        duration_text = duration_text + " " + str(days) + " d"
    if hours > 0:
        duration_text = duration_text + " " + str(hours) + " h"
    if minutes > 0:
        duration_text = duration_text + " " + str(minutes) + " min"
    if seconds > 0:
        duration_text = duration_text + " " + str(seconds) + " sec"
    return duration_text
    
def humanstatus(nb_error):
    'Return number of error in human readable format'
    if nb_error == 0:
        return "OK"
    else:
        return "ERROR (" + nb_error + " errors)"
# Get from command line argument the backup configuration to execute
if len(sys.argv) >= 2:
    selected_param=sys.argv[1]
else:
    selected_param="default"

# Load configuration file
defaultparam = gr_param("default")
load_count = 0
try:
    conf_file = open("grbackup.conf", "r")
    for data in yaml.load_all(conf_file, Loader=yaml.SafeLoader):
        # print(data)
        load_count = load_count + 1
        if load_count == 1:
            defaultparam=defaultparam.load(data)
            param = copy.deepcopy(defaultparam)
        else:
            loadparam= copy.deepcopy(defaultparam)
            loadparam.load(data)
            if selected_param == loadparam.name:
                param = copy.deepcopy(loadparam)
    if load_count == 1:
        param = copy.deepcopy(defaultparam)
except:
    print("[ERROR] Unable to load configuration file")
    exit()
    


logger = logging.getLogger("GRBackup")
logger.setLevel(logging.DEBUG)

if param.log_file:
    fh = logging.FileHandler(param.log_filename)
    fh.setLevel(logging.DEBUG)
    formatterDB = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh.setFormatter(formatterDB)
    logger.addHandler(fh)
if param.log_console: 
    sh= logging.StreamHandler()
    sh.setLevel(logging.INFO)
    formatterCS = logging.Formatter("[%(levelname)s] %(message)s")
    sh.setFormatter(formatterCS)
    logger.addHandler(sh)
if param.log_email:
    email_logfile= os.getcwd() + "/grbackup_email_"+str(uuid.uuid4().fields[-1])[:7]+".log"
    eh = logging.FileHandler(email_logfile)
    eh.setLevel(logging.INFO)
    formatterDB = logging.Formatter("[%(levelname)s] %(message)s")
    eh.setFormatter(formatterDB)
    logger.addHandler(eh)

#Start the serious work
logger.info("GRBackup "+version+ " - "+ param.label + " ("+param.name+")")
logger.debug("[START]")

if param.simulation:
    logger.info("Simulation mode")
if param.debug:
    logger.debug("Debug mode")
if param.log_console: 
    logger.debug("Log in console active")
if param.log_email: 
    logger.debug("Email report activated")
if param.log_file: 
    logger.debug("Log in file "+param.log_filename)
        
nb_files_download = 0
nb_files_error = 0
tot_size=0
start_time = time.time()


#Get date of last sync
if os.path.isfile(param.last_exec_filename):
    logger.debug("Get date last download")
    try:
        last_file = open(param.last_exec_filename, "r") 
        lastsync_date = int(last_file.read()) 
        last_file.close()
    except IOError:
        logger.error("Unable to read date of last execution in " + param.last_exec_filename)
        logger.info("[END]")
        exit()
    else:
        logger.info("Date last download: " + time.ctime(lastsync_date) )
        #lastsync_date = 1577999974 1578259134 #Hardcoded
        # 1508259132 (full)
else: 
    logger.info("First execution (no previous download detected)")
    lastsync_date = 0


#Open connection
logger.debug("Open connection to "+param.host)
try:
    transport = paramiko.Transport((param.host, param.port))
    transport.connect(username = param.username, password = param.password)
    sftp = paramiko.SFTPClient.from_transport(transport)
except:
    logger.error("Cannot connect to server " + param.host + " on port " + str(param.port))
    logger.info("[END]")
    exit()
else:
    logger.info("Connected "+param.host)

#Get list of files
try:
    logger.debug("Get list of file")
    filelist=sftp.listdir(param.sourcefolder)
except:
    logger.error("Cannot get list of files")
    logger.info("[END]")
    exit()
else:
    logger.info(str(len(filelist)) + " items in remote folder")
    if param.debug:
        pprint(filelist)

#For each file, donwload if new or updated
for filetoget in filelist:
    to_download = False
    path = param.sourcefolder + filetoget
    localpath = param.targetfolder + filetoget
    filestat = sftp.stat(param.sourcefolder+filetoget)
    #Only consider regular files, ignore folder (to be implemented later)
    if stat.S_ISREG(filestat.st_mode):
        if param.debug:
            logger.info("File "+filetoget+" : "+humanbytes(filestat.st_size))
            pprint(filestat)
        #Download a file if sync mode and new file
        if param.sync and os.path.isfile(localpath) == False:
           to_download = True
        #Download a file if modified after last update
        if filestat.st_mtime > lastsync_date:
            to_download = True
    else:
        logger.debug("Skip folder "+filetoget)

    
    if to_download:
        if param.simulation == False:
            logger.info("Get file "+ filetoget + " last modified on " +time.ctime(filestat.st_mtime))
            localpath = param.targetfolder + filetoget
            localpath_backup = localpath + ".back"
            #If file exists (previously downloaded), do a copy
            if os.path.isfile(localpath):
                logger.debug("Backup previous version: " + localpath_backup)
                try:
                    os.rename(localpath, localpath_backup)
                except:
                    logger.error("Cannot backup previous version")
                    logger.info("[END]")
                    exit()
            try:
                sftp.get(path, localpath, callback=None)
            except:
                logger.error("Error when downloading file.")
                nb_files_error = nb_files_error + 1
                #If backup file exists, restore it
                if os.path.isfile(localpath_backup):
                    logger.debuginfo("Restore backup from previous version "+localpath_backup)
                    os.rename(localpath_backup, localpath)
            else:
                nb_files_download = nb_files_download + 1
                tot_size = tot_size + filestat.st_size
                logger.info("[OK] "+humanbytes(filestat.st_size))
                #If backup file exists, delete it
                if os.path.isfile(localpath_backup):
                    logger.debug("Delete backup of previous version "+localpath_backup)
                    os.remove(localpath_backup)    
        else:
            logger.info("Simulation: get file "+ filetoget +" last modified on " + time.ctime(filestat.st_mtime))

#Close connection
logger.debug("Close connection")
try:
    sftp.close()
    transport.close()
except:
    logger.error("Unable to close connection properly")


#Save date of last successful sync
logger.debug("Save date last download")
try:
    last_file = open(param.last_exec_filename, "w") 
    last_file.write("%s" % int(time.time())) 
    last_file.close()
except IOError:
    logger.error("Unable to save date of last download")
else:
    logger.debug("Date last download saved: " +str(int(time.time())))

duration_text = humanduration(start_time,time.time())
#duration_text = "?"
logger.info("Finished: "+ str(nb_files_download)+" updated/new files downloaded - " + humanbytes(tot_size) + " - " + duration_text )

if param.log_email:
    logger.debug("Send email report to "+ param.dest_email_address)
    efile = open(email_logfile,mode='r')
    log_output= efile.read()
    efile.close()
    #log_output= str(nb_files_download) + " files transfered ("+ humanbytes(tot_size) + " in " + duration_text + ")"
    try:
        # Create a secure SSL context
        context = ssl.create_default_context()
        # Build email content
        # Create a multipart message and set headers
        subject = "[GRBACKUP] " + param.name +": "+ humanstatus(nb_files_error)+" ("+ str(nb_files_download) + " files - "+ humanbytes(tot_size) + " - " + duration_text + ")" 
        message = MIMEMultipart()
        message["From"] = param.source_email_address
        message["To"] = param.dest_email_address
        message["Subject"] = subject
        # Add body to email
        message.attach(MIMEText(log_output, "plain"))
        emailtext = message.as_string()
        with smtplib.SMTP_SSL(param.smtp_server, param.ssl_port, context=context) as server:
            server.login(param.smtp_login, param.smtp_password)
            server.sendmail(param.source_email_address, param.dest_email_address, emailtext)
    except:
        logger.error("Unable to send email report")
    else:
        logger.info("Email report sent to "+ param.dest_email_address)
    os.remove(email_logfile) 

