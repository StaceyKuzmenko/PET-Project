from ftplib import FTP
from datetime import datetime
import os

#credentials for accessing FTP
ftp_host = 'ftp.horseforce.ru'
username = 'ftp_user2'
password = 'K8OCG7fSGlO3'

#establishing ftp connetion and changing home folder to Engeocom
ftp = FTP(ftp_host)
ftp.login(user=username, passwd=password) 
ftp.cwd('Engeocom\\test')

#getting the list of filenames 
filelist = ftp.nlst()

#we will record the data to 2 sepsrste variables
latest_time = None
latest_name = None

#looping through all files and getting the latest file
for file in filelist:
    #get the time when the file was modified    
    modified_time = ftp.sendcmd('MDTM ' + file)
    
    #checking the conditions if the file has most recent
    if (latest_time is None) or (modified_time > latest_time):
        latest_name = file
        latest_time =modified_time
    
    #for debugging printing all files dates
    print ('found date >>>', datetime.strptime(modified_time[4:], "%Y%m%d%H%M%S").strftime("%d %B %Y %H:%M:%S"))

#name and date of the latest file in a folder
print(f'{latest_name} -  {datetime.strptime(latest_time[4:], "%Y%m%d%H%M%S").strftime("%d %B %Y %H:%M:%S")}')

#copying the file to the local folder
with open(os.path.join('ftp', latest_name), 'wb') as f:
    ftp.retrbinary('RETR '+ latest_name, f.write)

#closing the ftp connetion
ftp.close
