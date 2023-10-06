from ftplib import FTP
from datetime import datetime


#credentials for accessing FTP
ftp_host = 'ftp.horseforce.ru'
username = 'ftp_user2'
password = 'K8OCG7fSGlO3'

ftp = FTP(ftp_host)
ftp.login(user=username, passwd=password) 
ftp.cwd('Engeocom\\test')
filelist = ftp.nlst()
for file in filelist:    
    modifiedTime = ftp.sendcmd('MDTM ' + file)
    print('-----')
    print(modifiedTime)
    print (datetime.strptime(modifiedTime[4:], "%Y%m%d%H%M%S").strftime("%d %B %Y %H:%M:%S"))
    #print('-----')
print(filelist)
ftp.close