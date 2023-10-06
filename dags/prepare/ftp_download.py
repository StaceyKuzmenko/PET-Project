from ftplib import FTP
from datetime import datetime




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