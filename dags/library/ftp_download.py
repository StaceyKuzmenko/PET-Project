from ftplib import FTP
from datetime import datetime
import os
#import pydantic
#import logger

def get_files_from_ftp(folder_list:list, host, user:str, passwd:str):
    #establishing ftp connetion and 
    ftp = FTP(host)
    ftp.login(user=username, passwd=password)
    
    for folder in folder_list:
        print('FTP path: ', os.path.join('Engeocom', 'test', folder))
        #changing home folder to Engeocom
        ftp.cwd(os.path.join('/', 'Engeocom', 'test', folder))
        
        #getting the list of filenames 
        filelist = ftp.nlst()
        
        #looping through the list of files in a folder
        for file in filelist:
            latest_time = None
            latest_name = None
            #get the time when the file was modified    
            modified_time = ftp.sendcmd('MDTM ' + file)
            
            #finding the lates time and filename
            #we will record the data to 2 separate variables
            if (latest_time is None) or (modified_time > latest_time):
                latest_name = file
                latest_time = modified_time
            
            #for debugging printing all files dates
            print ('found date >>>', datetime.strptime(modified_time[4:], "%Y%m%d%H%M%S").strftime("%d %B %Y %H:%M:%S"))

        #name and date of the latest file in a folder
        print(f'{latest_name} -  {datetime.strptime(latest_time[4:], "%Y%m%d%H%M%S").strftime("%d %B %Y %H:%M:%S")}')

        #copying the file to the local folder
        local_folder_path = os.path.join('files_dir', folder, latest_name)
        with open(local_folder_path, 'wb') as f:
            ftp.retrbinary('RETR '+ latest_name, f.write)
    
    #closing the ftp connetion
    ftp.close



if __name__ == '__main__':
    ftp_host = 'ftp.horseforce.ru'
    username = 'ftp_user2'
    password = 'K8OCG7fSGlO3'
    
    folders = ('forecast', 'category', 'sales')
    get_files_from_ftp(folder_list=folders, host=ftp_host, user=username, passwd=password)


    