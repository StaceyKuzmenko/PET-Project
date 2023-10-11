from ftplib import FTP
from datetime import datetime
import os
#import pydantic
#import logger

def get_files_from_ftp(folder_list:list, host, user:str, passwd:str):
    #establishing ftp connetion and 
    ftp = FTP(host)
    ftp.login(user=user, passwd=passwd)
    
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
        file_name_with_full_path_to_local_folder = os.path.join('/', 'opt', 'airflow', 'plugins', 'files_dir', folder, latest_name)
        with open(file_name_with_full_path_to_local_folder, 'wb') as f:
            ftp.retrbinary('RETR '+ latest_name, f.write)
    
    #closing the ftp connetion
    ftp.close



if __name__ == '__main__':
    ftp_host = ''
    username = ''
    password = ''
    
    folders = ('forecast', 'category', 'sales')
    get_files_from_ftp(folder_list=folders, host=ftp_host, user=username, passwd=password)


    