from ftplib import FTP
from datetime import datetime
import os
import logging

#import pydantic

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("airflow.ftp")

def get_files_from_ftp(folder_list:list, host, user:str, passwd:str):
    """Загружает файлы по FTP и сохраниет их в соответствующие локальные папки"""
    #создаем FTP соединение
    ftp = FTP(host)
    ftp.login(user=user, passwd=passwd)
    
    #проходим поочередно каждую папку из списка folder_list
    for folder in folder_list:
        latest_time = None
        latest_name = None
        logger.debug('FTP path: ', os.path.join('Engeocom', 'test', folder))
        
        #меняем домашнюю дерикторию на Engeocom
        ftp.cwd(os.path.join('/', 'Engeocom', 'test', folder))
        
        #получаем список имен файлов находящихся в папке 
        filelist = ftp.nlst()

        #поочередно проходим по всему списку файлов
        for file in filelist:
            #получаем время последней модификации файла (в нашем случае оно совпадает с датой создания файла)    
            modified_time = ftp.sendcmd('MDTM ' + file)
            
            #определяем самое новое время и соответствующее ему имя файла
            if (latest_time is None) or (modified_time > latest_time):
                latest_name = file
                latest_time = modified_time
            
            #для отладки печатаем имя файла
            print ('found date >>>', datetime.strptime(modified_time[4:], "%Y%m%d%H%M%S").strftime("%d %B %Y %H:%M:%S"))

        #создаем новое имя для файла в формате ггггммдд
        new_file_name = f'{folder}-{latest_time[4:]}.csv'
        
        #копируем файл в локальную папку
        file_name_with_full_path_to_local_folder = os.path.join('/', 'opt', 'airflow', 'plugins', 'files_dir', folder, new_file_name)
        with open(file_name_with_full_path_to_local_folder, 'wb') as f:
            ftp.retrbinary('RETR '+ latest_name, f.write)
    
    #закрываем FTP соединение
    ftp.close()


if __name__ == '__main__':
    ftp_host = ''
    username = ''
    password = ''
    folders = ('forecast', 'category', 'sales')
    get_files_from_ftp(folder_list=folders, host=ftp_host, user=username, passwd=password)


    