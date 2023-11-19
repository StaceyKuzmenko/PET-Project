import os

local_dir = '/opt/files_dir/'

def find_the_latest_local_file_by_name(folder:str) -> str:
    """Сканируем папку и находим самый новый файл в ней (по имени)"""
    file_list = os.listdir(os.path.join('/', 'opt', 'airflow', 'plugins', 'files_dir', folder))
    latest_file = sorted(file_list, reverse=True)
    if len(latest_file) == 0:
        return ''
    else:
        return local_dir + str(folder) + '/' + str(latest_file[0])


if __name__=="__main__":
    find_the_latest_local_file_by_name()