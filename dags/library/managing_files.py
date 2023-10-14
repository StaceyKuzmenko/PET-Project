import os

def find_the_latest_local_file_by_name(folder:str) -> str:
    file_list = os.listdir(os.path.join('/', 'opt', 'airflow', 'plugins', 'files_dir', folder))
    latest_file = sorted(file_list, reverse=True)
    return '/opt/airflow/plugins/files_dir/' + str(folder) + '/' + str(latest_file[0])


if __name__=="__main__":
    find_the_latest_local_file_by_name()