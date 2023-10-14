copy "STG".test_{{ parameters.folder }} 
from '{{ parameters.latest_file }}'
with (format csv, delimiter ";", header);
