copy "STG".test_{{ params.folder }} 
from '{{ params.latest_file }}'
with (format csv, delimiter ";", header);
