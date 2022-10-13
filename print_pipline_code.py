"""
By using 'pipelines_penman' module the program: read cfg-files from 
directories, which are deterimed by filepaths; extract cfg-file
content and transform it; print in console commands, which are fully
ready to move in sql-file as a program body with section separating
comments.

"""

import pipelines_penman as pen

# cfg-file directory paths
filepath_gen = 'C:/korus_DAS/training/task/task2.1_(auto-writer)/csv__cfg_general.csv'
filepath_tab = 'C:/korus_DAS/training/task/task2.1_(auto-writer)/csv__cfg_tables.csv'

# extracting cfg-files contents
input_gen_data, tab_hdrs, cfg_fls_count = pen.gen_cfg_file_preparation(filepath_gen)
for i in range(len(input_gen_data)):
    input_gen_data[i] = pen.rd2wrt_transcriptor(input_gen_data[i])
input_tab_data = pen.tab_cfg_file_preparation(filepath_tab,tab_hdrs)
for i in range(len(input_tab_data)):
    input_tab_data[i] = pen.rd2wrt_transcriptor(input_tab_data[i])

# writing code to consol
env_id, src_sys_count = pen.writedown_general_data(input_gen_data)
id = pen.writedown_src_tables(input_tab_data, env_id)
id = pen.writedown_serv_tables(input_tab_data, env_id, id)
# input list end check
N = len(input_tab_data)
print('Is work done?    -', id >= N)
