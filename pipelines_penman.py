"""

The module for creation text file with code, which must create 
pipeline for transition data from source systems to dds-schema of
hdfs. Are creating: source tables, serving tables in dds_lgc, dds_lnk
and dds schemas, DAGs, DAG steps and dependences between them.
Creation are carried out with metaload v_1.3 functions. 
Writing such functions to output text file in command form with
required parameters are formed by module "mtl_v1_3.py" tools.

DONE:
Way to take required parameters for creating tables from
configuration files developed. Regrouping and transition readed
parameters to writing functions are implemented. Writing functions
do not write command text into any output file, they just print text
into console.

PLANNED:
Writing code in a file. Reading and writing functions for DAG command
block.

"""

import mtl_v1_3 as mtl
import csv

##############################################################################
## read configuration files block ############################################
##############################################################################

def csv_reader(filepath: str, delimiter:str = ';'):
    """
    Separates csv-configuration-file on two parts: column of field
    names (or field labels) and values (one or more items).
    Requirements to the input file:
     - first column contains the number of (ints) fill fields in each
    lines (to the right of this value excluding the one);
      - second column contains names or labels for corresponding row
    or separator comment (starts by symbol '#');
     - third part: unlimited columns count with the values (text or
    numeric)
    
    Input:
        filepath: str - configuration file path;
        delimiter: str - default ';' - string element separator.
    Output:
        labels: list - list of value labels (parameter row labels);
        values: list of lists - list of inner lists with values;
            allowed empty inner lists.

    """
    # CONSTANTS
    #     defining column numbers
    SERV_COL_NO = 0
    LBL_COL_NO = 1
    PRMT_COL_NO = 2
    # body
    labels = list()
    values = list()
    with open(filepath) as cfg_file:
        file = csv.reader(cfg_file, delimiter = delimiter)
        for row in file:
            elem_count = int(row[SERV_COL_NO])
            labels.append(row[LBL_COL_NO])
            info = [row[i] for i in range(PRMT_COL_NO, elem_count+1)]
            values.append(info)
    return labels, values

def dict_formation(lable, keys: list, items: list):
    """
    Returns dictionaty that contains <key>-<item> pairs and 'mode'-key
    with item from lable. The length of keys-list must be not longer
    than items-list.
    
    Input:
        lable - any element;
        keys: list - list of keys for the formed dictionary;
        items: list - list if items for the formed dictionary.
    Output:
        result: dict - dictionary with pairs <key>-<item> and
            <'mode'>-<lable>.
    
    """
    new_dict = {'mode': lable}
    for i in range(len(keys)):
        new_dict[keys[i]] = items[i]
    
    return new_dict
    
def gen_params_extract(mode_list: list, prmts: list,
                       prmt_hdrs: list, start_i: int = 0
                      ):
    """
    Rerurn list of dictionaries, each of them contains a row of
    parameters, which is required for a corresponding metaload
    functions.

    Input:
        mode_list: list - key-list - specified values 'mode' which
            define purpose of each inner list of 'prmts'.  
        prmts: list of lists - each inner list contains a number of
            single elements: dictionary <items>;
        prmt_hdrs: list - list of dictionaries of parameter names
            (result dict <keys>) for possible 'mode' values
        start_i: int - default value: 0 - initial index from which
            processing of lists 'mode_list' and 'prmts' begins.
    Output:
        gen_funcs_params: list - list of dictionaries with pairs 
            <prmt_hdr>-<prmt> and pair a key <'mode'>-<mode_value>.
    
    """
    # CONSTANTS
    # the first one is rarer among <NAME_>
    NAME_1 = 'env'
    NAME_2 = 'src_sys'
    # body
    file_rows_count = len(mode_list)
    first_list = list()
    second_list = list()
    for i in range(start_i, file_rows_count):
        mode = mode_list[i]
        hdrs = prmt_hdrs[mode]
        prmts_i = prmts[i]
        if mode == NAME_2:
            second_list.append(
                dict_formation(mode, hdrs, prmts_i)
            )
        elif mode == NAME_1:
            first_list.append(
                dict_formation(mode, hdrs, prmts_i)
            )
    return first_list, second_list

def gen_cfg_file_preparation(filepath: str, delimiter:str = ';'):
    """
    Extract from general cfg-file in csv-format сonfiguration
    data and parameters for metaload functions that set envitonment
    and add source-systems. The headers for groups of parameters and
    total cfg_files count also come out.

    Input:
        filepath: str - configuration file path;
        delimiter: str - default ';' - string element separator.
    Output:
        in_gen_param_data: list of dicts - parameter value block for
            general functions (set_env and add_src_schema);
        tab_parameter_headers: list
        cfg_files_count: int - total number of configuration files.

    (* in future versions, you need to add headers for the DAG block)
    """
    # CONSTANTS
    CFG_FC_ROW = 0
    ENV_HDRS_ROW = 3
    SRC_HDRS_ROW = 4
    SRC_T_HDRS_ROW = 7
    SRC_C_HDRS_ROW = 8
    SRV_T_HDRS_ROW = 9
    SRV_C_HDRS_ROW = 10
    DAG_NM_ROW = 13
    DAG_ST_ROW = 14
    DAG_DEP_ROW = 15
    # body
    labels, values = csv_reader(filepath, delimiter = delimiter)
    cfg_files_count = int(values[CFG_FC_ROW][0])
    hdrs_gen = dict(
        {
            labels[ENV_HDRS_ROW]: values[ENV_HDRS_ROW],
            labels[SRC_HDRS_ROW]: values[SRC_HDRS_ROW]
        }
    )
    hdrs_tab = dict(
        {
            labels[SRC_T_HDRS_ROW]: values[SRC_T_HDRS_ROW],
            labels[SRC_C_HDRS_ROW]: values[SRC_C_HDRS_ROW],
            labels[SRV_T_HDRS_ROW]: values[SRV_T_HDRS_ROW],
            labels[SRV_C_HDRS_ROW]: values[SRV_C_HDRS_ROW],
        }
    )
    start_i = SRV_C_HDRS_ROW + 1
    while labels[start_i] != '#cfg_end':
        start_i += 1
    
    gen_prmtrs1, gen_prmtrs2 = gen_params_extract(labels, values, hdrs_gen, start_i+1) 
    for second_list_row in gen_prmtrs2:
        gen_prmtrs1.append(second_list_row)
    out = [
        gen_prmtrs1,
        hdrs_tab,
        cfg_files_count
    ]
    return out

def tab_params_extract(mode_list: list, prmts: list,
                       prmt_hdrs: list, start_i: int = 0
                      ):
    """
    Rerurn list of dictionaries, each of them contains a row of
    item from <prmts> with keys from <prmt_hdrs>. Each cople for
    dicts are conected by means of <mode_list>. Processing starts
    from row with index "start_i".

    Input:
        mode_list: list - key-list - specified values 'mode' which
            define purpose of each inner list of 'prmts'.  
        prmts: list of lists - each inner list contains a number of
            single elements: dictionary <items>;
        prmt_hdrs: list - list of dictionaries of parameter names
            (result dict <keys>) for possible 'mode' values
        start_i: int - default value: 0 - initial index from which
            processing of lists 'mode_list' and 'prmts' begins.
    Output:
        gen_funcs_params: list - list of dictionaries with pairs 
            <prmt_hdr>-<prmt> and pair a key <'mode'>-<mode_value>.
    
    """
    # CONSTANTS
    NAME_1 = 'src_col'
    NAME_2 = 'serv_col'
    NAME_3 = 'serv_table'
    NAME_4 = 'src_table'
    # body
    file_rows_count = len(mode_list)
    out_list = list()
    for i in range(start_i, file_rows_count):
        mode = mode_list[i]
        if mode[0] =='#':
            pass
        else:
            hdrs = prmt_hdrs[mode]
            prmts_i = prmts[i]
            out_list.append(
                dict_formation(mode, hdrs, prmts_i)
            )
        

    return out_list

def tab_cfg_file_preparation(filepath: str, headers: list, delimiter:str = ';'):
    """
    Extract from table cfg-file - in csv-format - сonfiguration data
    and parameters for metaload functions, which add source and
    serving tables.

    Input:
        filepath: str - configuration file path;
        headers: list - list of dictionaries with parameter names for
            diferent sets;
        delimiter: str - default ';' - string element separator.
    Output:
        in_tab_param_data: list of dicts - parameter value block for
            table functions (add_{source/serving}_{table/column}).

    """
    labels, values = csv_reader(filepath, delimiter = delimiter)
    in_tab_param_data = tab_params_extract(labels,values, headers)

    return in_tab_param_data

def rd2wrt_transcriptor(d: dict):
    """
    Transforms dictionary items depending on their values.
    Transformations: 'null' -> 'null'; 'digit' -> int;
                     'text' -> "'text'".
    The items which are not string on input wouldn't be changed.
    Also items with key 'mode' wouldn't be changed.

    Input:
        d: dict - any dictionary.
    Output:
        d: dict - dictionary with transformed items.

    """
    if type(d) is not dict:
        return d
    dic_keys = list(d.keys())
    for key in dic_keys:
        if key == 'mode': continue
        item = d[key]
        if type(item) is not str: continue
        if item.isdigit():
            d[key] = int(item)
        elif item != 'null':
            d[key] = "'" + item + "'"
    return d

##############################################################################
## output of the resulting program text ######################################
##############################################################################

def add_comment(comment_text: str, char_count: int = 100):
    """
    Prepares a comment line with input text and the specified length.

    Input:
        comment_text: str - a text of the comment;
        char_count: int - a nimber of chars in comment line.
    Output:
        comment_line: str - a comment line

    """
    if comment_text[-1] != ' ':
        comment_text += ' '
    comment_start = '-- '
    comment_end = '-' * (char_count - len(comment_start) - len(comment_text))
    comment_line = comment_start + comment_text + comment_end
    return comment_line

def writedown_general_data(in_data: list):
    """
    Writes down command lines related to setting environment and
    adding source-systems.

    Input:
        in_data: list - input list of command parameters
    Output:
        env_id: str - environment scheme version identifier, that is
            specified.

    """
    # Constants
    LIMIT = 4
    START_ID = 1
    SHOW_INPUT_CONTENT = False
    # function body
    if SHOW_INPUT_CONTENT:
        input_row_count = len(in_data)
        if input_row_count > LIMIT:
            input_row_count = LIMIT
        for id in range(input_row_count):
            print(id, in_data[id], sep = '\t')
    src_systems_count =  len(in_data) - START_ID
    env_id = in_data[0]['env_id']
    N = len(in_data)
    print('\n' + \
          mtl.pre_post_fix(mtl.set_env(in_data[START_ID]['env_id']))
          )
    print(add_comment('ADD SOURCE_SYSTEMS'))
    for id in range(START_ID, N):
        params = in_data[id]
        print(mtl.pre_post_fix(mtl.f_add_source_system(
            params['env_id'],
            params['src_name'],
            params['descript']
             )))
    return env_id, src_systems_count

def writedown_add_src_tables(in_data: list, env_id: str, id: int):
    """
    Writes down command lines that creates source tables.
    
    Input:
        in_data: list - list of parameter blocks (in 'dict' type);
        env_id: str - an identifier of environment where tables will
            be created;
        id: int - the in_data list row number from which this function
            starts its work.
    Output:
        stop_id: int - the in_data list row number, where this function
            stoped its work.

    """
    N = len(in_data)
    print(add_comment('ADD SOURCE_TABLES'))
    if id < N:
        running_flag = in_data[id]['mode'] == 'src_table'
    else: running_flag = False
    while running_flag:
        params = in_data[id]
        print(mtl.pre_post_fix(mtl.f_add_source_table(
            env_id,
            params['src_name'],
            params['tablename'],
            params['subsystem'],
            params['src_schema']
        )))
        id += 1
        if id < N:
            running_flag = in_data[id]['mode'] == 'src_table'
        else: running_flag = False
    print(add_comment('END SOURCE_TABLES'))
    return id

def writedown_add_src_columns(in_data: list, env_id: str, id: int):
    """
    Writes down command lines that creates source table columns.
    
    Input:
        in_data: list - list of parameter blocks (in 'dict' type);
        env_id: str - an identifier of environment where tables will
            be created;
        id: int - the in_data list row number from which this function
            starts its work.
    Output:
        stop_id: int - the in_data list row number, where this function
            stoped its work.

    """
    N = len(in_data)
    print(add_comment('ADD SOURCE_COLUMNS'))
    if id < N:
        running_flag = in_data[id]['mode'] == 'src_col'
    else: running_flag = False
    while running_flag:
        params = in_data[id]
        print(mtl.pre_post_fix(mtl.f_add_source_column(
                tablename = params['tablename'],
                env_id = env_id,
                src_name = params['src_name'],
                src_schema = params['src_schema'],
                column_name = params['column_name'],
                data_type = params['data_type'],
                precision = params['precision'],
                scale = params['scale'],
                key_flg = params['key_flg'],
                batch_flg = params['batch_flg'],
                date_prc_flg = params['date_prc_flg']
        )))
        id += 1
        if id < N:
            running_flag = in_data[id]['mode'] == 'src_col'
        else: running_flag = False
    print(add_comment('END SOURCE_COLUMNS'))
    return id

def writedown_src_tables(in_data: list, env_id, id: int = 0):
    """
    Writes down command lines that creates source tables and fields
    in them for different source systems. Command lines are separated
    by systems at first, then, inside each system, by tables and
    fields.

    Input:
        in_data: list - list of parameter blocks (in 'dict' type).
        env_id: str - an identifier of environment where tables will
        be created.
    Output:
        stop_id: int - the in_data list row number, where this function
        stoped its work.

    """
    # constants
    ITER_LIMIT = 5000
    # function body
    N = len(in_data)
    iter = 0
    if id < N:
        src_tab_flag = in_data[id]['mode'] == 'src_table' or \
                       in_data[id]['mode'] == 'src_col'
    else: src_tab_flag = False
    while src_tab_flag:
        current_src_name = in_data[id]['src_name']
        print(add_comment('ADD SOURCE_SYSTEM ' \
                          + current_src_name.replace("'", ""))
              )
        print('\n' + \
              mtl.pre_post_fix(mtl.set_env(env_id)))
        
        id = writedown_add_src_tables(in_data, env_id, id)
        id = writedown_add_src_columns(in_data, env_id, id)
        
        print(add_comment('END SOURCE_SYSTEM ' \
                          + current_src_name.replace("'", "")))
        if id < N:
            src_tab_flag = in_data[id]['mode'] == 'src_table' or \
                           in_data[id]['mode'] == 'src_col'
        else: src_tab_flag = False
        iter += 1
        if iter > ITER_LIMIT:
            print('TimeOutError')
            break
    
    return id

def writedown_add_serv_tables(in_data: list, env_id: str, id: int):
    """
    Writes down command lines that creates serving tables.
    Input:
        in_data: list - list of parameter blocks (in 'dict' type);
        env_id: str - an identifier of environment where tables will
            be created;
        id: int - the in_data list row number from which this function
            starts its work.
    Output:
        stop_id: int - - the in_data list row number, where this function
            stoped its work.
    """
    N = len(in_data)
    print(add_comment('ADD SERVING_TABLES'))
    if id < N:
        running_flag = in_data[id]['mode'] == 'serv_table'
    else: running_flag = False
    while running_flag:
        params = in_data[id]
        print(mtl.pre_post_fix(mtl.f_add_serving_table(
            env_id = env_id,
            schema_name = params['schema_name'],
            tablename = params['tablename'],
            key_shifting_type = params['key_shifting_type']
        )))
        id += 1
        if id < N:
            running_flag = in_data[id]['mode'] == 'serv_table'
        else: running_flag = False
    print(add_comment('END SERVING_TABLES'))
    return id

def writedown_add_serv_columns(in_data: list, env_id: str, id: int):
    """
    Writes down command lines that creates serving table columns.
    
    Input:
        in_data: list - list of parameter blocks (in 'dict' type);
        env_id: str - an identifier of environment where tables will
            be created;
        id: int - the in_data list row number from which this function
            starts its work.
    Output:
        stop_id: int - the in_data list row number, where this function
            stoped its work.

    """
    N = len(in_data)
    print(add_comment('ADD SERVING_COLUMNS'))
    if id < N:
        running_flag = in_data[id]['mode'] == 'serv_col'
    else: running_flag = False
    while running_flag:
        params = in_data[id]
        print(mtl.pre_post_fix(mtl.f_add_serving_column(
            env_id = env_id,
            schema_name = params['schema_name'],
            tablename = params['tablename'],
            column_name = params['column_name'],
            data_type = params['data_type'],
            precision = params['precision'],
            scale = params['scale'],
            key_flg = params['key_flg']
        )))
        id += 1
        if id < N:
            running_flag = in_data[id]['mode'] == 'serv_col'
        else: running_flag = False
    print(add_comment('END SERVING_COLUMNS'))
    return id

def writedown_serv_tables(in_data: list, env_id: str, id: int):
    """
    Writes down command lines that creates serving tables and fields
    in them for different schemas. Command lines are separated by
    tables and fields.

    Input:
        in_data: list - list of parameter blocks (bloks have'dict'
            type);
        env_id: str - an identifier of environment where tables will
            be created;
        id: int - the in_data list row number from which this function
            starts its work.
    Output:
        stop_id: int - the in_data list row number, where this function
            stoped its work.
    """
    # constants
    ITER_LIMIT = 5000
    # function body
    N = len(in_data)
    iter = 0
    if id < N:
        serv_tab_flag = in_data[id]['mode'] == 'serv_table' or \
                        in_data[id]['mode'] == 'serv_col'
    else: serv_tab_flag = False
    print(add_comment('ADD SERVING_LAYERS'))
    print('\n' + \
              mtl.pre_post_fix(mtl.set_env(env_id)))
    while serv_tab_flag:
        id = writedown_add_serv_tables(in_data, env_id, id)
        id = writedown_add_serv_columns(in_data, env_id, id)
        if id < N:
            serv_tab_flag = in_data[id]['mode'] == 'serv_table' or \
                            in_data[id]['mode'] == 'serv_col'
        else: serv_tab_flag = False
        iter += 1
        if iter > ITER_LIMIT:
            print('TimeOutError')
            break
    print(add_comment('END SERVING_LAYERS'))
    return id

