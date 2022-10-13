"""

VERSION: 1.2
DATE FORMAT: (MM-DD-YYYY)
DATE OF LAST CHANGE: 10-06-2022,
Kazachenko M.V tm/@iNecron: V.1.0. The content and purpose of the
module are formulated. List of required functions added.

There is a set of functions that forms commands of the metaload v.1_3
(the list of performing commands is given below) for the formation of
a pipeline for tnrasferring data from a source to a DDS scheme. The 
commands are forming in the text format.

The performing metaload v.1_3 functions list:
1) mtl_prj_ctl.f_set_version_schema()
2) f_add_source_system()
3) f_add_source_table()
4) f_add_source_column()
5) f_add_serving_table()
6) f_add_serving_column()
7) f_get_tab_id()
8) f_get_serving_table_id()

"""

def set_env(env_id: str):
    """

    Form a text of command, that set an environment scheme version.
    The command is a metaload function:
        mtl_prj_ctl.f_set_version_schema('env_id').

    Input:
        env_id: str - environment scheme version identifier.
    Output:
        commandline: str - text of the command.

    """
    commandline = "mtl_prj_ctl.f_set_version_schema(" + env_id + ")"
    return commandline

def f_add_source_system(env_id: str, src_name: str, descript:str = 'null'):
    """

    Form a text of command, that add an source-system. The command is
    a metaload function:
        select f_add_source_system('env_id','src_name','descript');

    Input:
        env_id: str - environment scheme version identifier;
        src_name: str - source-system name
        descript: str - description of the source-system;
            default value: 'null'.
    Output:
        commandline: str - text of the command.

    """
    commandline = "f_add_source_system(" \
                    + env_id + ", " \
                    + src_name + ", " \
                    + descript + ")"
    return commandline

def f_get_tab_id(env_id: str, tablename:str, src_name: str,
                 src_schema:str = "'public'"
                 ):
    """
    
    Form a text of command, that requests the identifier of the given
    source table. The command is a metaload function:
        f_get_tab_id('env_id','src_scheme','tablename','src_name');
    
    Input:
        env_id: str - environment scheme version identifier;
        tablename: str - name of table on the source
        src_name: str - name of a source-system, the one contains the
            table;
        src_schema: str - name of a source-scheme, the one contains
        the table; default value: 'public'.
    Output:
        commandline: str - text of the command.
    """
    commandline = 'f_get_tab_id(' \
                    + env_id + ', ' \
                    + src_schema + ', ' \
                    + tablename + ', ' \
                    + src_name + ')'
    return commandline

def f_get_serving_table_id(env_id: str, schema_name: str, tablename:str):
    """
    
    Form a text of command, that requests the identifier of the given
    serving table. The command is a metaload function:
        f_get_serving_tab_id('env_id','src_scheme','tablename','src_name');
    
    Input:
        env_id: str - environment scheme version identifier;
        schema_name: str - name of a source-scheme, the one contains
            the table;
        tablename: str - name of table on the source;
    Output:
        commandline: str - text of the command.

    """
    commandline = "f_get_serving_tab_id(" \
                    + env_id + ", " \
                    + schema_name + ", " \
                    + tablename + ")"
    return commandline

def gen_source_table_name(subsystem: str, env_id: str,
                          src_name: str, tablename: str
                          ):
    """
    Generates a name of a source table according to naming rules.

    """
    newtablename = "'" \
        + subsystem.replace("'","") + '__' \
        + env_id.replace("'","") + '__' \
        + src_name.replace("'","") + '__' \
        + tablename.replace("'","") + '__data' \
        + "'"
    return newtablename

def gen_serving_table_name(tablename: str, schema_name: str):
    """
    Generates a name of a source table according to naming rules.

    """
    if schema_name == "'dds_lnk'":
        servtablename = "'" + tablename[1:-1] + '_v' + "'"
    elif schema_name == "'dds_lgc'" or schema_name == "'dds'":
        servtablename = "'" + tablename[1:-1] + '_t' + "'"
    else:
        print('ERROR: Invalid logical schema name!')
        return 
    return servtablename 

def f_add_source_table(env_id: str, src_name: str, tablename: str,
                       subsystem: str = "'sys'",
                       src_schema: str = "'public'"
                       ):
    """
    
    Form a text of command, that add an source-table. The command is
    a metaload function:
        f_add_source_table('env_id','src_name','tablename',
                            'subsystem','src_schema',
                            'newtablename');
    
    Input:
        env_id: str - environment scheme version identifier;
        src_name: str - name of a source-system, the one contains the
            table;
        tablename: str - name of table on the source;
        subsystem: str - name of the subsystem/DB/namespace; default
            value: 'sys';
        src_schema: str - name of a source-scheme, the one contains
        the table; default value: 'public';
        
        *newtablename: str - NOT REQUIRED AS AN INPUT PARAMETER - name
            of the table, that this command will create; the name is
            generated according to certain rules (by another function
            or other ways).
    Output:
        commandline: str - text of the command.
    
    """
    newtablename = gen_source_table_name(subsystem, env_id, src_name, tablename)
    commandline = "f_add_source_table(" \
                    + env_id + ", " \
                    + src_name + ", " \
                    + tablename + ", " \
                    + subsystem + ", " \
                    + src_schema + ", " \
                    + newtablename + ")"
    return commandline

def f_add_source_column(tablename: str, env_id: str, src_name: str, 
                        column_name: str, data_type: str,
                        src_schema: str = "'public'",
                        precision: int = 'null',
                        scale: int = 'null',
                        key_flg: str = "'n'",
                        batch_flg: str = "'n'",
                        date_prc_flg: str = "'n'"):
    """
    
    Form a text of command, that add an field to a source-table. The
    command is a metaload function:
        f_add_source_column('tab_id', 'column_name', 'data_type',
                            'precision', 'scale', 'key_flg',
                            'batch_flg', 'date_prc_flg',
                            'nullable');
    
    Input:
        tab_id - NOT REQUIRED AS AN INPUT PARAMETER - table identifier;
            defined using the function 'f_get_tab_id';
        column_name: str - table field name;
        data_type: str - field data type;
        precision: int - amount of symbols of the field;
            default value: null;
        scale: int - amount of digits after comma;
            default value: null;
        key_flg: str - sign of an integration key ('y' or 'n') by key;
            default value: 'n'.
        batch_flg: str - sign of an integration key ('y' or 'n') by 
            batch; default value: 'n'.
        date_prc_flg: str - sign of an integration key ('y' or 'n')
            by processing date; default value: 'n'.
        nullable: boolean - OPTIONAL. Flag of optional field;
            default value: True.
    Output:
        commandline: str - text of the command.

    """
    tab_id = f_get_tab_id(env_id, tablename, src_name, src_schema)
    commandline = "f_add_source_column(" \
                    + tab_id + ', ' \
                    + column_name + ', ' \
                    + data_type + ', ' \
                    + precision + ', ' \
                    + scale + ', ' \
                    + key_flg + ', ' \
                    + batch_flg + ', ' \
                    + date_prc_flg + ")"
    return commandline

def f_add_serving_table(env_id: str, schema_name: str, tablename: str,
                        key_shifting_type: str = "'LOCAL'"):
    """
    
    Form a text of command, that add an serving-table. The command is
    a metaload function:
        f_add_serving_table('env_id', 'schema_name',
                            'servtablename', 'key_shifting_type')
    
    Input:
        env_id: str - environment scheme version identifier;
        schema_name: str - name of a serving schema, the one contains
        the table;
        tablename: str - name of the table on the source or another
            table name;
        key_shifting_type: str - serving key generation mode;
            default value: "'LOCAL'"; Any posible values: "'GLOBAL'",
            "'NONE'".
    Output:
        commandline: str - text of the command.

    """
    servtablename = gen_serving_table_name(tablename, schema_name)
    if servtablename == None:
        return
    commandline = "f_add_serving_table(" \
                  + env_id + ', ' \
                  + schema_name + ', ' \
                  + servtablename + ', ' \
                  + key_shifting_type + ")"
    return commandline

def f_add_serving_column(env_id: str, schema_name: str, tablename: str,
                        column_name: str, data_type: str,
                        precision: int = 'null', scale: int = 'null',
                        key_flg: str = "'n'"
                        ):
    """
    
    Form a text of command, that add an field to a source-table. The
    command is a metaload function:
        f_add_serving_column('tab_id', 'column_name', 'data_type',
                             'precision', 'scale', 'key_flg')
        
    Input:
        tab_id - NOT REQUIRED AS AN INPUT PARAMETER - table identifier;
            defined using the function 'f_get_serving_table_id';
        column_name: str - table field name;
        data_type: str - field data type;
        precision: int - amount of symbols of the field;
            default value: null;
        scale: int - amount of digits after comma;
            default value: null;
        key_flg: str - sign of an integration key ('y' or 'n');
            default value: 'n'.
    Output:
        commandline: str - text of the command.
    
    
    """
    tab_id = f_get_serving_table_id(env_id, schema_name, tablename)
    commandline = "f_add_serving_column(" \
                    + tab_id + ', ' \
                    + column_name + ', ' \
                    + data_type + ', ' \
                    + precision + ', ' \
                    + scale + ', ' \
                    + key_flg + ")"
    return commandline

def pre_post_fix(line: str, prefix: str = "select ", postfix: str = ";"):
    """
    Add the prefix and the postfix to the line.
    """
    if line == None:
        return ''
    result = prefix + line + postfix
    return result

