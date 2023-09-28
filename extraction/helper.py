import re
import os
from pathlib import Path
from io import StringIO
import pandas as pd
import numpy as np
# from extraction.output import *
# from extraction.queries import *

class Mail:

    def send_mail(self) -> None:
        """_summary_
        """
        ...

def merge_files(source_folder_name : str ,output_format,output_file_name : str) -> None:
    """the merge_data_file method used to merge all the SP's individual file data in one consolidated file

    Args:
        source_folder_name (str): '/User/dummy/Desktop/file.csv'
        output_format (_type_): Excel
        output_file_name (str): output_file.csv
    """
    BASE_DIR : Path = Path(__file__).resolve().parent.parent

    output = output_format()

    output_folder : str = os.path.join(BASE_DIR,f'{source_folder_name}')
    
    list_of_files : list = os.listdir(output_folder)
    main_df : list = []

    for each_file in list_of_files:

        if each_file in 'output.xlsx' or '~$' in each_file:
            continue
        else:
            file_path : str = os.path.join(output_folder,each_file)
            try:
                df  : pd.DataFrame = pd.read_excel(file_path,header=0,engine='openpyxl',sheet_name=str(each_file).split('.')[0] + '_table_details')
                main_df.append(df)
            except:
                pass

    df : pd.DataFrame = pd.concat(main_df,ignore_index=True)
    
    output.save(df,output_file_name,f'{output_file_name}')

def tables_in_query(sql_str : str) -> str:
    """function will extract table names from the given query or StoredProcedure

    Args:
        sql_str (_str_): "SELECT * FROM table_name"

    Returns:
        _str_: table_name
    """

    # remove the /* */ comments
    q : str = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)

    # remove whole line -- and # comments
    lines : list = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

    # remove trailing -- and # comments
    q : str = " ".join([re.split("--|#", line)[0] for line in lines])

    # split on blanks, parens and semicolons
    tokens : list = re.split(r"[\s)(;]+", q)

    # scan the tokens. if we see a FROM or JOIN, we set the get_next
    # flag, and grab the next one (unless it's SELECT).

    result : list = []
    get_next : bool = False
    for tok in tokens:
        if get_next:
            if tok.lower() not in ["", "select","into","from","delete","statistics","join","if","end","begin","update","table","with"]:
                result.append(tok)
            
            get_next : bool = False        
        get_next : str = tok.lower() in ["from", "join","delete","update","table","into"]

    return result




def table_extraction(query_details : str ,connection,output_format,output_file_name : str ,each_sp : str) -> None:
    """the table_extraction function will extract the table names from 
        given query or stored procedure and save it in file format.
        
    Args:
        query_details (_str_): "SELECT * FROM table_name"
        connection (_classDB_):  DB connection instance
        output_format (_str_): excel / CSV / Json
    """

    pd.set_option('display.max_columns', 500)
    
    data1 : pd.DataFrame =pd.read_sql(query_details,connection)
    if data1.empty:
        pass
    else:
        
        output = output_format()
        
        list_tables : list = []
        for i in range(0,len(data1)):
            list_tables.append(tables_in_query(str(data1['query_txt'][i]).lower()))
        data1['tables'] = list_tables

        output.save(data1,output_file_name,f'{each_sp}_sp_details')
        
        df : pd.DataFrame =data1
        dff = df['tables']
        dff : pd.DataFrame = pd.DataFrame(dff)
        dff = dff.reset_index()
        df1 = dff.tables.apply(pd.Series)


        dff.tables.apply(pd.Series) \
        .merge(dff, right_index = True, left_index = True)


        dff.tables.apply(pd.Series) \
    .merge(dff, right_index = True, left_index = True) \
    .drop(["tables"], axis = 1) \
    .melt(id_vars=['index'],value_name = "tables")
        



        df2 = dff.assign(tables=dff.tables.str.split(","))
        df2 = dff.tables.apply(pd.Series) \
            .merge(dff, right_index=True, left_index=True) \
            .drop(["tables"], axis=1) \
            .melt(id_vars=['index'],value_name = "tables") \
            .drop("variable", axis=1) \
            .dropna()
        
        
        df3 = df2['tables'].unique()

        df3 : pd.DataFrame = pd.DataFrame(df3)
        # df3.to_excel(df3,'final_output.xlsx')
        df3.columns = ["table_name"]
        df3['sp_name'] = pd.Series([each_sp for x in range(len(df3.index))])
        output.save(df3,output_file_name,f'{each_sp}_table_details')





def read_input() -> list:
    """read_input method reads the Store Procedure names from the input.csv file and return the list of Store procedure

    Returns:
        list: ['SP1','SP2']
    """
    BASE_DIR : Path = Path(__file__).resolve().parent.parent
    input_path : str = os.path.join(BASE_DIR,'input.csv')
    
    list_sps : list = []
    with open(input_path,'r') as f:
        for sp in f.readlines():    
            list_sps.append(re.sub('\n','',sp))
    return list_sps