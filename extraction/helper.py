import re
import pandas as pd
import numpy as np
from extraction.output import *
from extraction.queries import *

class Mail:

    def send_mail(self):
        ...

def tables_in_query(sql_str):
    """function will extract table names from the given query or StoredProcedure

    Args:
        sql_str (_str_): "SELECT * FROM table_name"

    Returns:
        _str_: table_name
    """
    
    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)

    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])

    # split on blanks, parens and semicolons
    tokens = re.split(r"[\s)(;]+", q)

    # scan the tokens. if we see a FROM or JOIN, we set the get_next
    # flag, and grab the next one (unless it's SELECT).

    result = []
    get_next = False
    for tok in tokens:
        if get_next:
            if tok.lower() not in ["", "select"]:
                result.append(tok)
            
            get_next = False
        get_next = tok.lower() in ["from", "join"]

    return result




def table_extraction(query_details,connection,output_format,output_file_name,each_sp):
    """the table_extraction function will extract the table names from 
        given query or stored procedure and save it in file format.
        
    Args:
        query_details (_str_): "SELECT * FROM table_name"
        connection (_classDB_):  DB connection instance
        output_format (_str_): excel / CSV / Json
    """

    pd.set_option('display.max_columns', 500)
    
    data1=pd.read_sql(query_details,connection)
    
    if data1.empty:
        pass
    else:
        
        output = output_format()
        
        list_tables = []
        for i in range(0,len(data1)):
            list_tables.append(tables_in_query(str(data1['query_txt'][i]).lower()))
        data1['tables'] = list_tables

        output.save(data1,output_file_name,f'{each_sp}_sp_details')
        
        df=data1
        dff=df['tables']
        dff=pd.DataFrame(dff)
        dff=dff.reset_index()
        df1=dff.tables.apply(pd.Series)


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
        

        df3=df2['tables'].unique()

        df3=pd.DataFrame(df3)
        # df3.to_excel(df3,'final_output.xlsx')
        output.save(df3,output_file_name,f'{each_sp}_table_details')
        