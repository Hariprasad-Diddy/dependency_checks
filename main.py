from configuration.config_reader import ConfigInformation
from db_connect.connect import ClickstreamDB
from db_connect.connect import CommerceDB
from db_connect.connect import ErrorManagementDB
from db_connect.connect import AdhocQueryDB
from db_connect.connect import RBACDB
from db_connect.connect import PersonifyDB
from db_connect.connect import BifrostDB
import sys
import pandas as pd
import re
from extraction.queries import all_queries
from extraction.helper import *
from extraction.output import *

# # line
# pd.set_option('display.max_columns', 500)
# # line
# data1=pd.read_sql(query1,clickstream_connection)
# # line
# print(data1)
# # line
# data1.to_excel('last_six_months.xlsx')
# # line
# def tables_in_query(sql_str):

#     # remove the /* */ comments
#     q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)

#     # remove whole line -- and # comments
#     lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

#     # remove trailing -- and # comments
#     q = " ".join([re.split("--|#", line)[0] for line in lines])

#     # split on blanks, parens and semicolons
#     tokens = re.split(r"[\s)(;]+", q)

#     # scan the tokens. if we see a FROM or JOIN, we set the get_next
#     # flag, and grab the next one (unless it's SELECT).

#     result = []
#     get_next = False
#     for tok in tokens:
#         if get_next:
#             if tok.lower() not in ["", "select"]:
#                 result.append(tok)
#             get_next = False
#         get_next = tok.lower() in ["from", "join"]

#     return result


# # line
# ## DDP 
# list_tables = []
# for i in range(0,len(data1)):
#     list_tables.append(tables_in_query(str(data1['query_txt'][i]).lower()))
# data1['tables'] = list_tables
# # line
# print(data1)

# # line
# data1.to_excel('aadw_query_may.xlsx')
# # line
# data1.to_csv('aadw_query_may.csv')
# # line
# data1.to_excel('last_six_months.xlsx')
# # line
# df=data1
# # line
# print(df)
# # line
# dff=df['tables']
# # line
# dff=pd.DataFrame(dff)
# # line
# dff
# # line
# dff=dff.reset_index()
# # line
# dff
# # line
# df1=dff.tables.apply(pd.Series)
# # line
# print(df1)
# # line
# dff.tables.apply(pd.Series) \
#     .merge(dff, right_index = True, left_index = True)
# # line
# dff.tables.apply(pd.Series) \
#    .merge(dff, right_index = True, left_index = True) \
#    .drop(["tables"], axis = 1) \
#    .melt(id_vars=['index'],value_name = "tables")
# # line
# df2 = dff.assign(tables=dff.tables.str.split(","))
# df2 = dff.tables.apply(pd.Series) \
#     .merge(dff, right_index=True, left_index=True) \
#     .drop(["tables"], axis=1) \
#     .melt(id_vars=['index'],value_name = "tables") \
#     .drop("variable", axis=1) \
#     .dropna()
  
# print(df2)
# # line
# print(df2)
# # line
# df3=df2['tables'].unique()
# # line
# df3=pd.DataFrame(df3)
# # line
# df3
# # line
# df3.to_excel('a.xlsx')
# # line
# df3.to_csv('a.xlsx')







if __name__ == '__main__':
    
    # get platform details from user input eg. clickstream/commerce/UDP/DDP
    # get output format from the user eg. excel/csv
    # get the query level details from user eg. month_level/user_count_level/query_count_level
    # get the date range details from user Eg. SD 2023-01-01 ED 2023-02-01 OR 
    # get month level example 1,2 like that and get current day minus number of months data

    platform = input('Enter the platform name to read data : ')
    output_format = input('Enter the output format Eg :- excel / csv : ')
    output_file_name = input('Enter the output filename : ')
    query_details = input('Enter the data required based on date level / user_level / all_sps : ')
    # date_range = input('Enter how month many data required to extract : ')

    list_sps = [
                'sp_studio_content_performance'
                ]

    
        
    # for platform in all_platform:
    platform_dict = {
                    'clickstream':ClickstreamDB(),
                    'commerce':CommerceDB(),
                    'personify':PersonifyDB(),
                    'error_management':ErrorManagementDB(),
                    'adhoc':AdhocQueryDB(),
                    'rbac':RBACDB(),
                    'udp':ClickstreamDB(),
                    'ddp':CommerceDB(),
                    'vm':'vm',
                    'biforst':BifrostDB()
                    }
    
    output_format_dict = {
                    'excel':Excel,
                    'csv':CSV
                    }
    config_details = ConfigInformation('configuration','config.ini')
    
    database_connection_instance = platform_dict.get(platform)
    connection = database_connection_instance.db_connection(config_details.config[platform])
    cursor = connection.cursor()

    # mysql_instance = MySQLDB()
    # mysql_connection = mysql_instance.db_connection(config_details.config[platform_dict[platform]])
    # print("Connected to MySql Instance...!",mysql_connection)
    file_exists = FileExist().check_path_exists(output_file_name)
    
    for each_sp in list_sps:
        query_dict = {
                    'all_sps':all_queries.get(query_details).replace('replace_text',each_sp)
                    }
        
        table_extraction(query_dict[query_details],connection,output_format_dict.get(output_format),output_file_name,each_sp)
        print(f"{each_sp} extracted...!")
        