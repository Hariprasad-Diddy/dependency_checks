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



if __name__ == '__main__':
    
    # get platform details from user input eg. clickstream/commerce/UDP/DDP
    # get output format from the user eg. excel/csv
    # get the query level details from user eg. month_level/user_count_level/query_count_level
    # get the date range details from user Eg. SD 2023-01-01 ED 2023-02-01 OR 
    # get month level example 1,2 like that and get current day minus number of months data

    platform = input('Enter the platform name to read data : ')
    output_format = input('Enter the output format Eg :- excel / csv : ')
    # output_file_name = input('Enter the output filename : ')
    query_details = input('Enter the data required based on date level / user_level / all_sps : ')
    # date_range = input('Enter how month many data required to extract : ')

    list_sps = ['studio_content_performance','sp_studio_behaviour_by_source'
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
    
    for each_sp in list_sps:
        output_file_name = each_sp
        file_exists = FileExist().check_path_exists(output_file_name)
        query_dict = {
                    'all_sps':all_queries.get(query_details).replace('replace_text',each_sp)
                    }
        
        table_extraction(query_dict[query_details],connection,output_format_dict.get(output_format),output_file_name,each_sp)
        print(each_sp)

    merge_files('output',output_format_dict.get(output_format),'merged_data')  
    
    

    # data = """"""
    # data_df = []
    # for each_sp in list_sps:
    #     query_dict = {
    #                 'all_sps':all_queries.get(query_details).replace('replace_text',each_sp)
    #                 }
    #     cursor.execute(query_dict.get('all_sps'))
            
    #     for sql in cursor.fetchone():
    #         data += f"""{sql}"""
        
        
    #     for line_query in data.splitlines():
            
    #         if line_query.lower().find("drop table") >= 0:
    #             two_dimensional = []
    #             split_line = line_query.split(' ')
    #             file_table_list = list(filter(None, split_line))
    #             # c += 1
    #             two_dimensional.append(each_sp)
    #             two_dimensional.append(file_table_list[-1].replace(';',''))
    #             data_df.append(two_dimensional)
    # df = pd.DataFrame(data_df,columns=['sp_name','table_name'])
    # df.to_csv('/Users/hari.prasad.vc/Desktop/table_list2.csv',index=False)