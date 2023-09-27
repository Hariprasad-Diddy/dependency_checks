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
from extraction.method_extraction import TableNames,Commands
from extraction.method_extraction import MergeData

if __name__ == '__main__':
    
    # get platform details from user input eg. clickstream/commerce/UDP/DDP
    # get output format from the user eg. excel/csv
    # get the query level details from user eg. month_level/user_count_level/query_count_level
    # get the date range details from user Eg. SD 2023-01-01 ED 2023-02-01 OR 
    # get month level example 1,2 like that and get current day minus number of months data
    platform = input('Enter the platform name to read data : ')
    output_format = input('Enter the output format Eg :- excel / csv : ')
    final_file_name = input('Enter the output file name : ')
    query_details = 'all_sps' #input('Enter the data required based on date level / user_level / all_sps : ')
    # date_range = input('Enter how month many data required to extract : ')
    
    list_sps = read_input()

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
    
    TableNames().table_name_extraction(
                    list_sps,
                    all_queries,
                    query_details,
                    connection,
                    output_format_dict,
                    output_format,
                    FileExist,
                    table_extraction
                    )
    

    file_exists = FileExist().create_or_replace_file('merged_data')
    merge_files('output',output_format_dict.get(output_format),'merged_data')  
    
    

    commands = Commands()
    data_df = commands.table_command_extraction(
            list_sps,
            all_queries,
            query_details,
            cursor,
            output_format_dict,
            output_format,
            FileExist)
    
    merge = MergeData()
    merge.merge_data_files(data_df,final_file_name)