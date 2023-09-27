import os
import pandas as pd
from pathlib import Path

class TableNames:

    def __init__(self) -> None:
        """_summary_
        """
        pass


    def table_name_extraction(self,list_sps : list,all_queries : dict ,query_details : str,connection,output_format_dict : dict,output_format : str,FileExist,table_extraction) -> None:
        """the table_name_extraction method extracts the only table names from the Stored Procedure and save the output in the file

        Args:
            list_sps (list): ['SP1','SP2]
            all_queries (dict): {'all_sps':'SELECT * FROM table'}
            query_details (str): 'SELECT * FROM table1'
            connection (_type_): DataBase class
            output_format_dict (dict): {'excel':Excel,'csv':CSV}
            output_format (str): 'Excel'
            FileExist (_type_): FileExist class
            table_extraction (_type_): table_extraction method
        """
        for each_sp in list_sps:
            
            output_file_name : str = each_sp
            file_exists : FileExist = FileExist().create_or_replace_file(output_file_name)

            query_dict : dict = {
                    'all_sps':all_queries.get(query_details).replace('replace_text',each_sp),
                    'dependent':all_queries.get('dependent').replace('replace_sp_name',each_sp)
                    }
        
            table_extraction(query_dict[query_details],connection,output_format_dict.get(output_format),output_file_name,each_sp)
            print(each_sp)



class Commands:

    def __init__(self) -> None:
        """_summary_
        """
        pass


    def table_command_extraction(self,list_sps : list,all_queries : dict,query_details : str,cursor,output_format_dict : dict,output_format : str,FileExist) -> list:
        """the table_command_extraction method extracts the commands used for the table from the Stored Procedure and save the output in the file.
        e.g : table1 DROP and CREATE
              table2 TARGET
              table3 SOURCE

        Args:
            list_sps (list): ['SP1','SP2']
            all_queries (dict): {'all_sps':'SELECT * FROM table'}
            query_details (str): 'SELECT * FROM table1'
            cursor ([type]): DataBase Cursor class
            output_format_dict (dict): {'excel':Excel,'csv':CSV}
            output_format (str): 'Excel'
            FileExist ([type]): FileExist

        Returns:
            list: ['table1','table2']
        """
        data_df : list = list()

        for each_sp in list_sps:
        
            query_dict : dict = {
                        'all_sps':all_queries.get(query_details).replace('replace_text',each_sp),
                        'dependent':all_queries.get(query_details).replace('replace_sp_name',each_sp)
                        }
            
            cursor.execute(query_dict.get(query_details))

            try:  
                data : str = str()
                for sql in cursor.fetchone():

                    data += sql
                
                drop_and_create_tables_list : set = set()
                target_tables_list : set  = set()
                for line_query in data.splitlines():
                    
                    split_line : list = line_query.replace(';','').split(' ')
                    file_table_list : list = list(filter(None, split_line))

                    query : str = ' '.join(query_date for query_date in file_table_list)
                    
                    
                    if (line_query.lower().find("truncate table") >= 0 or line_query.lower().find("drop table") >= 0 \
                        or line_query.lower().find("truncate  table") >= 0 or line_query.lower().find("drop  table") >= 0):
                        
                        drop_and_create_tables_list.add(file_table_list[-1])

                    if (line_query.lower().find("insert into") >= 0 or line_query.lower().find("insert  into") >= 0):
                    
                        target_tables_list.add(file_table_list[2])

                final_target_tables_list : set = target_tables_list.difference(drop_and_create_tables_list)
                
                for target_table in final_target_tables_list:
                    data_df.append((target_table,each_sp,"TARGET"))
                
                for drop_and_create_table in drop_and_create_tables_list:
                    data_df.append((drop_and_create_table,each_sp,"DROP AND CREATE"))
                
            except: 
                pass

        return data_df
    

class MergeData:

    def __init__(self) -> None:
        """_summary_
        """
        pass


    def merge_data_files(self,data_df : list,final_file_name : str) -> None:
        """the merge_data_file method used to merge all the SP's individual file data in one consolidated file

        Args:
            data_df (list): ['SP1','SP2']
            final_file_name (str): merge_file_name
        """
        BASE_DIR : Path = Path(__file__).resolve().parent.parent

        drop_and_create_file_path : str = os.path.join(BASE_DIR,'output/final_output/drop_and_create.csv')
        final_file_path : str = os.path.join(BASE_DIR,f'output/final_output/{final_file_name}.csv')
        merged_file_path : str = os.path.join(BASE_DIR,'output/merged_data.xlsx')

        column_names  : list[str]= ["table_name",'sp_name',"method"]

        df : pd.DataFrame = pd.DataFrame(data_df,columns=column_names)
    
        if os.path.exists(drop_and_create_file_path):
            os.remove(drop_and_create_file_path)
        if os.path.exists(final_file_path):
            os.remove(final_file_path)


        df.to_csv(drop_and_create_file_path,index=False)


        clickstream_df : pd.DataFrame = pd.read_excel(merged_file_path,sheet_name='merged_data')
        table_df : pd.DataFrame = pd.read_csv(drop_and_create_file_path)

        merge_df : pd.DataFrame = clickstream_df.merge(table_df,how="outer",on=["table_name","sp_name"])


        merge_df.drop_duplicates()
        merge_df["method"].fillna("SOURCE",inplace=True)
        merge_df.to_csv(final_file_path,index=False)