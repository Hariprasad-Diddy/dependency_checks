import pandas as pd
from pathlib import Path
import os
import xlsxwriter

class OutputPath:
    BASE_DIR = Path(__file__).resolve().parent.parent
    output_folder = os.path.join(BASE_DIR,'output')

    def __init__(self,output_file_name):
        
        self.output_file_name = os.path.join(self.output_folder,output_file_name)
        

class FileExist(OutputPath):

    def __init__(self):
        ...
    
    def check_path_exists(self,output_file_name):
        super().__init__(output_file_name)

        if os.path.exists(self.output_file_name):
            pass
        else:
            workbook = xlsxwriter.Workbook(f'{self.output_file_name}.xlsx')
            workbook.close()

class Excel(OutputPath):

    def __init__(self):
        ...
    
    def save(self,dataframe,output_file_name,sheet_name):
        super().__init__(output_file_name)
        with pd.ExcelWriter(f'{self.output_file_name}.xlsx',mode='a',engine='openpyxl') as my_excel_obj:
            dataframe.to_excel(my_excel_obj,sheet_name=sheet_name,index=False)


class CSV(OutputPath):
    def save(self,dataframe,file_name):
        dataframe.to_csv(f'{self.config_folder}/aadw_query_may.csv')


class Json(OutputPath):
    def save(self,dataframe,output_path,file_name):
        dataframe.to_csv(f'{self.config_folder}/aadw_query_may.json')


class PostgresDB:
    def save(self,dataframe,output_path):
        print("save in the PostgresDB Database")

class SQLServerDB:

    def save(self,dataframe,output_path):
        print("save in the SQLServerDB Database")


class MySqlDB:

    def save(self,dataframe,output_path):
        print("save in the MySqlDB Database")



class Blob:

    def save(self,dataframe,output_path):
        print("save in the Blob")


class S3:

    def save(self,dataframe,output_path):
        print("save in the S3")


class GCP:

    def save(self,dataframe,output_path):
        print("save in the GCP")

