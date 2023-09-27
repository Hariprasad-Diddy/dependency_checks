import pandas as pd
from pathlib import Path
import os
import xlsxwriter

class OutputPath:
    """the OutputPath class create a output file
    """
    BASE_DIR : Path = Path(__file__).resolve().parent.parent
    output_folder : str = os.path.join(BASE_DIR,'output')

    def __init__(self,output_file_name : str) -> None:
        """constructor method instantiate the class and create a output file

        Args:
            output_file_name (str): output_file.csv
        """
        self.output_file_name : str = os.path.join(self.output_folder,output_file_name)
        
class FileExist(OutputPath):
    """_summary_

    Args:
        OutputPath (_type_): _description_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ...
    
    def create_or_replace_file(self,output_file_name : str) -> None:
        """_summary_

        Args:
            output_file_name (str): _description_
        """
        super().__init__(output_file_name)

        if os.path.exists(self.output_file_name):
            os.remove(self.output_file_name)
            workbook : xlsxwriter = xlsxwriter.Workbook(f'{self.output_file_name}.xlsx')
            workbook.close()
        else:
            workbook : xlsxwriter = xlsxwriter.Workbook(f'{self.output_file_name}.xlsx')
            workbook.close()

class Excel(OutputPath):
    """_summary_

    Args:
        OutputPath (_type_): _description_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ...
    
    def save(self,dataframe,output_file_name,sheet_name) -> None:
        """_summary_

        Args:
            dataframe (_type_): _description_
            output_file_name (_type_): _description_
            sheet_name (_type_): _description_
        """
        super().__init__(output_file_name)
        with pd.ExcelWriter(f'{self.output_file_name}.xlsx',mode='a',engine='openpyxl') as my_excel_obj:
            dataframe.to_excel(my_excel_obj,sheet_name=sheet_name,index=False)


class CSV(OutputPath):
    """_summary_

    Args:
        OutputPath (_type_): _description_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ...

    def save(self,dataframe,file_name : str) -> None:
        """_summary_

        Args:
            dataframe (_type_): _description_
            file_name (str): _description_
        """
        dataframe.to_csv(f'{self.config_folder}/aadw_query_may.csv')


class Json(OutputPath):
    """_summary_

    Args:
        OutputPath (_type_): _description_
    """
    def __init__(self, output_file_name: str) -> None:
        """_summary_

        Args:
            output_file_name (str): _description_
        """
        ...

    def save(self,dataframe,output_path,file_name) -> None:
        """_summary_

        Args:
            dataframe (_type_): _description_
            output_path (_type_): _description_
            file_name (_type_): _description_
        """
        dataframe.to_csv(f'{self.config_folder}/aadw_query_may.json')


class PostgresDB:
    """_summary_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ... 

    def save(self,dataframe : pd.DataFrame ,output_path : str) -> None:
        """_summary_

        Args:
            dataframe (pd.DataFrame): _description_
            output_path (str): _description_
        """
        print("save in the PostgresDB Database")

class SQLServerDB:
    """_summary_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ...

    def save(self,dataframe : pd.DataFrame,output_path : str) -> None:
        """_summary_

        Args:
            dataframe (pd.DataFrame): _description_
            output_path (str): _description_
        """
        print("save in the SQLServerDB Database")


class MySqlDB:
    """_summary_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ...

    def save(self,dataframe : pd.DataFrame ,output_path : str) -> None:
        """_summary_

        Args:
            dataframe (pd.DataFrame): _description_
            output_path (str): _description_
        """
        print("save in the MySqlDB Database")



class Blob:
    """_summary_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ... 

    def save(self,dataframe : pd.DataFrame ,output_path : str) -> None:
        """_summary_

        Args:
            dataframe (pd.DataFrame): _description_
            output_path (str): _description_
        """
        print("save in the Blob")


class S3:
    """_summary_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ...

    def save(self,dataframe : pd.DataFrame ,output_path : str) -> None:
        """_summary_

        Args:
            dataframe (pd.DataFrame): _description_
            output_path (str): _description_
        """
        print("save in the S3")


class GCP:
    """_summary_
    """
    def __init__(self) -> None:
        """_summary_
        """
        ...

    def save(self,dataframe : pd.DataFrame,output_path : str) -> None:
        """_summary_

        Args:
            dataframe (pd.DataFrame): _description_
            output_path (str): _description_
        """
        print("save in the GCP")

