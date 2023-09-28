import configparser
from pathlib import Path
import os

class ConfigInformation:
    """ ConfigInformation class read the configuration details from the config.ini file and instantiate the credentials 
    """
    def __init__(self,folder_name : str,file_name : str) -> None:
        """ constructor method accept two parameter and instantiate the credentials

        Args:
            folder_name (str): output
            file_name (str): test.csv
        """
        self.BASE_DIR : Path = Path(__file__).resolve().parent.parent
        self.config_folder : str = os.path.join(self.BASE_DIR,folder_name)
        self.config_file_path : str = os.path.join(self.config_folder,file_name)
        
        self.config  : configparser = configparser.ConfigParser()
        self.config.read(self.config_file_path)
        


    def config_details(self,config : str ,platform : str) -> list:
        """_summary_

        Args:
            config (str): _description_
            platform (str): _description_

        Returns:
            list: _description_
        """
        return config[platform]
    
