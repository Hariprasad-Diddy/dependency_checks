import configparser
from pathlib import Path
import os

class ConfigInformation:

    def __init__(self,folder_name,file_name):
        self.BASE_DIR = Path(__file__).resolve().parent.parent
        self.config_folder = os.path.join(self.BASE_DIR,folder_name)
        self.config_file_path = os.path.join(self.config_folder,file_name)
        
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file_path)
        


    def config_details(self,config,platform):
        return config[platform]
    
