import re
import pandas as pd

find_string : str = '-Q "exec'
sql_file_string : str = ".sql"
ktr_string : str = "param:table_name"
re_pattern = []

shell_script_path_list = ['/Users/hari.prasad.vc/projects/dwh-repo/MyntraDW/azkaban/redshift_job_scripts/fact_product_profile_extended.sh',
'/Users/hari.prasad.vc/projects/dwh-repo/MyntraDW/azkaban/redshift_job_scripts/cms_bin.sh',
'/Users/hari.prasad.vc/projects/customer-insights/azure_migration/azkaban/scripts_clickstream/Myntra_base_persona.sh',
'/Users/hari.prasad.vc/projects/dwh-repo/MyntraDW/azkaban/redshift_job_scripts/fact_product_profile_extended.sh']


empty_data_frame_list : list = []
for path in shell_script_path_list:

    
    with open(path) as sh_file:    
        file_name = path.split('/')[-1]
        for each_line in sh_file.readlines():
            
            if each_line.find(find_string) > 0:
                data = each_line[each_line.find(find_string):]
                empty_data_frame_list.append((file_name,data.replace("\n","").replace('"',"").replace("-Q","")))
                
            
            if each_line.find(sql_file_string) > 0:
                data = each_line.split('/')[-1].replace("=","").replace("\n","")
                empty_data_frame_list.append((file_name,data))
                

            if each_line.find(ktr_string) > 0:
                
                split_list = each_line.split(ktr_string)[1]
                data = 'sp_' + split_list.split('-param')[0].replace("=","").strip()
                empty_data_frame_list.append((file_name,data))
                


df = pd.DataFrame(empty_data_frame_list,columns=['script_name','sp_name'])
print(df)

