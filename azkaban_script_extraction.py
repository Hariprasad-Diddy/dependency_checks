import os
import json
import ast
import pandas as pd
import requests as req
from bs4 import BeautifulSoup
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
file_name : str = 'Customer_Insights_Commerce_100_144.xlsx'
output_path : str= os.path.join(BASE_DIR,file_name)

# udp : list[dict]= [{'project_name':'DailyJobs','flow_name':'gift_card_sku_backup','project_id':4},
# {'project_name':'DailyJobs','flow_name':'fact_courier_serviceability_snapshot','project_id':4},
# {'project_name':'DailyJobs','flow_name':'wallet_transaction_log','project_id':4},
# {'project_name':'DailyJobs','flow_name':'fact_giftcard','project_id':4},
# {'project_name':'DailyJobs','flow_name':'index_rebuild_daily_commerce','project_id':4},
# {'project_name':'DailyJobs','flow_name':'index_rebuild_daily_clickstream','project_id':4},
# {'project_name':'DailyJobs','flow_name':'index_rebuild_weekly_clickstream','project_id':4},
# {'project_name':'DailyJobs','flow_name':'end_mfg_jobs','project_id':4},
# {'project_name':'DailyJobs','flow_name':'mk_customer_address_history_redshift','project_id':4},
# {'project_name':'DailyJobs','flow_name':'index_rebuild_weekly_commerce','project_id':4},
# {'project_name':'DailyJobs','flow_name':'fact_rejected_items','project_id':4}]

udp : list[dict] = [{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'OR_style_id_inventory_with_rev_Commerce'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'dailyrun_fact_category_mfp_metrics'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'Size_deviations_in_a_style_v3_bottomwear'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'sar_deal_performance_eors13'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'omni_scm_dashboard'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'Visibility_dash'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'sar_install_cohort_acq'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'health_snapshot'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'DE_Final_Table'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'End_PP'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'style_delist_returns'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'style_delist_performance'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'style_delist_brokenness'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'business_performance_metrics_007_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'business_performance_metrics_str_003_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'customer_perf_metrics_hq_003_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'customer_performance_metrics_apr_002_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'financial_metrics_001_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'inventory_health_metrics_absinv_oneeightydays_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'inventory_health_metrics_openinginv_001_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'inventory_health_metrics_doh_001_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'inventory_health_metrics_styleval_001_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'inventory_health_metrics_brokeness_001_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'operations_performance_metrics_pofr_002_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'operations_performance_metrics_spo_002_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'operations_performance_metrics_slotadherence_001_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'operations_performance_metrics_rtv_004_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'ish_cps_at'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'customer_performance_metrics_amm_001_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'ish_cps_brand'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'operations_performance_metrics_irr_001_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'ish_cps_bu'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'operations_performance_metrics_shortageperc_002_azk'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'ish_id_tier'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'mss_vs_garment_measurement'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'PC_Top_Selling_Styles_Final_Table'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'PC_Top_Selling_Styles_Last_Month_Base'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'Whitesea_STR_Views'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'ss_key_size_brokeness_job'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'SP_SS_PSS_table_WL_RT_ATC'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'scm_metrics_gh'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'HS_Daily_Email'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'Returns_Daily_View_Refresh'},
{'project_name': 'Customer_Insights_Commerce', 'project_id': 5, 'flow_name':'HS_Emailer'}]
url : str = "http://azkaban.myntra.com"
username : str = 'azkaban'
password : str = 'Azkaban@435*'
data : dict = {"action":"login","username":f"{username}","password":"{password}"}


with req.session() as s:
    
    HEADER : dict = {'Accept':"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
              "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
              "Referer":"http://azkaban.myntra.com/manager?project=Test",
        'Cookie':"JSESSIONID=126q138rauy5e99xhb7bcrl4c; azkaban.failure.message=; azkaban.warn.message=; azkaban.success.message=; azkaban.browser.session.id=06f70165-a5eb-4f79-b849-e3204c2f8fd4"
        }
    
    empty_dataframe_list : list = []
    for each_flow in udp:
        
        
        # API to fetch sub jobs details
        project_and_flow_details = req.get(f"http://azkaban.myntra.com/manager?session.id=9f47d646-ac4c-4995-82e0-1c70cab16358&ajax=fetchflowgraph&project={each_flow['project_name']}&flow={each_flow['flow_name']}",headers=HEADER)
        # print(project_and_flow_details.text)
        try:
            project_and_flow_details_nodes : dict = ast.literal_eval(str(project_and_flow_details.text))
        except:
            project_and_flow_details_nodes : dict = json.loads(str(project_and_flow_details.text))

        # API to fetch schedule jobs or not
        scheduled_jobs = req.get(f"http://azkaban.myntra.com/schedule?session.id=9f47d646-ac4c-4995-82e0-1c70cab16358&ajax=fetchSchedule&projectId={each_flow['project_id']}&flowId={each_flow['flow_name']}",headers=HEADER)
        
        try:
            scheduled_jobs_nodes : dict = ast.literal_eval(str(scheduled_jobs.text))
        except:
            scheduled_jobs_nodes : dict = json.loads(str(scheduled_jobs.text))

        if scheduled_jobs_nodes.__len__() != 0 or project_and_flow_details_nodes.__len__ != 0:
            all_sub_jobs : list = []
            try:    
                for node in project_and_flow_details_nodes['nodes']:
                    if 'in' in node:
                        all_sub_jobs += node['in']
                        
                    else:
                        all_sub_jobs += [node['id']]
                        
            except:
                all_sub_jobs += [each_flow['flow_name']]
                
            for job in all_sub_jobs:
                b = req.get(f"http://azkaban.myntra.com/manager?project={each_flow['project_name']}&flow={each_flow['flow_name']}&job={job}",allow_redirects=True,headers=HEADER)
                soup = BeautifulSoup(b.content, 'html5lib') # If this line causes an error, run 'pip install html5lib' or install html5lib
                
                for script_file in soup.find_all('td'):
                    
                    file = str(script_file)

                    if file.find(".sh") > 0:
                        empty_dataframe_list.append((each_flow['project_name'],each_flow['project_id'],each_flow['flow_name'],file.replace("<td>","").replace("</td>","")))
                    if file.find("$") > 0:
                        empty_dataframe_list.append((each_flow['project_name'],each_flow['project_id'],each_flow['flow_name'],file.replace("<td>","").replace("</td>","")))
        print("Flow name : ",each_flow['flow_name'])
    df = pd.DataFrame(empty_dataframe_list,columns=['project_name','project_id','flow_name','script']) 
    df.drop_duplicates(inplace=True)           
    df.to_excel(output_path,index=False)
                