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
    final_file_name = input('Enter the output file name : ')
    query_details = input('Enter the data required based on date level / user_level / all_sps : ')
    # date_range = input('Enter how month many data required to extract : ')

    list_sps = ['sp_brand_track_report',
'sp_cart_filler_queries',
'sp_conversion_by_state',
'bifrost_data_load.py',
'sp_ClickstreamAggregates',
'bifrost_data_load.py',
'sp_covid_servicable_pincode',
'sp_cross_sell_queries_v2',
'ctr_ws',
'sp_Customer_Premiumness_incremental',
'sp_daily_aggregates_lv',
'sp_Daily_Aggregates_Web',
'sp_daily_user_sis_aggregate',
'sp_DG_Myntra_Insider_1',
'sp_DG_Myntra_Insider_2',
'sp_DG_Myntra_Insider_3',
'sp_DG_Myntra_Insider_4',
'sp_DG_Myntra_Insider_5',
'sp_DG_Myntra_Insider_6',
'sp_DG_Myntra_Insider_7',
'sp_DG_Myntra_Insider_8',
'sp_DG_Myntra_Insider_9',
'sp_DG_Myntra_Insider_10',
'sp_EORS_Style_Hourly_Input_Metrics',
'sp_First_Installs',
'sp_Funnel_UDP_Metrics_SIP',
'sp_ga_ma_session_metric',
'sp_GH_user_type',
'sp_Indirect_Revenue_Attribution_Monetisation',
'sp_insider_dashboard_1',
'sp_insider_dashboard_2',
'sp_Install_Funnel_Dashboard',
'sp_install_uninstall_events',
'sp_Insider_Masterclass_events_2',
'sp_mktg_rnf_platform_metrics',
'sp_move_month',
'sp_move_week',
'sp_mo_engagement_metrics',
'sp_mo_install_source',
'sp_mo_okr_rev_metrics',
'sp_mo_yoy_growth',
'sp_mynmall_obs_traffic',
'SP_Myntra_base_persona_1',
'SP_Myntra_base_persona_2',
'SP_Myntra_base_persona_3',
'my_orders_sk',
'sp_OKR_LTV_Churn_Conversion',
'sp_pc_dashboard_search',
'sp_PDPBase',
'sp_PDPUsageMetrics',
'sp_PDP_Delivery_info',
'sp_PLA',
'sp_product_carousel_urls',
'sp_Qoa_daily_updation',
'sp_qos_revised',
'sp_QoT_Daily_Dashboard',
'sp_qr_code',
'reco_dashboard_queries',
'sp_RR_dashboard_monthly_cohort',
'sp_RR_dashboard_daily_metric',
'sp_SAHA_Base_Daily_Load',
'sp_SAHA_Insert_DML',
'sp_SAHA_Day_Level_Agg',
'sp_SAHA_Insert_DML',
'sp_SAHA_Product_Level_Agg',
'sp_sbi_data',
'sp_Search_Keywords_Funnel',
'sp_SF42_final_query',
'sp_ShopX_Daily_Load',
'sp_ShopX_Update_Returns',
'sp_signup_cards_performance',
'sp_size_and_fit_dashboard_commerce_upo',
'size_and_fit_dashboard_clickstream_job',
'sp_category_experience_dashboard',
'sp_mno_events',
'sp_SS_EORS12_platform_metrics_daily',
'sp_ugc_dashboard',
'sp_studio_behaviour_by_source',
'sp_studio_content_performance',
'sp_studio_user_author_follows',
'sp_studio_user_topic_follows',
'sp_tbs_campaign_data',
'sp_traffic_aggregates_daily',
'sp_Unique_Customer_Views_Style_Health',
'sp_customer_tier_mapping',
'sp_user_locations',
'sp_User_Notification_Detailed',
'sp_User_Search_Keywords_Funnel'
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
        file_exists = FileExist().create_or_replace_file(output_file_name)
        query_dict = {
                    'all_sps':all_queries.get(query_details).replace('replace_text',each_sp),
                    'dependent':all_queries.get('dependent').replace('replace_sp_name',each_sp)
                    }
        
        table_extraction(query_dict[query_details],connection,output_format_dict.get(output_format),output_file_name,each_sp)
        print(each_sp)

    file_exists = FileExist().create_or_replace_file('merged_data')
    merge_files('output',output_format_dict.get(output_format),'merged_data')  
    
    

    
    data_df : list = list()
    for each_sp in list_sps:
        print(each_sp,"each_spsss")
        query_dict = {
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
                    
                    # split_line = line_query.replace(';','').split(' ')
                    # file_table_list = list(filter(None, split_line))
                    
                
                    drop_and_create_tables_list.add(file_table_list[-1])

                if (line_query.lower().find("insert into") >= 0 or line_query.lower().find("insert  into") >= 0):
                    # split_line = line_query.replace(';','').split(' ')
                    # file_table_list = list(filter(None, split_line))
                 
                    
                    target_tables_list.add(file_table_list[2])
                    # print(target_tables)
            final_target_tables_list : set = target_tables_list.difference(drop_and_create_tables_list)
            
            for target_table in final_target_tables_list:
                data_df.append((target_table,each_sp,"TARGET"))
            
            for drop_and_create_table in drop_and_create_tables_list:
                data_df.append((drop_and_create_table,each_sp,"DROP AND CREATE"))
            
        except: 
            pass
    
    df = pd.DataFrame(data_df,columns=["table_name",'sp_name',"method"])
    
    if os.path.exists('/Users/hari.prasad.vc/Desktop/drop_and_create.csv'):
        os.remove('/Users/hari.prasad.vc/Desktop/drop_and_create.csv')
    if os.path.exists(f'/Users/hari.prasad.vc/Desktop/{final_file_name}.csv'):
        os.remove(f'/Users/hari.prasad.vc/Desktop/{final_file_name}.csv')


    df.to_csv('/Users/hari.prasad.vc/Desktop/drop_and_create.csv',index=False)


    import pandas as pd

    clickstream_df = pd.read_excel('/Users/hari.prasad.vc/Documents/myntra_poc/dependencies_check/output/merged_data.xlsx',sheet_name='merged_data')
    table_df = pd.read_csv('/Users/hari.prasad.vc/Desktop/drop_and_create.csv')

    merge_df = clickstream_df.merge(table_df,how="outer",on=["table_name","sp_name"])

    # file_exists = FileExist().create_or_replace_file(final_file_name)
    merge_df.drop_duplicates()
    merge_df["method"].fillna("SOURCE",inplace=True)
    merge_df.to_csv(f'/Users/hari.prasad.vc/Desktop/{final_file_name}.csv',index=False)