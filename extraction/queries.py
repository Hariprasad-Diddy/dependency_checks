import pandas as pd


all_queries = {
    'all_sps':"""
            select m.definition as query_txt,
                o.name
                FROM sys.sql_modules m
                INNER JOIN
                sys.objects o
                ON m.object_id = o.object_id
                where m.definition like '%replace_text%'
            """,
    'month_level' : """select query_txt
                        from adhoc_query.adhoc_query_log 
                        where start_time BETWEEN '2023-08-01' and '2023-08-02'""",

    'query_level_count':"""select distinct submitted_by,
                            query_txt,
                            count(query_txt)
                            from adhoc_query.adhoc_query_log aql
                            where start_time BETWEEN '{REPLACE_START_DATE}' and '{REPLACE_END_DATE}'
                            and end_time BETWEEN '{REPLACE_START_DATE}' and '{REPLACE_END_DATE}'
                            group by 1,2
                            """,
    
    'user_level_count':"""select a.query_txt,
                            a.submitted_by ,
                            count(query_txt) as qc,
                            COUNT(DISTINCT query_txt) as dqc
                            from adhoc_query.adhoc_query_log a
                            where a.submitted_by in ('sandeep.r@myntra.com',
                            'sreekanth.vempati@myntra.com',
                            'abhinav.ravi@myntra.com',
                            'vikram.garg@myntra.com',
                            'siddhartha.devapujula@myntra.com',
                            'sukhneer.singh@myntra.com',
                            'parmar.m@myntra.com',
                            'anil.kumar@myntra.com',
                            'sagnik.sarkar@myntra.com',
                            'gopinath.a@myntra.com',
                            'himani.bhutani@myntra.com',
                            'korah.malayil@myntra.com',
                            'rahul.mishra2@myntra.com',
                            'sahib.majithia@myntra.com',
                            'sangeet.jaiswal@myntra.com',
                            'saif.jawaid@myntra.com',
                            'dhruv.patel@myntra.com',
                            'rohit.gupta2@myntra.com',
                            'hrishikesh.ganu@myntra.com',
                            'ujjal.dutta@myntra.com',
                            'samyak.jain1@myntra.com',
                            'sandeep.narayan@myntra.com',
                            'tanya.soni@myntra.com',
                            'mohak.sukhwani@myntra.com',
                            'nidhi.kumari1@myntra.com',
                            'somesh.jaishwal@myntra.com',
                            'aayushi.das@myntra.com',
                            'sadbhavana.babar@myntra.com',
                            'shrey.pandey1@myntra.com',
                            'adnan.ali@myntra.com',
                            'suraj.yadwad@myntra.com',
                            'vamsi.chilamakurthi@myntra.com',
                            'diksha.kumari@myntra.com',
                            'aakriti.budhraja@myntra.com',
                            'satyajeet.singh@myntra.com',
                            'akhil.raj@myntra.com',
                            'siddhant.doshi@myntra.com',
                            'vijay.kumar3@myntra.com',
                            'mohit.chawla@myntra.com',
                            'soumalya.seal@myntra.com',
                            'vishi.jain@myntra.com',
                            'saikat.kumar@myntra.com',
                            'madhurima.mandal1@myntra.com',
                            'ishan.kumar1@myntra.com',
                            'glen.martindas@myntra.com',
                            'revenue-labs-ds@myntra.com'
                            ) and 
                            a.start_time BETWEEN '{REPLACE_START_DATE}' and '{REPLACE_END_DATE}'
                            and a.end_time BETWEEN '{REPLACE_START_DATE}' and '{REPLACE_END_DATE}'
                            group by 1,2"""
            
}



