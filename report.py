import pandas as pd
import pymysql
import json
import os
from dotenv import load_dotenv
load_dotenv()

def evaluate_expression(expression, variables):
    allowed_globals = {'__builtins__': None}
    allowed_globals.update(variables)
    return int(eval(expression, allowed_globals))

user = os.environ.get("RUSER")
password = os.environ.get("RPASS")
host = os.environ.get("RHOST")
db = os.environ.get("RDB")

cnx = pymysql.connect(
    user = user,
    password = password,
    host = host,
    db = db
)

## Daily consumption data
raw_query = f'''select 
                    f.ID ,
                    dc.feeder,
                    dc.ip,
                    dc.slave_id,
                    dc.KWH,
                    dc.mannual,
                    dc.RunHr,
                    dc.date
                from 
                    Feeders f 
                left join 
                    daily_consumption dc 
                on 
                    f.IP = dc.IP 
                and 
                    f.slave_id = dc.slave_id '''

raw_data = pd.read_sql(raw_query,con=cnx)
raw_df = raw_data.copy()

raw_df['mannual'] = raw_df['mannual'].fillna(0)
raw_df['mannual'] = raw_df['mannual'].replace({'5000':0})
raw_df[['KWH', 'mannual']] = raw_df[['KWH', 'mannual']].astype(int)
raw_df['total'] = raw_df['KWH']+raw_df['mannual']

## Mapping data
mapp_query = f'''select 
                    id,
                    name,
                    formula ,
                    mapping_response 
                from 
                    mapping_data md '''

feeder_df = pd.read_sql(mapp_query,con=cnx)


## Function for generating the value for a report based on report_id and date

def ValueGenerator(raw_data,mapping_df,req_date,report_id):
    raw_df = raw_data[raw_data['date'].str.contains(f"{req_date}")].copy()
    # print(raw_df.shape)
    req_df = mapping_df[mapping_df['id']==int(f"{report_id}")].copy()
    req_df = req_df.reset_index(drop= True)
    mapp_response_list = mapping_df.loc[mapping_df['id']==int(f"{report_id}"), 'mapping_response'].values[0]
    mapp_list = json.loads(mapp_response_list)
    values_dict={}
    feeder_dict={}
    for each in mapp_list:
        var_name = each['var']
        req_id = each['id']
        run_hr = each['run_hour']
        # print(run_hr)
        feeder_level = raw_df.loc[raw_df['ID']==req_id, 'feeder'].values[0]
        feeder_dict[var_name]=feeder_level
        if run_hr==True:
            run_hr_value = (raw_df.loc[raw_df['ID']==req_id, 'RunHr'].values[0])
            values_dict[var_name]=int(run_hr_value)
        else:
            kwh_value = raw_df.loc[raw_df['ID']==req_id, 'total'].values[0]
            values_dict[var_name]=kwh_value 
    
    req_df['feeder_levels'] = [feeder_dict]
    req_df['feeder_values'] = [values_dict]
    req_df['date'] = f"{req_date}"
    req_df['value'] = req_df.apply(lambda row: evaluate_expression(row['formula'], row['feeder_values']), axis=1)
    
    return req_df


value_df = ValueGenerator(raw_df,feeder_df,'2023-06-29',5) ## value report for report_id=5 and date='2023-06-29'
