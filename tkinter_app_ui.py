import tkinter as tk
from tkinter import filedialog, messagebox, ttk

# for data processing
import pandas as pd
import numpy as np
import glob 
import os 
import datetime
from datetime import date, datetime, timedelta

import warnings
warnings.filterwarnings("ignore")

from google.cloud import bigquery
from google.oauth2 import service_account

# for app handling  
import sys
from pythonnet import set_runtime
set_runtime("netfx")
from os import environ
from pathlib import Path

runtime = Path.cwd() / "python310.dll"

if runtime.exists():
    environ["PYTHONNET_PYDLL"] = str(runtime.resolve())
    environ["BASE_DIR"] = str(Path.cwd().resolve())

# set up api call bigquery
def key_path_transform(path):
    if getattr(sys, 'frozen', False):
        key_path_transform = os.path.join(sys._MEIPASS, path)
    else:
        key_path_transform = path
    return key_path_transform

key_path = key_path_transform("api_key.json")

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

def etl_collect_data(client):

    # query from Bigquery
    query_baseline = '''
        SELECT  
          *
        FROM `mch-dwh-409503.MCH_Output.CONSUMPTION_RATE_SEASONALITY_by_PROVINCE_DPNAME`
    '''
    df_baseline = client.query(query_baseline).to_dataframe()

    query_master_calendar = '''
        SELECT  
          *
        FROM `mch-dwh-409503.MCH_Output.MASTER_CALENDAR_CONTRIBUTION`
    '''
    df_master_calendar = client.query(query_master_calendar).to_dataframe()

    # conditions to filter last 3 months df_groupSKU_master
    latestYear = df_baseline['YEAR'].max()
    latestMonth = df_baseline[df_baseline['YEAR'] == df_baseline['YEAR'].max()]['MONTH'].max()
    if latestMonth >= 3:
        fromPeriod = latestYear*100 + (latestMonth-3)
    else: 
        fromPeriod = (latestYear-1)*100 + (latestMonth-3+12)

    query_groupSKU_master = f'''
        SELECT  
          *
        FROM `mch-dwh-409503.MCH_Output.GROUP_SKU_MASTER_by_REGION_NAME_DPNAME`
        WHERE 
            YEAR*100 + MONTH > {fromPeriod}
    '''
    df_groupSKU_master = client.query(query_groupSKU_master).to_dataframe()
    
    query_groupSKU_byProvince = f'''
        SELECT  
          *
        FROM `mch-dwh-409503.MCH_Output.SO_AGG_by_PROVINCE_DPNAME`
        WHERE 
            YEAR*100 + MONTH > {fromPeriod}
    '''
    df_groupSKU_byProvince = client.query(query_groupSKU_byProvince).to_dataframe()
    
    currentYear = datetime.now().year   
    currentMonth = datetime.now().month
    
    collect_fromPeriod = (currentYear - 1)*100 + currentMonth
    
    if currentMonth == 1: 
        forecast_fromPeriod = (currentYear - 1)*100 + 12 
        forecast_toPeriod = (currentYear + 1)*100 + 1
    else: 
        forecast_fromPeriod = currentYear*100 + (currentMonth - 1)
        forecast_toPeriod = (currentYear + 1)*100 + currentMonth

    query_master_date = f'''
        SELECT
          DATE,
          DAY,
          MONTH,
          YEAR,
          ISO_WEEKNUM, 
          DATE_TRUNC(DATE, WEEK(MONDAY)) AS PERIOD_TAG,
          WORKING_DAY_SALE_IN,
          WORKING_DAY_DC,
          HOLIDAY,
          LUNAR_DAY,
          LUNAR_MONTH, 
          LUNAR_YEAR
        FROM `mch-dwh-409503.MCH_DP.MASTER_CALENDAR`
        WHERE 
          YEAR*100 + MONTH >= {collect_fromPeriod}
          AND YEAR*100 + MONTH <= {forecast_toPeriod} 
    '''
    df_master_date = client.query(query_master_date).to_dataframe()

    df_forecast_week = df_master_date[
        df_master_date['YEAR'] * 100 + df_master_date['MONTH'] >= forecast_fromPeriod
    ][['YEAR', 'MONTH', 'ISO_WEEKNUM']].drop_duplicates().assign(
        Week = df_master_date['YEAR'] * 100 + df_master_date['ISO_WEEKNUM'] 
    )
    
    df_week = df_master_date[['YEAR', 'MONTH', 'ISO_WEEKNUM']].drop_duplicates().assign(
        Week = df_master_date['YEAR'] * 100 + df_master_date['ISO_WEEKNUM'] 
    )
    
    # convert datatype of df_forecast_week
    df_forecast_week['Week'] = df_forecast_week['Week'].astype('float') 
    df_week['Week'] = df_week['Week'].astype('float') 
    
    query_RR_by_province_DPName = '''
        SELECT  
          * 
        FROM `mch-dwh-409503.MCH_DP.RR_by_PROVINCE_DPNAME`
    '''
    df_RR_by_PROVINCE_DPNAME = client.query(query_RR_by_province_DPName).to_dataframe()
    
#     # conditions to filter last 3 months df_groupSKU_master
#     startForecastYear = df_master_date['YEAR'].min()
#     startForecastDate = df_master_date[df_master_date['YEAR'] == startForecastYear]['DATE'].min()

#     startForecastWeek = df_master_date[df_master_date['DATE'] == startForecastDate]['ISO_WEEKNUM'].min()
#     filter_so_fromDate = ( date.fromisocalendar(startForecastYear, startForecastWeek, 1) + timedelta(days=-35) ).strftime('%Y-%m-%d')
#     filter_so_toDate = date.fromisocalendar(startForecastYear, startForecastWeek, 1).strftime('%Y-%m-%d')

    query_SO_weekly_last_5w = f'''
        SELECT
            *
        FROM `mch-dwh-409503.MCH_DP.SO_WEEKLY_AGG_by_PROVINCE_DPNAME`
        WHERE PERIOD_TAG IN (
          SELECT DISTINCT 
            PERIOD_TAG
          FROM `mch-dwh-409503.MCH_DP.SO_WEEKLY_AGG_by_PROVINCE_DPNAME`
          ORDER BY 1 DESC
          LIMIT 5
        )
    '''

    df_SO_weekly_last_5w = client.query(query_SO_weekly_last_5w).to_dataframe()
    
    query_stock_weekly_last_5w = f'''
        SELECT
            *
        FROM `mch-dwh-409503.MCH_DP.STOCK_WEEKLY_by_DC_REGION_NAME_DPNAME`
        WHERE PERIOD_TAG IN (
          SELECT DISTINCT 
            PERIOD_TAG
          FROM `mch-dwh-409503.MCH_DP.STOCK_WEEKLY_by_DC_REGION_NAME_DPNAME`
          ORDER BY 1 DESC
          LIMIT 5
        )
    '''

    df_stock_weekly_last_5w = client.query(query_stock_weekly_last_5w).to_dataframe()
    
    query_stock_monthly_last_2m = f'''
        SELECT
            *
        FROM `mch-dwh-409503.MCH_DP.STOCK_MONTHLY_by_DC_REGION_NAME_DPNAME`
        WHERE PERIOD_TAG IN (
          SELECT DISTINCT 
            PERIOD_TAG
          FROM `mch-dwh-409503.MCH_DP.STOCK_MONTHLY_by_DC_REGION_NAME_DPNAME`
          ORDER BY 1 DESC
          LIMIT 2
        )
    '''

    df_stock_monthly_last_2m = client.query(query_stock_monthly_last_2m).to_dataframe()
    
    query_past_innovation = '''
        SELECT 
          SUB_DIVISION_NAME,
          GROUP_SKU,
          DEMAND_PLANNING_STANDARD_SKU_NAME,
          REGION_NAME,
          PROVINCE,
          CHANNEL,
          YEAR, 
          MONTH, 
          SO_VALUE,
          SO_QTY
        FROM `mch-dwh-409503.MCH_Output.BASELINE_by_PROVINCE_DPNAME` t1 
        INNER JOIN ( 
          SELECT DISTINCT
            SUB_DIVISION_NAME,
            GROUP_SKU,
            DEMAND_PLANNING_STANDARD_SKU_NAME,
            REGION_NAME,
            PROVINCE,
            CHANNEL, 
            COUNT(DISTINCT MONTH) AS CNT_MONTHS
          FROM `mch-dwh-409503.MCH_Output.BASELINE_by_PROVINCE_DPNAME` 
          GROUP BY 1,2,3,4,5,6
          HAVING CNT_MONTHS < 12
        ) t2 
        USING (
          SUB_DIVISION_NAME,
            GROUP_SKU,
            DEMAND_PLANNING_STANDARD_SKU_NAME,
            REGION_NAME,
            PROVINCE,
            CHANNEL
        )
    '''
    df_past_innovation = client.query(query_past_innovation).to_dataframe()
    
    return df_baseline, df_master_calendar, df_groupSKU_master, df_groupSKU_byProvince, df_master_date, df_forecast_week, df_week, df_RR_by_PROVINCE_DPNAME, df_SO_weekly_last_5w, df_stock_weekly_last_5w, df_stock_monthly_last_2m, df_past_innovation
def etl_clean_transform_muf_input(df_muf_input):
    df_muf_input = df_muf_input.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    df_muf_input = df_muf_input[~ df_muf_input['Building Blocks'].isin(['Baseline', 'Seasonality', 'Baseline - Adjustment']) ]

    # unpivot 
    df_muf_input_melted = pd.melt(df_muf_input, 
                                  id_vars=['Group SKU', 'Sub Division', 'Site', 'Building Blocks', 'Year', 
                                           'Channel', 'Uplift type', 'Region',  'Risk %'], 
                                  value_vars=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
                                 ).rename(columns = {'variable': 'Month'})

    df_muf_input_melted['value'] = pd.to_numeric(df_muf_input_melted['value'], errors='coerce').fillna(0)
    df_muf_input_melted['Risk %'] = pd.to_numeric(df_muf_input_melted['Risk %'], errors='coerce').fillna(0)

    return df_muf_input_melted

def etl_clean_transform_contribution_input(df_contribution, df_region_contribution):
    
    def etl_clean_input_data(df):
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        return df
    
    # loop through all input files to clean
    for df in [df_contribution, df_region_contribution]: 
        df = etl_clean_input_data(df)
    
    # unpivot
    df_contribution_melted = pd.melt(df_contribution, 
                                     id_vars=['Demand Planning Standard SKU Name', 'Group SKU', 'Channel', 'Sub Division Name', 'Duplication'], 
                                     value_vars=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
                                    ).rename(columns = {'variable': 'Month', 'value': 'Contribution'})


    df_region_contribution_melted = pd.melt(df_region_contribution, 
                                            id_vars=['DP Name', 'Region Name', 'Channel Code'], 
                                            value_vars=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
                                           ).rename(columns = {'variable': 'Month', 'value': 'Contribution'})
    
    return df_contribution_melted, df_region_contribution_melted

def etl_baseline(df_baseline, df_master_calendar):
    dimensions = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME', 'PROVINCE', 'CHANNEL']

    df_baseline_forecast = df_baseline[
        dimensions + ['ROLLING_12M_DAILY_CONSUMPTION_RATE']
    ].assign(key = 0).rename(columns = {
        'ROLLING_12M_DAILY_CONSUMPTION_RATE': 'DAILY_CONSUMPTION_RATE' 
    }).merge(df_master_calendar[
        ( df_master_calendar['YEAR']*100 + df_master_calendar['MONTH'] >= (datetime.now().year - 1)*100 + datetime.now().month ) 
        & ( df_master_calendar['YEAR']*100 + df_master_calendar['MONTH'] < (datetime.now().year + 1)*100 + datetime.now().month )
            ].assign(key=0).rename(columns= {
                'YEAR': 'FORECAST_YEAR',
                'MONTH': 'FORECAST_MONTH'
            }), 
             on='key', how='outer')

    # baseline consumption
    df_baseline_forecast['BASELINE'] = df_baseline_forecast['DAILY_CONSUMPTION_RATE'] * df_baseline_forecast['CALENDAR_DAYS'] / 1000
    # baseline adjusted (gap between SO DAY contribution VS CALENDAR DAY contribution)
    df_baseline_forecast['BASELINE_ADJ'] = (df_baseline_forecast['BASELINE'] * df_baseline_forecast['SO_DAYS_CONTRIBUTION'] /  df_baseline_forecast['CALENDAR_DAYS_CONTRIBUTION'] - df_baseline_forecast['BASELINE'])

    df_baseline_forecast = df_baseline_forecast[dimensions + ['DAILY_CONSUMPTION_RATE', 'FORECAST_YEAR', 'FORECAST_MONTH', 'BASELINE', 'BASELINE_ADJ']]
    
    df_baseline_forecast[['FORECAST_YEAR', 'FORECAST_MONTH']] = df_baseline_forecast[['FORECAST_YEAR', 'FORECAST_MONTH']].astype('float')
    
    # baseline, baseline adj, seasonality
    df_baseline_forecast = pd.merge(
        df_baseline_forecast,
        df_baseline[dimensions + ['MONTH', 'ROLLING_SEASONALITY_RATE']].rename(columns = {'ROLLING_SEASONALITY_RATE': 'SEASONALITY'}),
        how = 'left',
        left_on = dimensions + ['FORECAST_MONTH'],
        right_on = dimensions + ['MONTH']
    )
      
    return df_baseline_forecast

# MANUAL GROUP SKU 
def etl_manual_groupSKU(df_contribution_melted, df_region_contribution_melted, df_groupSKU_master):
    def etl_regionContribution_groupSKU(df_groupSKU_master):
        df_groupSKU_master = df_groupSKU_master.rename(columns = {
            'SUB_DIVISION_NAME': 'Sub Division Name', 
            'GROUP_SKU': 'Group SKU', 
            'REGION_NAME': 'Region Name',
            'CHANNEL': 'Channel',
        })

        # filter channel / region / SO_value
        df_groupSKU_master = df_groupSKU_master[ 
            df_groupSKU_master['Channel'].isin([
                'GT0', 'MT0-WCM', 'MT0-non WCM', 'GT0_C1', 'KA0', 'NETCO_C1', 'NETCO_HRC'
            ])
            & (~ df_groupSKU_master['Region Name'].isin(['Others']) )
            & (df_groupSKU_master['SO_VALUE'] != 0)
        ]

        dimensions = ['Group SKU', 'Region Name', 'Channel']
        dimensions_total = list(set(dimensions) - set(['Region Name']))
        measures = ['SO_QTY']
        rename_measures = ['volume_SO']
        rename_total_measures = ['total_' + i for i in rename_measures]
        rename_measure_dictionary = dict(zip(measures, rename_measures))
        rename_total_measure_dictionary = dict(zip(rename_measures, rename_total_measures))
        # Region_Group SKU
        df_region_groupSKU = df_groupSKU_master.groupby(dimensions)[measures].sum().reset_index().rename(columns = rename_measure_dictionary)

        df_region_groupSKU = pd.merge(
                                    df_region_groupSKU,
                                    df_region_groupSKU.groupby(dimensions_total)[rename_measures].sum().reset_index().rename(columns = rename_total_measure_dictionary),
                                    on = dimensions_total
                                )

        df_region_groupSKU['Contribution'] = np.where( df_region_groupSKU['total_volume_SO'] == 0, np.nan, df_region_groupSKU['volume_SO'] / df_region_groupSKU['total_volume_SO'] )

        return df_region_groupSKU

    def etl_regionContribution_subDiv(df_groupSKU_master):
        df_groupSKU_master = df_groupSKU_master.rename(columns = {
            'SUB_DIVISION_NAME': 'Sub Division Name', 
            'GROUP_SKU': 'Group SKU', 
            'REGION_NAME': 'Region Name',
            'CHANNEL': 'Channel',
        })

        # filter channel / region / SO_value
        df_groupSKU_master = df_groupSKU_master[ 
            df_groupSKU_master['Channel'].isin([
                'GT0', 'MT0-WCM', 'MT0-non WCM', 'GT0_C1', 'KA0', 'NETCO_C1', 'NETCO_HRC'
            ])
            & (~ df_groupSKU_master['Region Name'].isin(['Others']) )
            & (df_groupSKU_master['SO_VALUE'] != 0)
        ]

        dimensions = ['Sub Division Name', 'Region Name', 'Channel']
        dimensions_total = list(set(dimensions) - set(['Region Name']))
        measures = ['SO_QTY']
        rename_measures = ['volume_SO']
        rename_total_measures = ['total_' + i for i in rename_measures]
        rename_measure_dictionary = dict(zip(measures, rename_measures))
        rename_total_measure_dictionary = dict(zip(rename_measures, rename_total_measures))
        # Region_Sub Division
        df_region_subDivision = df_groupSKU_master.groupby(['Sub Division Name', 'Region Name', 'Channel'])[measures].sum().reset_index().rename(columns = rename_measure_dictionary)

        df_region_subDivision = pd.merge(
                                    df_region_subDivision,
                                    df_region_subDivision.groupby(dimensions_total)[rename_measures].sum().reset_index().rename(columns = rename_total_measure_dictionary),
                                    on = dimensions_total
                                )

        df_region_subDivision['Contribution'] = np.where( df_region_subDivision['total_volume_SO'] == 0, np.nan, df_region_subDivision['volume_SO'] / df_region_subDivision['total_volume_SO'] )

        return df_region_subDivision

    df_region_groupSKU = etl_regionContribution_groupSKU(df_groupSKU_master)
    df_region_subDivision = etl_regionContribution_subDiv(df_groupSKU_master)

    REGIONS = ['Miền Bắc', 'Miền Trung', 'Miền Tây', 'Miền Đông', 'HCM']

    # Expand DataFrame with each value in the list
    df_manual_groupSKU_region = pd.DataFrame(np.repeat(df_contribution_melted.values, len(REGIONS), axis=0), columns=df_contribution_melted.columns)
    df_manual_groupSKU_region['Input Region Name'] = np.tile(REGIONS, len(df_contribution_melted))
    df_manual_groupSKU_region['Region Name'] = df_manual_groupSKU_region['Input Region Name']

    df_manual_groupSKU_NW = pd.merge(
                                pd.merge(
                                    pd.merge(
                                        df_contribution_melted,
                                        df_region_contribution_melted,
                                        how = 'left',
                                        left_on = ['Demand Planning Standard SKU Name', 'Channel', 'Month'],
                                        right_on = ['DP Name', 'Channel Code', 'Month'],
                                        suffixes = ['', '_region_contribution']
                                    ),
                                    df_region_groupSKU[['Group SKU', 'Channel', 'Region Name', 'Contribution']],
                                    how = 'left',
                                    on = ['Group SKU', 'Channel'],
                                    suffixes = ['', '_region_groupSKU']
                                ),
                                df_region_subDivision[['Sub Division Name', 'Channel', 'Region Name', 'Contribution']],
                                how = 'left',
                                on = ['Sub Division Name', 'Channel'], 
                                suffixes = ['', '_region_subDivision']
                            ).rename(columns = {'Contribution': 'Contribution_DPName_inCat'})

    df_manual_groupSKU_NW['Input Region Name'] = 'NW'

    df_manual_groupSKU_NW['Region Name'] = np.where(df_manual_groupSKU_NW['Region Name'].notnull(), 
                                                    df_manual_groupSKU_NW['Region Name'],
                                                    np.where(df_manual_groupSKU_NW['Region Name_region_groupSKU'].notnull(), 
                                                             df_manual_groupSKU_NW['Region Name_region_groupSKU'],
                                                             df_manual_groupSKU_NW['Region Name_region_subDivision']
                                                       )
                                                   )

    df_manual_groupSKU_NW['Contribution_Cat_inRegion'] = np.where(df_manual_groupSKU_NW['Contribution_region_contribution'].notnull(), 
                                                    df_manual_groupSKU_NW['Contribution_region_contribution'],
                                                    np.where(df_manual_groupSKU_NW['Contribution_region_groupSKU'].notnull(), 
                                                             df_manual_groupSKU_NW['Contribution_region_groupSKU'],
                                                             df_manual_groupSKU_NW['Contribution_region_subDivision']
                                                       )
                                                   )

    df_manual_groupSKU_NW['Contribution'] = df_manual_groupSKU_NW['Contribution_Cat_inRegion'] * df_manual_groupSKU_NW['Contribution_DPName_inCat']

    df_manual_groupSKU_NW = df_manual_groupSKU_NW[df_manual_groupSKU_region.columns]
    
    df_manual_groupSKU_region = df_manual_groupSKU_region.drop_duplicates()
    df_manual_groupSKU_NW = df_manual_groupSKU_NW.drop_duplicates()

    df_manual_groupSKU = pd.concat([df_manual_groupSKU_region, df_manual_groupSKU_NW], ignore_index = True)

    # # drop duplicates due to multiple left join
    df_manual_groupSKU = df_manual_groupSKU.drop_duplicates()

    df_manual_groupSKU.columns = 'Manual Group SKU.' + df_manual_groupSKU.columns
    
    # convert datatype
    df_manual_groupSKU['Manual Group SKU.Month'] = df_manual_groupSKU['Manual Group SKU.Month'].astype('float') 
    
    return df_manual_groupSKU

# DEFAULT GROUP SKU 
def etl_default_groupSKU(df_groupSKU_master, df_contribution_melted):
    df_groupSKU_master = df_groupSKU_master.rename(columns = {
            'DEMAND_PLANNING_STANDARD_SKU_NAME': 'Demand Planning Standard SKU Name',
            'SUB_DIVISION_NAME': 'Sub Division Name', 
            'GROUP_SKU': 'Group SKU', 
            'REGION_NAME': 'Region Name',
            'CHANNEL': 'Channel',
        })

    # filter channel / region / SO_value
    df_groupSKU_master = df_groupSKU_master[ 
        df_groupSKU_master['Channel'].isin([
            'GT0', 'MT0-WCM', 'MT0-non WCM', 'GT0_C1', 'KA0', 'NETCO_C1', 'NETCO_HRC'
        ])
        & (~ df_groupSKU_master['Region Name'].isin(['Others']) )
        & (df_groupSKU_master['SO_VALUE'] != 0)
    ]

    # calculate groupSKU NW from df_groupSKU_master
    dimensions = ["Demand Planning Standard SKU Name", "Group SKU", "Channel", "Sub Division Name", "Region Name"]
    # contribution is by Group SKU / Channel / Sub Division Name
    dimensions_total_NW = ["Group SKU", "Channel", "Sub Division Name"]
    dimensions_total_region = ["Group SKU", "Channel", "Region Name"]
    measures = ['SO_QTY']
    rename_measures = ['volume_SO']
    rename_total_measures = ['total_' + i for i in rename_measures]
    rename_measure_dictionary = dict(zip(measures, rename_measures))
    rename_total_measure_dictionary = dict(zip(rename_measures, rename_total_measures))

    df_default_groupSKU_NW = df_groupSKU_master.groupby(dimensions)[measures].sum().reset_index().rename(columns = rename_measure_dictionary)

    df_default_groupSKU_NW = pd.merge(df_default_groupSKU_NW,
                                   df_default_groupSKU_NW.groupby(dimensions_total_NW)[rename_measures].sum().reset_index().rename(columns = rename_total_measure_dictionary),
                                   how = 'left',
                                   on = dimensions_total_NW
                                  )

    df_default_groupSKU_NW['Contribution'] = np.where(df_default_groupSKU_NW['total_volume_SO'] == 0, 0, df_default_groupSKU_NW['volume_SO'] / df_default_groupSKU_NW['total_volume_SO'])

    # get left_only when joining df_groupSKU_default with df_manual_groupSKU_step2 
    df_default_groupSKU_NW = pd.merge(df_default_groupSKU_NW,
                                     df_contribution_melted[['Group SKU', 'Channel']].drop_duplicates(),
                                     how = 'left', 
                                     on = ['Group SKU', 'Channel'],
                                     indicator = True
                                    )

    df_default_groupSKU_NW = df_default_groupSKU_NW[df_default_groupSKU_NW['_merge'] == 'left_only']

    # add column Input Region = 'NW'
    df_default_groupSKU_NW['Input Region Name'] = 'NW'

    # calculate groupSKU default from df_groupSKU_master
    df_default_groupSKU_region = df_groupSKU_master.groupby(dimensions)[measures].sum().reset_index().rename(columns = rename_measure_dictionary)

    df_default_groupSKU_region = pd.merge(df_default_groupSKU_region,
                                   df_default_groupSKU_region.groupby(dimensions_total_region)[rename_measures].sum().reset_index().rename(columns = rename_total_measure_dictionary),
                                   how = 'left',
                                   on = dimensions_total_region
                                  )

    df_default_groupSKU_region['Contribution'] = np.where(df_default_groupSKU_region['total_volume_SO'] == 0, 0, df_default_groupSKU_region['volume_SO'] / df_default_groupSKU_region['total_volume_SO'])

    # get left_only when joining df_groupSKU_default with df_manual_groupSKU_step2 
    df_default_groupSKU_region = pd.merge(df_default_groupSKU_region,
                                     df_contribution_melted[['Group SKU', 'Channel']].drop_duplicates(),
                                     how = 'left', 
                                     on = ['Group SKU', 'Channel'],
                                     indicator = True
                                    )

    df_default_groupSKU_region = df_default_groupSKU_region[df_default_groupSKU_region['_merge'] == 'left_only']

    # add column Input Region = Region Name
    df_default_groupSKU_region['Input Region Name'] = df_default_groupSKU_region['Region Name']

    df_default_groupSKU = pd.concat([df_default_groupSKU_region, df_default_groupSKU_NW], ignore_index = True)

    df_default_groupSKU = df_default_groupSKU[dimensions + ['Contribution', 'Input Region Name']]

    df_default_groupSKU.columns = 'Default Group SKU.' + df_default_groupSKU.columns
    
    return df_default_groupSKU

def etl_price(df_price, df_week):
    def etl_clean_input_data(df):
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        return df

    df_price_list = df_price.rename(columns = {
        'SUB_DIVISION_NAME': 'Sub Division Name',
        'GROUP_SKU': 'Group SKU',
        'DPNAME': 'DPName', 
        'ITEM_CODE': 'Item Code',
        'PRODUCT_NAME': 'Product Name', 
        'CHANNEL': 'Channel', 
        'REGION_NAME': 'Region'
    })
    
    df_price_list = etl_clean_input_data(df_price_list)

    channel_list = ['MT0', 'GT0', 'KA0', 'GT0_C1', 'NETCO_C1', 'NETCO_HRC']
    region_list = ['Miền Bắc', 'Miền Đông', 'Miền Trung', 'Miền Tây', 'HCM', 'NW']


    # unpivot price_list
    df_price_list_melted = pd.melt(df_price_list, 
                                     id_vars=['Sub Division Name', 'Group SKU', 'DPName', 'Item Code', 'Product Name', 'Channel', 'Region'], 
                                     value_vars=list(set(df_price_list.columns) - set(['Sub Division Name', 'Group SKU', 'DPName', 'Item Code', 'Product Name', 'Channel', 'Region']))
                                    ).rename(columns = {'variable': 'Week', 'value': 'Price'})

    df_price_list_melted['Week'] = df_price_list_melted['Week'].astype('float') 
    
    # convert datatype of df_price_list_melted
    df_price_list_melted = df_price_list_melted[df_price_list_melted['Week'].isin(df_week['Week'])]

    # pivot
    df_price_list_melted_pivotChannel = pd.pivot_table(df_price_list_melted, 
                                           values='Price', 
                                           index=['Sub Division Name', 'Group SKU', 'DPName', 'Item Code', 'Product Name', 'Region', 'Week'], 
                                           columns='Channel').reset_index()

    # create new columns for channel in df_channel_list in case the df doesnt have it
    for channel in channel_list:
        if channel not in df_price_list_melted_pivotChannel.columns:
            df_price_list_melted_pivotChannel[channel] = np.nan


    for channel in channel_list:   
        df_price_list_melted_pivotChannel[channel] = df_price_list_melted_pivotChannel[channel].fillna(df_price_list_melted_pivotChannel['GT0'])
        df_price_list_melted_pivotChannel[channel] = np.where(
            (df_price_list_melted_pivotChannel[channel].isnull() | df_price_list_melted_pivotChannel[channel] == 0)
            & (df_price_list_melted_pivotChannel['GT0'] > 0), 
            df_price_list_melted_pivotChannel['GT0'],
            np.where(
                (df_price_list_melted_pivotChannel[channel].isnull() | df_price_list_melted_pivotChannel[channel] == 0)
                & (df_price_list_melted_pivotChannel['GT0'] == 0), 
                df_price_list_melted_pivotChannel['GT0_C1'],
            df_price_list_melted_pivotChannel[channel]
            )
        )

    # df_price_list_melted_2 
    df_price_list_melted_2 = pd.melt(df_price_list_melted_pivotChannel, 
                                     id_vars=['Sub Division Name', 'Group SKU', 'DPName', 'Item Code', 'Product Name', 'Region', 'Week'], 
                                     value_vars=channel_list
                                    ).rename(columns = {'variable': 'Channel', 'value': 'Price'})

    # pivot
    df_price_list_melted_2_pivotRegion = pd.pivot_table(df_price_list_melted_2, 
                                           values='Price', 
                                           index=['Sub Division Name', 'Group SKU', 'DPName', 'Item Code', 'Product Name', 'Week', 'Channel'], 
                                           columns='Region', 
                                           aggfunc='sum').reset_index()

    # create new columns for region in df_region_list in case the df doesnt have it
    for region in region_list:
        if region not in df_price_list_melted_2_pivotRegion.columns:
            df_price_list_melted_2_pivotRegion[region] = np.nan

    for region in region_list:
        df_price_list_melted_2_pivotRegion[region] = df_price_list_melted_2_pivotRegion[region].fillna(df_price_list_melted_2_pivotRegion['Miền Bắc'])
        df_price_list_melted_2_pivotRegion[region] = np.where(
            df_price_list_melted_2_pivotRegion[region].isnull() | df_price_list_melted_2_pivotRegion[region] == 0,
            df_price_list_melted_2_pivotRegion['Miền Bắc'],
            df_price_list_melted_2_pivotRegion[region]
        )

    # df_price_list_melted_3 
    df_price_list_melted_3 = pd.melt(df_price_list_melted_2_pivotRegion, 
                                     id_vars=['Sub Division Name', 'Group SKU', 'DPName', 'Item Code', 'Product Name', 'Week', 'Channel'], 
                                     value_vars=region_list
                                    ).rename(columns = {'variable': 'Region', 'value': 'Price'})

    df_price_list_melted_3 = pd.merge(df_price_list_melted_3, 
                                      df_week,
                                      how = 'left',
                                      on = ['Week']
                                     )

    df_price_list_month = df_price_list_melted_3.groupby([
        "Sub Division Name", "Group SKU", "DPName", "Channel", "Region", "MONTH", "YEAR"
        ])['Price'].mean().reset_index()
    
    df_price_list_month[['YEAR', 'MONTH']] = df_price_list_month[['YEAR', 'MONTH']].astype('float64')
    
    # change from MT0 -> MT0-WCM and MT0-non WCM
    df_price_list_month_mt = pd.merge(
        df_price_list_month[
            df_price_list_month['Channel'] == 'MT0'
        ].assign(mapping = 1),
        pd.DataFrame({
            'mapping': [1, 1],
            'suffix': ['-WCM', '-non WCM']
        }),
        on = ['mapping']
    ).drop('mapping', axis = 1)

    df_price_list_month_mt['Channel'] = df_price_list_month_mt['Channel'] + df_price_list_month_mt['suffix']

    df_price_list_month_mt = df_price_list_month_mt.drop('suffix', axis = 1)

    df_price_list_month_non_mt = df_price_list_month[
            df_price_list_month['Channel'] != 'MT0'
        ]

    df_price_list_month_final = pd.concat([
        df_price_list_month_non_mt,
        df_price_list_month_mt
        ], 
        ignore_index = True
    )

    return df_price_list_month_final

def province_contribution_byDPName(df_groupSKU_byProvince):
    # filter channel / region / SO_value
    df_groupSKU_byProvince = df_groupSKU_byProvince[ 
        df_groupSKU_byProvince['CHANNEL'].isin([
            'GT0', 'MT0-WCM', 'MT0-non WCM', 'GT0_C1', 'KA0', 'NETCO_C1', 'NETCO_HRC'
        ])
        & (~ df_groupSKU_byProvince['REGION_NAME'].isin(['Others']) )
        & (df_groupSKU_byProvince['SO_VALUE'] != 0)
    ]

    # calculate groupSKU NW from df_groupSKU_byProvince
    dimensions = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'REGION_NAME', 'PROVINCE', 'CHANNEL']
    # contribution is by 'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',  'PROVINCE', 'CHANNEL'
    dimensions_total_region = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'REGION_NAME', 'CHANNEL']
    measures = ['SO_QTY']
    rename_measures = ['volume_SO']
    rename_total_measures = ['total_' + i for i in rename_measures]
    rename_measure_dictionary = dict(zip(measures, rename_measures))
    rename_total_measure_dictionary = dict(zip(rename_measures, rename_total_measures))

    df_contribution_byProvince_inRegion = df_groupSKU_byProvince.groupby(dimensions)[measures].sum().reset_index().rename(columns = rename_measure_dictionary)

    df_contribution_byProvince_inRegion = pd.merge(df_contribution_byProvince_inRegion,
                                   df_contribution_byProvince_inRegion.groupby(dimensions_total_region)[rename_measures].sum().reset_index().rename(columns = rename_total_measure_dictionary),
                                   how = 'left',
                                   on = dimensions_total_region
                                  )

    df_contribution_byProvince_inRegion['Contribution'] = np.where(df_contribution_byProvince_inRegion['total_volume_SO'] == 0, 0, df_contribution_byProvince_inRegion['volume_SO'] / df_contribution_byProvince_inRegion['total_volume_SO'])

    df_contribution_byProvince_inRegion = df_contribution_byProvince_inRegion[dimensions + ['Contribution']]

    return df_contribution_byProvince_inRegion

def province_contribution_byGroupSKU(df_groupSKU_byProvince):
    # filter channel / region / SO_value
    df_groupSKU_byProvince = df_groupSKU_byProvince[ 
        df_groupSKU_byProvince['CHANNEL'].isin([
            'GT0', 'MT0-WCM', 'MT0-non WCM', 'GT0_C1', 'KA0', 'NETCO_C1', 'NETCO_HRC'
        ])
        & (~ df_groupSKU_byProvince['REGION_NAME'].isin(['Others']) )
        & (df_groupSKU_byProvince['SO_VALUE'] != 0)
    ]

    # calculate groupSKU NW from df_groupSKU_byProvince
    dimensions = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'REGION_NAME', 'PROVINCE', 'CHANNEL']
    # contribution is by 'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',  'PROVINCE', 'CHANNEL'
    dimensions_total_region = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'REGION_NAME', 'CHANNEL']
    measures = ['SO_QTY']
    rename_measures = ['volume_SO']
    rename_total_measures = ['total_' + i for i in rename_measures]
    rename_measure_dictionary = dict(zip(measures, rename_measures))
    rename_total_measure_dictionary = dict(zip(rename_measures, rename_total_measures))

    df_contribution_byProvince_inRegion = df_groupSKU_byProvince.groupby(dimensions)[measures].sum().reset_index().rename(columns = rename_measure_dictionary)

    df_contribution_byProvince_inRegion = pd.merge(df_contribution_byProvince_inRegion,
                                   df_contribution_byProvince_inRegion.groupby(dimensions_total_region)[rename_measures].sum().reset_index().rename(columns = rename_total_measure_dictionary),
                                   how = 'left',
                                   on = dimensions_total_region
                                  )

    df_contribution_byProvince_inRegion['Contribution'] = np.where(df_contribution_byProvince_inRegion['total_volume_SO'] == 0, 0, df_contribution_byProvince_inRegion['volume_SO'] / df_contribution_byProvince_inRegion['total_volume_SO'])

    df_contribution_byProvince_inRegion = df_contribution_byProvince_inRegion[dimensions + ['Contribution']]

    return df_contribution_byProvince_inRegion

def province_contribution_bySubDivision(df_groupSKU_byProvince):
    # filter channel / region / SO_value
    df_groupSKU_byProvince = df_groupSKU_byProvince[ 
        df_groupSKU_byProvince['CHANNEL'].isin([
            'GT0', 'MT0-WCM', 'MT0-non WCM', 'GT0_C1', 'KA0', 'NETCO_C1', 'NETCO_HRC'
        ])
        & (~ df_groupSKU_byProvince['REGION_NAME'].isin(['Others']) )
        & (df_groupSKU_byProvince['SO_VALUE'] != 0)
    ]

    # calculate groupSKU NW from df_groupSKU_byProvince
    dimensions = ['SUB_DIVISION_NAME', 'REGION_NAME', 'PROVINCE', 'CHANNEL']
    # contribution is by 'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',  'PROVINCE', 'CHANNEL'
    dimensions_total_region = ['SUB_DIVISION_NAME', 'REGION_NAME', 'CHANNEL']
    measures = ['SO_QTY']
    rename_measures = ['volume_SO']
    rename_total_measures = ['total_' + i for i in rename_measures]
    rename_measure_dictionary = dict(zip(measures, rename_measures))
    rename_total_measure_dictionary = dict(zip(rename_measures, rename_total_measures))

    df_contribution_byProvince_inRegion = df_groupSKU_byProvince.groupby(dimensions)[measures].sum().reset_index().rename(columns = rename_measure_dictionary)

    df_contribution_byProvince_inRegion = pd.merge(df_contribution_byProvince_inRegion,
                                   df_contribution_byProvince_inRegion.groupby(dimensions_total_region)[rename_measures].sum().reset_index().rename(columns = rename_total_measure_dictionary),
                                   how = 'left',
                                   on = dimensions_total_region
                                  )

    df_contribution_byProvince_inRegion['Contribution'] = np.where(df_contribution_byProvince_inRegion['total_volume_SO'] == 0, 0, df_contribution_byProvince_inRegion['volume_SO'] / df_contribution_byProvince_inRegion['total_volume_SO'])

    df_contribution_byProvince_inRegion = df_contribution_byProvince_inRegion[dimensions + ['Contribution']]

    return df_contribution_byProvince_inRegion

def province_contribution_byDefault(df_groupSKU_byProvince):
    # filter channel / region / SO_value
    df_groupSKU_byProvince = df_groupSKU_byProvince[ 
        df_groupSKU_byProvince['CHANNEL'].isin([
            'GT0', 'MT0-WCM', 'MT0-non WCM', 'GT0_C1', 'KA0', 'NETCO_C1', 'NETCO_HRC'
        ])
        & (~ df_groupSKU_byProvince['REGION_NAME'].isin(['Others']) )
        & (df_groupSKU_byProvince['SO_VALUE'] != 0)
    ]

    # calculate groupSKU NW from df_groupSKU_byProvince
    dimensions = ['SUB_DIVISION_NAME', 'REGION_NAME', 'PROVINCE']
    # contribution is by 'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',  'PROVINCE', 'CHANNEL'
    dimensions_total_region = ['SUB_DIVISION_NAME', 'REGION_NAME']
    measures = ['SO_QTY']
    rename_measures = ['volume_SO']
    rename_total_measures = ['total_' + i for i in rename_measures]
    rename_measure_dictionary = dict(zip(measures, rename_measures))
    rename_total_measure_dictionary = dict(zip(rename_measures, rename_total_measures))

    df_contribution_byProvince_inRegion = df_groupSKU_byProvince.groupby(dimensions)[measures].sum().reset_index().rename(columns = rename_measure_dictionary)

    df_contribution_byProvince_inRegion = pd.merge(df_contribution_byProvince_inRegion,
                                   df_contribution_byProvince_inRegion.groupby(dimensions_total_region)[rename_measures].sum().reset_index().rename(columns = rename_total_measure_dictionary),
                                   how = 'left',
                                   on = dimensions_total_region
                                  )

    df_contribution_byProvince_inRegion['Contribution'] = np.where(df_contribution_byProvince_inRegion['total_volume_SO'] == 0, 0, df_contribution_byProvince_inRegion['volume_SO'] / df_contribution_byProvince_inRegion['total_volume_SO'])

    df_contribution_byProvince_inRegion = df_contribution_byProvince_inRegion[dimensions + ['Contribution']]

    return df_contribution_byProvince_inRegion

def etl_muf(df_muf_input_melted, df_default_groupSKU, df_manual_groupSKU, df_baseline_forecast, df_price_list_month, df_master_data, df_province_contribution_inRegion_byDPName, df_province_contribution_inRegion_byGroupSKU, df_province_contribution_inRegion_bySubDivision, df_province_contribution_inRegion_byDefault):
    
    # group by id_vars and calculate sum / muf input from other teams
    df_muf_input_final_with_baseline = df_muf_input_melted.groupby(['Group SKU', 'Sub Division', 'Site', 'Building Blocks', 
                                 'Year', 'Channel', 'Uplift type', 'Region', 'Month', 'Risk %'])['value'].sum().reset_index()
    # change datatype of column Year, Month

    df_muf_input_final_with_baseline[['Year', 'Month']] = df_muf_input_final_with_baseline[['Year', 'Month']].astype('float') 

    # def etl_muf_input_final_withBaseline(df_muf_input_final_with_baseline, df_price_list_month, df_default_groupSKU, df_manual_groupSKU): 
    # using df_groupSKU_default and df_manual_groupSKU_final to map with df_muf_input_final
    df_muf_input_final = pd.merge(pd.merge(df_muf_input_final_with_baseline,
                                          df_default_groupSKU,
                                          how = 'left',
                                          left_on = ['Group SKU', 'Channel', 'Region'],
                                          right_on = ['Default Group SKU.Group SKU', 'Default Group SKU.Channel', 'Default Group SKU.Input Region Name']
                                         ),
                                 df_manual_groupSKU,
                                 how = 'left',
                                 left_on = ['Group SKU', 'Channel', 'Region', 'Month'],
                                 right_on = ['Manual Group SKU.Group SKU', 'Manual Group SKU.Channel', 'Manual Group SKU.Input Region Name', 'Manual Group SKU.Month']
                                )

    # add columns 
    df_muf_input_final['DP Contribution'] = np.where(df_muf_input_final['Manual Group SKU.Contribution'].notnull(), 
                                                     df_muf_input_final['Manual Group SKU.Contribution'], 
                                                     df_muf_input_final['Default Group SKU.Contribution']
                                                    )

    df_muf_input_final['DP Name'] = np.where(df_muf_input_final['Manual Group SKU.Demand Planning Standard SKU Name'].notnull(), 
                                                     df_muf_input_final['Manual Group SKU.Demand Planning Standard SKU Name'], 
                                                     df_muf_input_final['Default Group SKU.Demand Planning Standard SKU Name']
                                                    )

    df_muf_input_final['Region Final'] = np.where(df_muf_input_final['Manual Group SKU.Region Name'].notnull(), 
                                                     df_muf_input_final['Manual Group SKU.Region Name'], 
                                                     df_muf_input_final['Default Group SKU.Region Name']
                                                    )


    # select only needed columns
    df_muf_input_final = df_muf_input_final[[
        'Group SKU', 'DP Name', 'Region Final', 'Region', 'Channel', 'Building Blocks', 
        'Uplift type', 'Risk %', 'Year',  'Month', 'value', 'DP Contribution'
    ]].rename(columns = {
        'Group SKU': 'GROUP_SKU',
        'DP Name': 'DEMAND_PLANNING_STANDARD_SKU_NAME',
        'Region Final': 'REGION_NAME',
        'Region': 'REGION',
        'Channel': 'CHANNEL',
        'Building Blocks': 'BUILDING_BLOCKS',
        'Uplift type': 'UPLIFT_TYPE',
        'Year': 'FORECAST_YEAR',
        'Month': 'FORECAST_MONTH'
    })

    df_muf_input_final = pd.merge(
        df_muf_input_final,
        df_master_data[
            (df_master_data['ACTIVE_STATUS'] == 'Active')
            & (df_master_data['MARKET_TYPE'] == 'Local')
        ][['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME']].drop_duplicates(),
        on = ['GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME']
    )

    df_muf_input_final1 = pd.merge(
        df_muf_input_final,
        df_province_contribution_inRegion_byDPName,
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME', 'CHANNEL'],
        suffixes = ['', '_byDPName']
    )

    df_muf_input_final_byDPname = df_muf_input_final1[
        df_muf_input_final1['PROVINCE'].notnull()
    ]

    df_muf_input_final2 = pd.merge(
        df_muf_input_final1[
            df_muf_input_final1['PROVINCE'].isnull()
        ],
        df_province_contribution_inRegion_byGroupSKU,
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'REGION_NAME', 'CHANNEL'],
        suffixes = ['', '_byGroupSKU']
    )    

    df_muf_input_final_byGroupSKU = df_muf_input_final2[
        df_muf_input_final2['PROVINCE_byGroupSKU'].notnull()
    ]

    df_muf_input_final3 = pd.merge(
        df_muf_input_final2[
            df_muf_input_final2['PROVINCE_byGroupSKU'].isnull()
        ],
        df_province_contribution_inRegion_bySubDivision,
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'REGION_NAME', 'CHANNEL'],
        suffixes = ['', '_bySubDivision']
    )

    df_muf_input_final_bySubDivision = df_muf_input_final3[
        df_muf_input_final3['PROVINCE_bySubDivision'].notnull()
    ]
    
    df_muf_input_final4 = pd.merge(
        df_muf_input_final3[
            df_muf_input_final3['PROVINCE_bySubDivision'].isnull()
        ],
        df_province_contribution_inRegion_byDefault,
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'REGION_NAME'],
        suffixes = ['', '_byDefault']
    )
    
    df_muf_input_final_byDefault = df_muf_input_final4[
        df_muf_input_final4['PROVINCE_byDefault'].notnull()
    ]

    df_muf_input_final_byDPname = df_muf_input_final_byDPname.rename(columns = {'Contribution': 'PROVINCE_Contribution'})[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME',
           'REGION', 'PROVINCE', 'CHANNEL', 'BUILDING_BLOCKS', 'UPLIFT_TYPE', 'Risk %',
           'FORECAST_YEAR', 'FORECAST_MONTH', 'value', 'DP Contribution','PROVINCE_Contribution']].drop_duplicates()

    df_muf_input_final_byGroupSKU['PROVINCE'] = df_muf_input_final_byGroupSKU['PROVINCE_byGroupSKU'] 
    df_muf_input_final_byGroupSKU['PROVINCE_Contribution'] = df_muf_input_final_byGroupSKU['Contribution_byGroupSKU'] 
    df_muf_input_final_byGroupSKU = df_muf_input_final_byGroupSKU[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME',
           'REGION', 'PROVINCE', 'CHANNEL', 'BUILDING_BLOCKS', 'UPLIFT_TYPE', 'Risk %',
           'FORECAST_YEAR', 'FORECAST_MONTH', 'value', 'DP Contribution','PROVINCE_Contribution']].drop_duplicates()

    df_muf_input_final_bySubDivision['PROVINCE'] = df_muf_input_final_bySubDivision['PROVINCE_bySubDivision'] 
    df_muf_input_final_bySubDivision['PROVINCE_Contribution'] = df_muf_input_final_bySubDivision['Contribution_bySubDivision'] 
    df_muf_input_final_bySubDivision = df_muf_input_final_bySubDivision[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME',
           'REGION', 'PROVINCE', 'CHANNEL', 'BUILDING_BLOCKS', 'UPLIFT_TYPE', 'Risk %',
           'FORECAST_YEAR', 'FORECAST_MONTH', 'value', 'DP Contribution','PROVINCE_Contribution']].drop_duplicates()

    df_muf_input_final_byDefault['PROVINCE'] = df_muf_input_final_byDefault['PROVINCE_byDefault'] 
    df_muf_input_final_byDefault['PROVINCE_Contribution'] = df_muf_input_final_byDefault['Contribution_byDefault'] 
    df_muf_input_final_byDefault = df_muf_input_final_byDefault[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME',
           'REGION', 'PROVINCE', 'CHANNEL', 'BUILDING_BLOCKS', 'UPLIFT_TYPE', 'Risk %',
           'FORECAST_YEAR', 'FORECAST_MONTH', 'value', 'DP Contribution','PROVINCE_Contribution']].drop_duplicates()

    df_muf_input_final = pd.concat([
        df_muf_input_final_byDPname,
        df_muf_input_final_byGroupSKU,
        df_muf_input_final_bySubDivision,
        df_muf_input_final_byDefault
    ], ignore_index = True)

    # baseline, baseline adj, seasonality
    df_baseline_seasonality_blocks = df_baseline_forecast[
        # filter baseline with SUB_DIVISION_NAME from df_muf_input
        df_baseline_forecast['SUB_DIVISION_NAME'].isin(df_muf_input_final['SUB_DIVISION_NAME'].unique()) 
    ].melt(
        id_vars=['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH'], 
        value_vars=['BASELINE', 'BASELINE_ADJ', 'SEASONALITY']
          ).rename(columns = {
        'variable': 'BUILDING_BLOCKS'
    })
    df_baseline_seasonality_blocks['UPLIFT_TYPE'] = np.where(
        df_baseline_seasonality_blocks['BUILDING_BLOCKS'] == 'SEASONALITY', '%Vol', 'Vol'
    )
    df_baseline_seasonality_blocks['DP Contribution'] = 1 
    df_baseline_seasonality_blocks['PROVINCE_Contribution'] = 1 

    df_muf_input_final = pd.concat(
        [
            df_muf_input_final,
            df_baseline_seasonality_blocks
        ], 
        ignore_index = False
    )

    df_muf_input_final[['Risk %', 'value']] = df_muf_input_final[['Risk %', 'value']].fillna(0)
    df_muf_input_final = pd.merge(df_muf_input_final,
                                 df_price_list_month.rename(
                                     columns = {
                                         'Sub Division Name': 'SUB_DIVISION_NAME', 
                                         'Group SKU': 'GROUP_SKU',
                                         'DPName': 'DEMAND_PLANNING_STANDARD_SKU_NAME',
                                         'Channel': 'CHANNEL',
                                         'Region': 'REGION_NAME',
                                         'MONTH': 'FORECAST_MONTH', 
                                         'YEAR': 'FORECAST_YEAR'
                                     }
                                 ), 
                                 how = 'left',
                                 on = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'CHANNEL', 'REGION_NAME', 'FORECAST_YEAR', 'FORECAST_MONTH'],
                                )

    def etl_calculate_so_uplift_base(df_muf_input_final):
        # input val
        df_muf_input_val = df_muf_input_final[df_muf_input_final['UPLIFT_TYPE'] == 'Val']
        # calculate Sell Out - Vol (Kcase) / Val (Bio)
        df_muf_input_val['Sell Out - Val (Bio)'] = df_muf_input_val['value'] * df_muf_input_val['DP Contribution'] * df_muf_input_val['PROVINCE_Contribution']
        df_muf_input_val['Sell Out - Vol (Kcase)'] = df_muf_input_val['Sell Out - Val (Bio)'] * 1000000 / np.maximum(df_muf_input_val['Price'], 1e-8) #to handle the zero division error

        # input vol
        df_muf_input_vol = df_muf_input_final[df_muf_input_final['UPLIFT_TYPE'] == 'Vol']
        # calculate Sell Out - Vol (Kcase) / Val (Bio)
        df_muf_input_vol['Sell Out - Vol (Kcase)'] = df_muf_input_vol['value'] * df_muf_input_vol['DP Contribution'] * df_muf_input_vol['PROVINCE_Contribution']
        df_muf_input_vol['Sell Out - Val (Bio)'] =  df_muf_input_vol['Sell Out - Vol (Kcase)'] * df_muf_input_vol['Price'] / 1000000

        # step 2: combine df_muf_input_val & df_muf_input_vol to create df_SO_uplift
        df_SO_uplift = pd.concat([df_muf_input_val, df_muf_input_vol], ignore_index = False)

        df_SO_uplift[['Sell Out - Vol (Kcase)', 'Sell Out - Val (Bio)']] = df_SO_uplift[['Sell Out - Vol (Kcase)', 'Sell Out - Val (Bio)']].fillna(0)

        df_uplift_percent = pd.merge(
            df_muf_input_final[
                df_muf_input_final['UPLIFT_TYPE'].isin(['%Val', '%Vol'])
                               ],
            df_SO_uplift.groupby(
                [
                    'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
                    'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH',
                    'Price'
                ]
            ).agg({'Sell Out - Val (Bio)': 'sum', 'Sell Out - Vol (Kcase)': 'sum'}).reset_index(), 
            on = [
                    'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
                    'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH',
                    'Price'
                 ]
        )

        # validate uplift 
        df_uplift_percent = df_uplift_percent[
                (df_uplift_percent['REGION'] == 'NW')
                | (df_uplift_percent['REGION'] == df_uplift_percent['REGION_NAME'])
                | df_uplift_percent['REGION'].isnull()
        ]

        df_uplift_percent = df_uplift_percent.groupby(
            ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
               'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR',
               'FORECAST_MONTH', 'BUILDING_BLOCKS', 'UPLIFT_TYPE', 'Risk %',
               'DP Contribution', 'PROVINCE_Contribution', 'Price',
               'Sell Out - Val (Bio)', 'Sell Out - Vol (Kcase)']
        )['value'].sum().reset_index()

        df_uplift_percent2 = df_uplift_percent.pivot(index = list(set(df_uplift_percent.columns) - set(['UPLIFT_TYPE', 'value'])) , columns = ['UPLIFT_TYPE'], values = ['value']).reset_index()
        df_uplift_percent2.columns = ["".join(tup) for tup in df_uplift_percent2.columns.to_flat_index()]
        if 'value%Val' not in df_uplift_percent2.columns:
            df_uplift_percent2['value%Val'] = 0
        if 'value%Vol' not in df_uplift_percent2.columns:
            df_uplift_percent2['value%Vol'] = 0

        df_uplift_percent2[['value%Val', 'value%Vol']] = df_uplift_percent2[['value%Val', 'value%Vol']].fillna(0) 

        # calculate value / vol uplift
        df_uplift_percent2['Sell Out - Vol (Kcase) / uplift'] = df_uplift_percent2['Sell Out - Vol (Kcase)'] * df_uplift_percent2['value%Vol'] + df_uplift_percent2['Sell Out - Val (Bio)'] * df_uplift_percent2['value%Val'] * 1000000 / np.maximum(df_uplift_percent2['Price'], 1e-8) #to handle the zero division error
        df_uplift_percent2['Sell Out - Val (Bio) / uplift'] = df_uplift_percent2['Sell Out - Val (Bio)'] * df_uplift_percent2['value%Val'] + df_uplift_percent2['Sell Out - Vol (Kcase)'] * df_uplift_percent2['value%Vol'] * np.maximum(df_uplift_percent2['Price'], 1e-8) / 1000000 #to handle the zero division error

        df_uplift_percent2[['Sell Out - Vol (Kcase)', 'Sell Out - Val (Bio)']] = df_uplift_percent2[['Sell Out - Vol (Kcase) / uplift', 'Sell Out - Val (Bio) / uplift']]

        df_SO_view = pd.concat([df_SO_uplift, df_uplift_percent2], ignore_index = True).drop(columns = [
                                                                                            'value%Val', 'value%Vol',
                                                                                           'Sell Out - Vol (Kcase) / uplift', 
                                                                                        'Sell Out - Val (Bio) / uplift']
                                                                                    )

        return df_SO_view

    def etl_calculate_uplift_percent(df_muf_input_final, df_SO_uplift):
        df_uplift_percent = pd.merge(
            df_muf_input_final[
                df_muf_input_final['UPLIFT_TYPE'].isin(['%Val', '%Vol'])
                               ],
            df_SO_uplift.groupby(
                [
                    'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
                    'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH',
                    'Price'
                ]
            ).agg({'Sell Out - Val (Bio)': 'sum', 'Sell Out - Vol (Kcase)': 'sum'}).reset_index(), 
            on = [
                    'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
                    'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH',
                    'Price'
                 ]
        )

        # validate uplift 
        df_uplift_percent = df_uplift_percent[
                (df_uplift_percent['REGION'] == 'NW')
                | (df_uplift_percent['REGION'] == df_uplift_percent['REGION_NAME'])
                | df_uplift_percent['REGION'].isnull()
        ]

        df_uplift_percent = df_uplift_percent.groupby(
            ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
               'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR',
               'FORECAST_MONTH', 'BUILDING_BLOCKS', 'UPLIFT_TYPE', 'Risk %',
               'DP Contribution', 'PROVINCE_Contribution', 'Price',
               'Sell Out - Val (Bio)', 'Sell Out - Vol (Kcase)']
        )['value'].sum().reset_index()

        df_uplift_percent2 = df_uplift_percent.pivot(index = list(set(df_uplift_percent.columns) - set(['UPLIFT_TYPE', 'value'])) , columns = ['UPLIFT_TYPE'], values = ['value']).reset_index()
        df_uplift_percent2.columns = ["".join(tup) for tup in df_uplift_percent2.columns.to_flat_index()]
        if 'value%Val' not in df_uplift_percent2.columns:
            df_uplift_percent2['value%Val'] = 0
        if 'value%Vol' not in df_uplift_percent2.columns:
            df_uplift_percent2['value%Vol'] = 0

        df_uplift_percent2[['value%Val', 'value%Vol']] = df_uplift_percent2[['value%Val', 'value%Vol']].fillna(0) 

        # calculate value / vol uplift
        df_uplift_percent2['Sell Out - Vol (Kcase) / uplift'] = df_uplift_percent2['Sell Out - Vol (Kcase)'] * df_uplift_percent2['value%Vol'] + df_uplift_percent2['Sell Out - Val (Bio)'] * df_uplift_percent2['value%Val'] * 1000000 / np.maximum(df_uplift_percent2['Price'], 1e-8) #to handle the zero division error
        df_uplift_percent2['Sell Out - Val (Bio) / uplift'] = df_uplift_percent2['Sell Out - Val (Bio)'] * df_uplift_percent2['value%Val'] + df_uplift_percent2['Sell Out - Vol (Kcase)'] * df_uplift_percent2['value%Vol'] * np.maximum(df_uplift_percent2['Price'], 1e-8) / 1000000 #to handle the zero division error

        df_uplift_percent2[['Sell Out - Vol (Kcase)', 'Sell Out - Val (Bio)']] = df_uplift_percent2[['Sell Out - Vol (Kcase) / uplift', 'Sell Out - Val (Bio) / uplift']]

        df_SO_view = pd.concat([df_SO_uplift, df_uplift_percent2], ignore_index = True).drop(columns = [
                                                                                            'value%Val', 'value%Vol',
                                                                                           'Sell Out - Vol (Kcase) / uplift', 
                                                                                        'Sell Out - Val (Bio) / uplift']
                                                                                    ) 
        return df_SO_view

    def etl_calculate_so_uplift_value(df_muf_input_final):
        # input val
        df_muf_input_val = df_muf_input_final[df_muf_input_final['UPLIFT_TYPE'] == 'Val']
        # calculate Sell Out - Vol (Kcase) / Val (Bio)
        df_muf_input_val['Sell Out - Val (Bio)'] = df_muf_input_val['value'] * df_muf_input_val['DP Contribution'] * df_muf_input_val['PROVINCE_Contribution']
        df_muf_input_val['Sell Out - Vol (Kcase)'] = df_muf_input_val['Sell Out - Val (Bio)'] * 1000000 / np.maximum(df_muf_input_val['Price'], 1e-8) #to handle the zero division error

        # input vol
        df_muf_input_vol = df_muf_input_final[df_muf_input_final['UPLIFT_TYPE'] == 'Vol']
        # calculate Sell Out - Vol (Kcase) / Val (Bio)
        df_muf_input_vol['Sell Out - Vol (Kcase)'] = df_muf_input_vol['value'] * df_muf_input_vol['DP Contribution'] * df_muf_input_vol['PROVINCE_Contribution']
        df_muf_input_vol['Sell Out - Val (Bio)'] =  df_muf_input_vol['Sell Out - Vol (Kcase)'] * df_muf_input_vol['Price'] / 1000000

        # step 2: combine df_muf_input_val & df_muf_input_vol to create df_SO_uplift
        df_SO_uplift = pd.concat([df_muf_input_val, df_muf_input_vol], ignore_index = False)

        df_SO_uplift[['Sell Out - Vol (Kcase)', 'Sell Out - Val (Bio)']] = df_SO_uplift[['Sell Out - Vol (Kcase)', 'Sell Out - Val (Bio)']].fillna(0)    

        return df_SO_uplift

    def etl_calculate_muf_final(df_SO_uplift_base_apply_other_building_blocks, df_SO_uplift_other_building_blocks):
        df_SO_view = pd.concat([df_SO_uplift_base_apply_other_building_blocks, df_SO_uplift_other_building_blocks], ignore_index = True)

        df_MUF = pd.concat(
        [df_SO_view.assign(
                        VIEW_BY = 'MUF',
                        SO_VALUE = lambda x: (1 - x['Risk %']) * x['Sell Out - Val (Bio)'],
                        SO_VOLUME = lambda x: (1- x['Risk %']) * x['Sell Out - Vol (Kcase)']
                    ),
         df_SO_view[df_SO_view['Risk %'] != 0].assign(
                        VIEW_BY = 'Opportunity',
                        SO_VALUE = lambda x: x['Risk %'] * x['Sell Out - Val (Bio)'],
                        SO_VOLUME = lambda x: x['Risk %'] * x['Sell Out - Vol (Kcase)']
                    )
        ], 
        ignore_index = True
        ).groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR',
           'FORECAST_MONTH', 'BUILDING_BLOCKS', 'VIEW_BY']
                         )[['SO_VALUE', 'SO_VOLUME']].sum().reset_index()

        df_MUF = df_MUF.drop_duplicates()

        return df_MUF

    df_base_building_blocks = df_muf_input_final[
        (df_muf_input_final['BUILDING_BLOCKS'].isin(['BASELINE', 'BASELINE_ADJ', 'SEASONALITY', 'Carry-over Innovation']))
    ]

    df_other_building_blocks = df_muf_input_final[
        (~df_muf_input_final['BUILDING_BLOCKS'].isin(['BASELINE', 'BASELINE_ADJ', 'SEASONALITY', 'Carry-over Innovation']))
    ]

    df_uplift_percent = df_muf_input_final[
        df_muf_input_final['UPLIFT_TYPE'].isin(['%Val', '%Vol'])
    ]

    # uplift val, vol baseline, baseline_adj, seasonality
    df_SO_uplift_base_value = etl_calculate_so_uplift_value(df_base_building_blocks)

    # uplift percent other building blocks apply to val/vol of baseline, baseline_adj, seasonality
    df_SO_uplift_base_apply_percent = etl_calculate_uplift_percent(df_uplift_percent, df_SO_uplift_base_value)

    # uplift val, vol other building blocks
    df_SO_uplift_other_building_blocks_value = etl_calculate_so_uplift_value(df_other_building_blocks)

    # muf
    df_muf = etl_calculate_muf_final(df_SO_uplift_base_apply_percent, df_SO_uplift_other_building_blocks_value)
    
    return df_muf

def etl_MUF_withDC(df_muf, df_mapping_province_DC):
    df_MUF_withDC = pd.merge(
        df_muf,
        df_mapping_province_DC[['DEMAND_PLANNING_STANDARD_SKU_NAME', 'PROVINCE', 'DC']],
        on = ['DEMAND_PLANNING_STANDARD_SKU_NAME', 'PROVINCE']
    )
    return df_MUF_withDC

def etl_weeklyPhasing(df_MUF_withDC, df_RR_by_PROVINCE_DPNAME, df_master_data, df_codeMappingMaster, df_mappingItemCode, df_master_date, df_forecast_week):
    df_mapping_productNumber = pd.merge(
        pd.melt(
            df_codeMappingMaster,
            id_vars = list(set(df_codeMappingMaster.columns) - set(['Miền Bắc', 'Miền Trung', 'HCM', 'Miền Đông', 'Miền Tây'])),
            value_vars = ['Miền Bắc', 'Miền Trung', 'HCM', 'Miền Đông', 'Miền Tây']
            ).rename(columns = {
                'variable': 'REGION_NAME',
                'value': 'PRODUCT_NUMBER'              
                        }
                    ),
        df_master_data[['PRODUCT_NUMBER', 'SUPPLY_STRATEGY']].drop_duplicates(),
        on = ['PRODUCT_NUMBER']
    )

    df_weighted_week = pd.merge(
        df_RR_by_PROVINCE_DPNAME,
        df_master_date.assign(WEEK_ID = np.where(df_master_date['DAY'] <= 7, 1, 
                                  np.where(df_master_date['DAY'] <= 14, 2, 
                                           np.where(df_master_date['DAY'] <= 21, 3, 
                                                    np.where(df_master_date['DAY'] <= 28, 4, 
                                                             5)
                                                   )
                                          )
                                 ),
                          WORKING_DAY_DC = np.where(df_master_date['WORKING_DAY_DC'] == True, 1, 0)
            ).groupby(['ISO_WEEKNUM', 'PERIOD_TAG', 'YEAR', 'MONTH', 'WEEK_ID'])['WORKING_DAY_DC'].sum().reset_index(),
        on = ['WEEK_ID']
    )

    df_weighted_week['SO_VOLUME'] = df_weighted_week['RUNNING_RATE'] * df_weighted_week['WORKING_DAY_DC']  

    df_weighted_week = df_weighted_week[
        df_weighted_week['SO_VOLUME'] > 0 
    ]

    df_week_contribution = pd.merge(
        df_weighted_week.groupby([
            'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
            'PROVINCE', 'CHANNEL', 'YEAR', 'MONTH', 'ISO_WEEKNUM', 'PERIOD_TAG'
        ])['SO_VOLUME'].sum().reset_index(),
        df_weighted_week.groupby([
            'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
            'PROVINCE', 'CHANNEL', 'YEAR', 'MONTH'
        ])['SO_VOLUME'].sum().reset_index().rename(columns = {
            'SO_VOLUME': 'TOTAL_SO_VOLUME'
        }),
        on = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
            'PROVINCE', 'CHANNEL', 'YEAR', 'MONTH']
    )

    df_week_contribution['WEEK_CONTRIBUTION'] = df_week_contribution['SO_VOLUME'] / df_week_contribution['TOTAL_SO_VOLUME']

    df_week_contribution = df_week_contribution[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
            'PROVINCE', 'CHANNEL', 'YEAR', 'MONTH', 'ISO_WEEKNUM', 'PERIOD_TAG', 'WEEK_CONTRIBUTION']]

    df_week_contribution[['YEAR', 'MONTH', 'WEEK_CONTRIBUTION']] = df_week_contribution[['YEAR', 'MONTH', 'WEEK_CONTRIBUTION']].astype('float')

    df_WUF_withDC = pd.merge(
        df_MUF_withDC,
        df_week_contribution.rename(columns = {
            'YEAR': 'FORECAST_YEAR',
            'MONTH': 'FORECAST_MONTH',
            'ISO_WEEKNUM': 'FORECAST_WEEK'
        }),
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH']
    )
    
    df_WUF_withDC_1 = df_WUF_withDC[
        df_WUF_withDC['WEEK_CONTRIBUTION'].notnull()
    ]

    # logic mapping: if Supply Stratrgy = null => X mappingItemCode X codeMappingMaster; elif Supply Strategy = 'Plan B' => X codeMappingMaster 
    df_weeklyPhasing_1 = pd.concat([
        pd.merge(
            pd.merge(
                df_mapping_productNumber[df_mapping_productNumber['SUPPLY_STRATEGY'].isnull()].rename(columns = {'REGION_NAME': 'REGION_ITEM_CODE'}),
                df_mappingItemCode,
                on = ['REGION_ITEM_CODE']
            )[['DEMAND_PLANNING_STANDARD_SKU_NAME', 'PRODUCT_NUMBER', 'SUPPLY_STRATEGY', 'CHANNEL', 'REGION_ITEM_CODE', 'DC', 'START_WEEK', 'END_WEEK']],
            df_WUF_withDC_1,
            on = ['DEMAND_PLANNING_STANDARD_SKU_NAME', 'CHANNEL', 'DC']
        ),
        pd.merge(
            df_mapping_productNumber[
                df_mapping_productNumber['SUPPLY_STRATEGY'] == 'Plan B'
            ][['DEMAND_PLANNING_STANDARD_SKU_NAME', 'PRODUCT_NUMBER', 'SUPPLY_STRATEGY', 'CHANNEL', 'REGION_NAME', 'START_WEEK', 'END_WEEK']],
            df_WUF_withDC_1,
            on = ['DEMAND_PLANNING_STANDARD_SKU_NAME', 'CHANNEL', 'REGION_NAME']
        )], 
        ignore_index = True
    )

    # filter period applied for each productNumber
    df_weeklyPhasing_1 = df_weeklyPhasing_1[
        (df_weeklyPhasing_1['FORECAST_YEAR']*100 + df_weeklyPhasing_1['FORECAST_WEEK'] >= df_weeklyPhasing_1['START_WEEK'])
        & (df_weeklyPhasing_1['FORECAST_YEAR']*100 + df_weeklyPhasing_1['FORECAST_WEEK'] <= df_weeklyPhasing_1['END_WEEK'])
    ]

    # calculate forecast value
    df_weeklyPhasing_1['FORECAST_SO_VALUE'] = df_weeklyPhasing_1['SO_VALUE'] * df_weeklyPhasing_1['WEEK_CONTRIBUTION']
    df_weeklyPhasing_1['FORECAST_SO_VOLUME'] = df_weeklyPhasing_1['SO_VOLUME'] * df_weeklyPhasing_1['WEEK_CONTRIBUTION']


    df_weeklyPhasing_1 = df_weeklyPhasing_1.drop_duplicates()

    df_weeklyPhasing_agg_1 = df_weeklyPhasing_1.groupby([
        'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'PRODUCT_NUMBER', 
        'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'VIEW_BY',
        'FORECAST_YEAR', 'FORECAST_MONTH', 'FORECAST_WEEK', 'PERIOD_TAG', 'event_time', 'version', 'note'
    ])[['FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME']].sum().reset_index()
    
    df_week_contribution_default = df_master_date.assign(WEIGHT_GT = np.where(df_master_date['DAY'] <= 7, 0.7, 
                                  np.where(df_master_date['DAY'] <= 21, 1, 1.3
                                          )
                                 ) 
            )[
        (df_master_date['WORKING_DAY_DC'] == True)
    ].groupby(['ISO_WEEKNUM', 'YEAR', 'MONTH', 'PERIOD_TAG'])['WEIGHT_GT'].sum().reset_index()

    df_week_contribution_default = df_master_date.assign(WEIGHT_GT = np.where(df_master_date['DAY'] <= 7, 0.7, 
                                  np.where(df_master_date['DAY'] <= 21, 1, 1.3
                                          )
                                 ) 
            )[
        (df_master_date['WORKING_DAY_DC'] == True)
    ].groupby(['ISO_WEEKNUM', 'YEAR', 'MONTH', 'PERIOD_TAG'])['WEIGHT_GT'].sum().reset_index()

    df_week_contribution_default = pd.merge(
        df_week_contribution_default,
        df_week_contribution_default.groupby(
            ['YEAR', 'MONTH']
        ).agg({
            'ISO_WEEKNUM': 'min', 
            'WEIGHT_GT': 'sum'
        }).reset_index().rename(columns = {
            'ISO_WEEKNUM': 'MIN_ISO_WEEKNUM_FULL_MONTH',
            'WEIGHT_GT': 'WEIGHT_GT_FULL_MONTH'
        }),
        on = ['YEAR', 'MONTH']
    )

    df_week_contribution_default['WEEK_CONTRIBUTION_GT'] = df_week_contribution_default['WEIGHT_GT'] / df_week_contribution_default['WEIGHT_GT_FULL_MONTH']  
    df_week_contribution_default['WEEK_CONTRIBUTION_MT'] = np.where( (df_week_contribution_default['ISO_WEEKNUM'] - df_week_contribution_default['MIN_ISO_WEEKNUM_FULL_MONTH']).isin([0,2]), 0.5, 0 )

    df_week_contribution_default[['YEAR', 'MONTH']] = df_week_contribution_default[['YEAR', 'MONTH']].astype('float')

    df_week_contribution_default = df_week_contribution_default[[
        'ISO_WEEKNUM', 'YEAR', 'MONTH', 'PERIOD_TAG', 'WEEK_CONTRIBUTION_GT', 'WEEK_CONTRIBUTION_MT'
    ]].rename(columns = {
        'YEAR': 'FORECAST_YEAR',
        'MONTH': 'FORECAST_MONTH',
        'ISO_WEEKNUM': 'FORECAST_WEEK'
    })

    df_WUF_withDC_2 = pd.merge(
        df_WUF_withDC[
            df_WUF_withDC['WEEK_CONTRIBUTION'].isnull()
        ].drop(['FORECAST_WEEK', 'PERIOD_TAG', 'WEEK_CONTRIBUTION'], axis = 1),
        df_week_contribution_default,
        on = ['FORECAST_YEAR', 'FORECAST_MONTH']   
    )

    # logic mapping: if Supply Stratrgy = null => X mappingItemCode X codeMappingMaster; elif Supply Strategy = 'Plan B' => X codeMappingMaster 
    df_weeklyPhasing_2 = pd.concat([
        pd.merge(
            pd.merge(
                df_mapping_productNumber[df_mapping_productNumber['SUPPLY_STRATEGY'].isnull()].rename(columns = {'REGION_NAME': 'REGION_ITEM_CODE'}),
                df_mappingItemCode,
                on = ['REGION_ITEM_CODE']
            )[['DEMAND_PLANNING_STANDARD_SKU_NAME', 'PRODUCT_NUMBER', 'SUPPLY_STRATEGY', 'CHANNEL', 'REGION_ITEM_CODE', 'DC', 'START_WEEK', 'END_WEEK']],
            df_WUF_withDC_2,
            on = ['DEMAND_PLANNING_STANDARD_SKU_NAME', 'CHANNEL', 'DC']
        ),
        pd.merge(
            df_mapping_productNumber[
                df_mapping_productNumber['SUPPLY_STRATEGY'] == 'Plan B'
            ][['DEMAND_PLANNING_STANDARD_SKU_NAME', 'PRODUCT_NUMBER', 'SUPPLY_STRATEGY', 'CHANNEL', 'REGION_NAME', 'START_WEEK', 'END_WEEK']],
            df_WUF_withDC_2,
            on = ['DEMAND_PLANNING_STANDARD_SKU_NAME', 'CHANNEL', 'REGION_NAME']
        )], 
        ignore_index = True
    )

    # filter period applied for each productNumber
    df_weeklyPhasing_2 = df_weeklyPhasing_2[
        (df_weeklyPhasing_2['FORECAST_YEAR']*100 + df_weeklyPhasing_2['FORECAST_WEEK'] >= df_weeklyPhasing_2['START_WEEK'])
        & (df_weeklyPhasing_2['FORECAST_YEAR']*100 + df_weeklyPhasing_2['FORECAST_WEEK'] <= df_weeklyPhasing_2['END_WEEK'])
    ]

    # calculate forecast value
    df_weeklyPhasing_2['FORECAST_SO_VALUE'] = np.where(
        df_weeklyPhasing_2['CHANNEL'].isin(['MT0-WCM', 'MT0-non WCM']), 
        df_weeklyPhasing_2['SO_VALUE'] * df_weeklyPhasing_2['WEEK_CONTRIBUTION_MT'],
        df_weeklyPhasing_2['SO_VALUE'] * df_weeklyPhasing_2['WEEK_CONTRIBUTION_GT']
    )

    df_weeklyPhasing_2['FORECAST_SO_VOLUME'] = np.where(
        df_weeklyPhasing_2['CHANNEL'].isin(['MT0-WCM', 'MT0-non WCM']), 
        df_weeklyPhasing_2['SO_VOLUME'] * df_weeklyPhasing_2['WEEK_CONTRIBUTION_MT'],
        df_weeklyPhasing_2['SO_VOLUME'] * df_weeklyPhasing_2['WEEK_CONTRIBUTION_GT']
    )

    df_weeklyPhasing_2 = df_weeklyPhasing_2.drop_duplicates()

    df_weeklyPhasing_agg_2 = df_weeklyPhasing_2.groupby([
        'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'PRODUCT_NUMBER', 
        'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'VIEW_BY',
        'FORECAST_YEAR', 'FORECAST_MONTH', 'FORECAST_WEEK', 'PERIOD_TAG', 
        'event_time', 'version', 'note'
    ])[['FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME']].sum().reset_index()
    
    df_weeklyPhasing_agg_final = pd.concat([df_weeklyPhasing_agg_1, df_weeklyPhasing_agg_2], ignore_index = True)
    
    df_weeklyPhasing_agg_final = pd.merge(
        df_weeklyPhasing_agg_final,
        df_forecast_week.groupby(
            ['YEAR', 'MONTH']
        )['ISO_WEEKNUM'].min().reset_index().rename(
            columns = {
                'YEAR': 'FORECAST_YEAR',
                'MONTH': 'FORECAST_MONTH',
                'ISO_WEEKNUM': 'FIRST_WEEK_OF_MONTH'
            }
        ),
        on = ['FORECAST_YEAR', 'FORECAST_MONTH']
    )
    
    df_weeklyPhasing_agg_final['WEEK_INDEX'] = df_weeklyPhasing_agg_final['FORECAST_WEEK'] - df_weeklyPhasing_agg_final['FIRST_WEEK_OF_MONTH'] + 1
    
    df_weeklyPhasing_agg_final = df_weeklyPhasing_agg_final[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'VIEW_BY',
           'FORECAST_YEAR', 'FORECAST_MONTH', 'FORECAST_WEEK', 'WEEK_INDEX', 'PERIOD_TAG',
           'event_time', 'version', 'note',
           'FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME']]
    
    return df_weeklyPhasing_agg_final

def etl_weeklyPhasing_simulation(df_MUF_withDC, df_RR_by_PROVINCE_DPNAME, df_master_data, df_codeMappingMaster, df_mappingItemCode, df_master_date, df_forecast_week):
    df_mapping_productNumber = pd.merge(
        pd.melt(
            df_codeMappingMaster,
            id_vars = list(set(df_codeMappingMaster.columns) - set(['Miền Bắc', 'Miền Trung', 'HCM', 'Miền Đông', 'Miền Tây'])),
            value_vars = ['Miền Bắc', 'Miền Trung', 'HCM', 'Miền Đông', 'Miền Tây']
            ).rename(columns = {
                'variable': 'REGION_NAME',
                'value': 'PRODUCT_NUMBER'              
                        }
                    ),
        df_master_data[['PRODUCT_NUMBER', 'SUPPLY_STRATEGY']].drop_duplicates(),
        on = ['PRODUCT_NUMBER']
    )

    df_weighted_week = pd.merge(
        df_RR_by_PROVINCE_DPNAME,
        df_master_date.assign(WEEK_ID = np.where(df_master_date['DAY'] <= 7, 1, 
                                  np.where(df_master_date['DAY'] <= 14, 2, 
                                           np.where(df_master_date['DAY'] <= 21, 3, 
                                                    np.where(df_master_date['DAY'] <= 28, 4, 
                                                             5)
                                                   )
                                          )
                                 ),
                          WORKING_DAY_DC = np.where(df_master_date['WORKING_DAY_DC'] == True, 1, 0)
            ).groupby(['ISO_WEEKNUM', 'PERIOD_TAG', 'YEAR', 'MONTH', 'WEEK_ID'])['WORKING_DAY_DC'].sum().reset_index(),
        on = ['WEEK_ID']
    )

    df_weighted_week['SO_VOLUME'] = df_weighted_week['RUNNING_RATE'] * df_weighted_week['WORKING_DAY_DC']  

    df_weighted_week = df_weighted_week[
        df_weighted_week['SO_VOLUME'] > 0 
    ]

    df_week_contribution = pd.merge(
        df_weighted_week.groupby([
            'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
            'PROVINCE', 'CHANNEL', 'YEAR', 'MONTH', 'ISO_WEEKNUM', 'PERIOD_TAG'
        ])['SO_VOLUME'].sum().reset_index(),
        df_weighted_week.groupby([
            'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
            'PROVINCE', 'CHANNEL', 'YEAR', 'MONTH'
        ])['SO_VOLUME'].sum().reset_index().rename(columns = {
            'SO_VOLUME': 'TOTAL_SO_VOLUME'
        }),
        on = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
            'PROVINCE', 'CHANNEL', 'YEAR', 'MONTH']
    )

    df_week_contribution['WEEK_CONTRIBUTION'] = df_week_contribution['SO_VOLUME'] / df_week_contribution['TOTAL_SO_VOLUME']

    df_week_contribution = df_week_contribution[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
            'PROVINCE', 'CHANNEL', 'YEAR', 'MONTH', 'ISO_WEEKNUM', 'PERIOD_TAG', 'WEEK_CONTRIBUTION']]

    df_week_contribution[['YEAR', 'MONTH', 'WEEK_CONTRIBUTION']] = df_week_contribution[['YEAR', 'MONTH', 'WEEK_CONTRIBUTION']].astype('float')

    df_WUF_withDC = pd.merge(
        df_MUF_withDC,
        df_week_contribution.rename(columns = {
            'YEAR': 'FORECAST_YEAR',
            'MONTH': 'FORECAST_MONTH',
            'ISO_WEEKNUM': 'FORECAST_WEEK'
        }),
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH']
    )
    
    df_WUF_withDC_1 = df_WUF_withDC[
        df_WUF_withDC['WEEK_CONTRIBUTION'].notnull()
    ]

    # logic mapping: if Supply Stratrgy = null => X mappingItemCode X codeMappingMaster; elif Supply Strategy = 'Plan B' => X codeMappingMaster 
    df_weeklyPhasing_1 = pd.concat([
        pd.merge(
            pd.merge(
                df_mapping_productNumber[df_mapping_productNumber['SUPPLY_STRATEGY'].isnull()].rename(columns = {'REGION_NAME': 'REGION_ITEM_CODE'}),
                df_mappingItemCode,
                on = ['REGION_ITEM_CODE']
            )[['DEMAND_PLANNING_STANDARD_SKU_NAME', 'PRODUCT_NUMBER', 'SUPPLY_STRATEGY', 'CHANNEL', 'REGION_ITEM_CODE', 'DC', 'START_WEEK', 'END_WEEK']],
            df_WUF_withDC_1,
            on = ['DEMAND_PLANNING_STANDARD_SKU_NAME', 'CHANNEL', 'DC']
        ),
        pd.merge(
            df_mapping_productNumber[
                df_mapping_productNumber['SUPPLY_STRATEGY'] == 'Plan B'
            ][['DEMAND_PLANNING_STANDARD_SKU_NAME', 'PRODUCT_NUMBER', 'SUPPLY_STRATEGY', 'CHANNEL', 'REGION_NAME', 'START_WEEK', 'END_WEEK']],
            df_WUF_withDC_1,
            on = ['DEMAND_PLANNING_STANDARD_SKU_NAME', 'CHANNEL', 'REGION_NAME']
        )], 
        ignore_index = True
    )

    # filter period applied for each productNumber
    df_weeklyPhasing_1 = df_weeklyPhasing_1[
        (df_weeklyPhasing_1['FORECAST_YEAR']*100 + df_weeklyPhasing_1['FORECAST_WEEK'] >= df_weeklyPhasing_1['START_WEEK'])
        & (df_weeklyPhasing_1['FORECAST_YEAR']*100 + df_weeklyPhasing_1['FORECAST_WEEK'] <= df_weeklyPhasing_1['END_WEEK'])
    ]

    # calculate forecast value
    df_weeklyPhasing_1['FORECAST_SO_VALUE'] = df_weeklyPhasing_1['SO_VALUE'] * df_weeklyPhasing_1['WEEK_CONTRIBUTION']
    df_weeklyPhasing_1['FORECAST_SO_VOLUME'] = df_weeklyPhasing_1['SO_VOLUME'] * df_weeklyPhasing_1['WEEK_CONTRIBUTION']


    df_weeklyPhasing_1 = df_weeklyPhasing_1.drop_duplicates()

    df_weeklyPhasing_agg_1 = df_weeklyPhasing_1.groupby([
        'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'PRODUCT_NUMBER', 
        'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'VIEW_BY',
        'FORECAST_YEAR', 'FORECAST_MONTH', 'FORECAST_WEEK', 'PERIOD_TAG'
    ])[['FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME']].sum().reset_index()
    
    df_week_contribution_default = df_master_date.assign(WEIGHT_GT = np.where(df_master_date['DAY'] <= 7, 0.7, 
                                  np.where(df_master_date['DAY'] <= 21, 1, 1.3
                                          )
                                 ) 
            )[
        (df_master_date['WORKING_DAY_DC'] == True)
    ].groupby(['ISO_WEEKNUM', 'YEAR', 'MONTH', 'PERIOD_TAG'])['WEIGHT_GT'].sum().reset_index()

    df_week_contribution_default = df_master_date.assign(WEIGHT_GT = np.where(df_master_date['DAY'] <= 7, 0.7, 
                                  np.where(df_master_date['DAY'] <= 21, 1, 1.3
                                          )
                                 ) 
            )[
        (df_master_date['WORKING_DAY_DC'] == True)
    ].groupby(['ISO_WEEKNUM', 'YEAR', 'MONTH', 'PERIOD_TAG'])['WEIGHT_GT'].sum().reset_index()

    df_week_contribution_default = pd.merge(
        df_week_contribution_default,
        df_week_contribution_default.groupby(
            ['YEAR', 'MONTH']
        ).agg({
            'ISO_WEEKNUM': 'min', 
            'WEIGHT_GT': 'sum'
        }).reset_index().rename(columns = {
            'ISO_WEEKNUM': 'MIN_ISO_WEEKNUM_FULL_MONTH',
            'WEIGHT_GT': 'WEIGHT_GT_FULL_MONTH'
        }),
        on = ['YEAR', 'MONTH']
    )

    df_week_contribution_default['WEEK_CONTRIBUTION_GT'] = df_week_contribution_default['WEIGHT_GT'] / df_week_contribution_default['WEIGHT_GT_FULL_MONTH']  
    df_week_contribution_default['WEEK_CONTRIBUTION_MT'] = np.where( (df_week_contribution_default['ISO_WEEKNUM'] - df_week_contribution_default['MIN_ISO_WEEKNUM_FULL_MONTH']).isin([0,2]), 0.5, 0 )

    df_week_contribution_default[['YEAR', 'MONTH']] = df_week_contribution_default[['YEAR', 'MONTH']].astype('float')

    df_week_contribution_default = df_week_contribution_default[[
        'ISO_WEEKNUM', 'YEAR', 'MONTH', 'PERIOD_TAG', 'WEEK_CONTRIBUTION_GT', 'WEEK_CONTRIBUTION_MT'
    ]].rename(columns = {
        'YEAR': 'FORECAST_YEAR',
        'MONTH': 'FORECAST_MONTH',
        'ISO_WEEKNUM': 'FORECAST_WEEK'
    })

    df_WUF_withDC_2 = pd.merge(
        df_WUF_withDC[
            df_WUF_withDC['WEEK_CONTRIBUTION'].isnull()
        ].drop(['FORECAST_WEEK', 'PERIOD_TAG', 'WEEK_CONTRIBUTION'], axis = 1),
        df_week_contribution_default,
        on = ['FORECAST_YEAR', 'FORECAST_MONTH']   
    )

    # logic mapping: if Supply Stratrgy = null => X mappingItemCode X codeMappingMaster; elif Supply Strategy = 'Plan B' => X codeMappingMaster 
    df_weeklyPhasing_2 = pd.concat([
        pd.merge(
            pd.merge(
                df_mapping_productNumber[df_mapping_productNumber['SUPPLY_STRATEGY'].isnull()].rename(columns = {'REGION_NAME': 'REGION_ITEM_CODE'}),
                df_mappingItemCode,
                on = ['REGION_ITEM_CODE']
            )[['DEMAND_PLANNING_STANDARD_SKU_NAME', 'PRODUCT_NUMBER', 'SUPPLY_STRATEGY', 'CHANNEL', 'REGION_ITEM_CODE', 'DC', 'START_WEEK', 'END_WEEK']],
            df_WUF_withDC_2,
            on = ['DEMAND_PLANNING_STANDARD_SKU_NAME', 'CHANNEL', 'DC']
        ),
        pd.merge(
            df_mapping_productNumber[
                df_mapping_productNumber['SUPPLY_STRATEGY'] == 'Plan B'
            ][['DEMAND_PLANNING_STANDARD_SKU_NAME', 'PRODUCT_NUMBER', 'SUPPLY_STRATEGY', 'CHANNEL', 'REGION_NAME', 'START_WEEK', 'END_WEEK']],
            df_WUF_withDC_2,
            on = ['DEMAND_PLANNING_STANDARD_SKU_NAME', 'CHANNEL', 'REGION_NAME']
        )], 
        ignore_index = True
    )

    # filter period applied for each productNumber
    df_weeklyPhasing_2 = df_weeklyPhasing_2[
        (df_weeklyPhasing_2['FORECAST_YEAR']*100 + df_weeklyPhasing_2['FORECAST_WEEK'] >= df_weeklyPhasing_2['START_WEEK'])
        & (df_weeklyPhasing_2['FORECAST_YEAR']*100 + df_weeklyPhasing_2['FORECAST_WEEK'] <= df_weeklyPhasing_2['END_WEEK'])
    ]

    # calculate forecast value
    df_weeklyPhasing_2['FORECAST_SO_VALUE'] = np.where(
        df_weeklyPhasing_2['CHANNEL'].isin(['MT0-WCM', 'MT0-non WCM']), 
        df_weeklyPhasing_2['SO_VALUE'] * df_weeklyPhasing_2['WEEK_CONTRIBUTION_MT'],
        df_weeklyPhasing_2['SO_VALUE'] * df_weeklyPhasing_2['WEEK_CONTRIBUTION_GT']
    )

    df_weeklyPhasing_2['FORECAST_SO_VOLUME'] = np.where(
        df_weeklyPhasing_2['CHANNEL'].isin(['MT0-WCM', 'MT0-non WCM']), 
        df_weeklyPhasing_2['SO_VOLUME'] * df_weeklyPhasing_2['WEEK_CONTRIBUTION_MT'],
        df_weeklyPhasing_2['SO_VOLUME'] * df_weeklyPhasing_2['WEEK_CONTRIBUTION_GT']
    )

    df_weeklyPhasing_2 = df_weeklyPhasing_2.drop_duplicates()

    df_weeklyPhasing_agg_2 = df_weeklyPhasing_2.groupby([
        'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'PRODUCT_NUMBER', 
        'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'VIEW_BY',
        'FORECAST_YEAR', 'FORECAST_MONTH', 'FORECAST_WEEK', 'PERIOD_TAG'
    ])[['FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME']].sum().reset_index()
    
    df_weeklyPhasing_agg_final = pd.concat([df_weeklyPhasing_agg_1, df_weeklyPhasing_agg_2], ignore_index = True)
    
    df_weeklyPhasing_agg_final = pd.merge(
        df_weeklyPhasing_agg_final,
        df_forecast_week.groupby(
            ['YEAR', 'MONTH']
        )['ISO_WEEKNUM'].min().reset_index().rename(
            columns = {
                'YEAR': 'FORECAST_YEAR',
                'MONTH': 'FORECAST_MONTH',
                'ISO_WEEKNUM': 'FIRST_WEEK_OF_MONTH'
            }
        ),
        on = ['FORECAST_YEAR', 'FORECAST_MONTH']
    )   
    
    df_weeklyPhasing_agg_final['WEEK_INDEX'] = df_weeklyPhasing_agg_final['FORECAST_WEEK'] - df_weeklyPhasing_agg_final['FIRST_WEEK_OF_MONTH'] + 1
    
    df_weeklyPhasing_agg_final = df_weeklyPhasing_agg_final[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'VIEW_BY',
           'FORECAST_YEAR', 'FORECAST_MONTH', 'FORECAST_WEEK', 'WEEK_INDEX', 'PERIOD_TAG',
           'FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME']]

    return df_weeklyPhasing_agg_final

def etl_stock_policy(df_stock_policy, df_master_data):
    df_stock_policy_byProvince_DPName = pd.merge(
        df_stock_policy,
        df_master_data[['PRODUCT_NUMBER', 'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME']],
        left_on = ['Mã Đại diện'],
        right_on = ['PRODUCT_NUMBER']
    )

    df_stock_policy_byProvince_DPName['WEIGHTED_VALUE'] = df_stock_policy_byProvince_DPName['Chính sách tồn kho (Ngày)'] * df_stock_policy_byProvince_DPName['DS Bán ra 60 ngày (Giá NPP)']

    df_stock_policy_byProvince_DPName = df_stock_policy_byProvince_DPName.groupby([
        'DC', 'Thành phố', 'DEMAND_PLANNING_STANDARD_SKU_NAME'
    ])[['WEIGHTED_VALUE', 'DS Bán ra 60 ngày (Giá NPP)']].sum().reset_index()

    df_stock_policy_byProvince_DPName['STOCK_POLICY'] = df_stock_policy_byProvince_DPName['WEIGHTED_VALUE'] / df_stock_policy_byProvince_DPName['DS Bán ra 60 ngày (Giá NPP)'] 

    df_stock_policy_byProvince_DPName = df_stock_policy_byProvince_DPName.rename(
        columns = {
            'Thành phố': 'PROVINCE'
        }
    ).groupby(['PROVINCE', 'DEMAND_PLANNING_STANDARD_SKU_NAME'])['STOCK_POLICY'].mean().reset_index()

    return df_stock_policy_byProvince_DPName

def etl_conversion_si(df_stock_policy_byProvince_DPName, df_manual_stock_policy, df_SO_weekly_last_5w, df_weeklyPhasing_agg, df_mapping_province_DC, df_master_date):
    df_SO_weekly_last_5w['PERIOD_TAG'] = pd.to_datetime(df_SO_weekly_last_5w['PERIOD_TAG']) 
    df_weeklyPhasing_agg['PERIOD_TAG'] = pd.to_datetime(df_weeklyPhasing_agg['PERIOD_TAG']) 
    df_master_date['PERIOD_TAG'] = pd.to_datetime(df_master_date['PERIOD_TAG']) 

    df_SO_weekly_last_5w = df_SO_weekly_last_5w.rename(
        columns = {
            'YEAR': 'FORECAST_YEAR',
            'MONTH': 'FORECAST_MONTH',
            'ISO_WEEK_NUM': 'FORECAST_WEEK',
            'SO_VALUE': 'FORECAST_SO_VALUE',
            'SO_VOLUME': 'FORECAST_SO_VOLUME'
        }
    )
    df_SO_weekly_last_5w[['FORECAST_YEAR', 'FORECAST_MONTH']] = df_SO_weekly_last_5w[['FORECAST_YEAR', 'FORECAST_MONTH']].astype('float')

    df_SO_weekly_last_5w_final = pd.merge(
        df_SO_weekly_last_5w,
        df_mapping_province_DC[['DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME','PROVINCE', 'DC']],
        on = ['DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME', 'PROVINCE']
    )

    # convert to Kcase and Billion VND
    df_SO_weekly_last_5w_final['FORECAST_SO_VOLUME'] = df_SO_weekly_last_5w['FORECAST_SO_VOLUME'] / 1000
    df_SO_weekly_last_5w_final['FORECAST_SO_VALUE'] = df_SO_weekly_last_5w['FORECAST_SO_VALUE'] / 1000000000

    df_weeklyPhasing_final = pd.merge(
        pd.merge(
            pd.merge(
                pd.concat(
                    [
                        df_SO_weekly_last_5w_final.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
                       'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'PERIOD_TAG'])[['FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME']].sum().reset_index(),
                        df_weeklyPhasing_agg.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
                       'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'PERIOD_TAG', 'event_time', 'version', 'note'])[['FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME']].sum().reset_index()
                    ],
                    ignore_index = True
                ),
                df_master_date.assign(
                    SO_DAYS = np.where(df_master_date['WORKING_DAY_DC'] == True, 1, 0)
                ).groupby(['PERIOD_TAG'])['SO_DAYS'].sum().reset_index(),
                on = ['PERIOD_TAG']
            ),
            df_stock_policy_byProvince_DPName,
            how = 'left',
            on = ['PROVINCE', 'DEMAND_PLANNING_STANDARD_SKU_NAME']
        ),
        pd.melt(
            df_manual_stock_policy, 
            id_vars = df_manual_stock_policy.columns[:2],
            value_vars = df_manual_stock_policy.columns[2:],
            var_name = 'REGION_NAME',
            value_name = 'MANUAL_STOCK_POLICY'
        ),
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME']
    )   

    df_weeklyPhasing_final['STOCK_POLICY'] = np.where(
        df_weeklyPhasing_final['STOCK_POLICY'].notnull(),
        df_weeklyPhasing_final['STOCK_POLICY'],
        np.where(
            df_weeklyPhasing_final['MANUAL_STOCK_POLICY'].notnull(),
            df_weeklyPhasing_final['MANUAL_STOCK_POLICY'],
            3
        )
    )

    # Creating Lag features
    def sales_lag(df):
        for i in range(1,6):
            df.sort_values(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'PERIOD_TAG'],inplace=True)
            df["lag_" + str(i)] = df.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC'])['FORECAST_SO_VOLUME'].shift(i)

    # Creating future features
    def sales_future(df):
        for i in range(1,4):
            df.sort_values(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'PERIOD_TAG'],inplace=True)
            df["future_" + str(i)] = df.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC'])['FORECAST_SO_VOLUME'].shift(-i)

    # Creating Lag features
    def so_days_lag(df):
        for i in range(1,6):
            df.sort_values(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'PERIOD_TAG'],inplace=True)
            df["so_lag_" + str(i)] = df.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC'])['SO_DAYS'].shift(i)

    # Creating future features
    def so_days_future(df):
        for i in range(1,4):
            df.sort_values(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'PERIOD_TAG'],inplace=True)
            df["so_future_" + str(i)] = df.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC'])['SO_DAYS'].shift(-i)

    sales_lag(df_weeklyPhasing_final)
    sales_future(df_weeklyPhasing_final)
    so_days_lag(df_weeklyPhasing_final)
    so_days_future(df_weeklyPhasing_final)

    df_weeklyPhasing_final[['FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME', 'lag_1',
           'lag_2', 'lag_3', 'lag_4', 'lag_5', 'future_1', 'future_2', 'future_3']] = df_weeklyPhasing_final[['FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME', 'lag_1',
           'lag_2', 'lag_3', 'lag_4', 'lag_5', 'future_1', 'future_2', 'future_3']].astype('float')

    df_weeklyPhasing_final['STOCK_POLICY'] = df_weeklyPhasing_final['STOCK_POLICY'].fillna(0)

    df_weeklyPhasing_final['CLOSING_STOCK'] = df_weeklyPhasing_final[['lag_1', 'lag_2', 'lag_3', 'lag_4', 'lag_5', 'future_1', 'future_2', 'future_3', 'FORECAST_SO_VOLUME']].sum(axis=1) / df_weeklyPhasing_final[['so_lag_1', 'so_lag_2', 'so_lag_3', 'so_lag_4',
           'so_lag_5', 'so_future_1', 'so_future_2', 'so_future_3', 'SO_DAYS']].sum(axis=1) * df_weeklyPhasing_final['STOCK_POLICY']

    # Creating OPENING STOCK = LAG_1 OF CLOSING_STOCK
    def opening_stock(df):
        df.sort_values(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
       'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'PERIOD_TAG'],inplace=True)
        df["OPENING_STOCK"] = df.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
       'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC'])['CLOSING_STOCK'].shift(1)

    opening_stock(df_weeklyPhasing_final)
    df_weeklyPhasing_final['OPENING_STOCK'] = df_weeklyPhasing_final['OPENING_STOCK'].fillna(0)

    df_weeklyPhasing_final['FORECAST_SI_VOLUME'] = df_weeklyPhasing_final['FORECAST_SO_VOLUME'] + df_weeklyPhasing_final['CLOSING_STOCK'] - df_weeklyPhasing_final['OPENING_STOCK']

    df_weeklyPhasing_final = df_weeklyPhasing_final[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC',
           'PERIOD_TAG', 'FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME', 'STOCK_POLICY',
            'CLOSING_STOCK', 'OPENING_STOCK', 'FORECAST_SI_VOLUME', 'event_time', 'version', 'note']]
    
    return df_weeklyPhasing_final

def etl_conversion_si_simulation(df_stock_policy_byProvince_DPName, df_manual_stock_policy, df_SO_weekly_last_5w, df_weeklyPhasing_agg, df_mapping_province_DC, df_master_date):
    df_SO_weekly_last_5w['PERIOD_TAG'] = pd.to_datetime(df_SO_weekly_last_5w['PERIOD_TAG']) 
    df_weeklyPhasing_agg['PERIOD_TAG'] = pd.to_datetime(df_weeklyPhasing_agg['PERIOD_TAG']) 
    df_master_date['PERIOD_TAG'] = pd.to_datetime(df_master_date['PERIOD_TAG']) 

    df_SO_weekly_last_5w = df_SO_weekly_last_5w.rename(
        columns = {
            'YEAR': 'FORECAST_YEAR',
            'MONTH': 'FORECAST_MONTH',
            'ISO_WEEK_NUM': 'FORECAST_WEEK',
            'SO_VALUE': 'FORECAST_SO_VALUE',
            'SO_VOLUME': 'FORECAST_SO_VOLUME'
        }
    )
    df_SO_weekly_last_5w[['FORECAST_YEAR', 'FORECAST_MONTH']] = df_SO_weekly_last_5w[['FORECAST_YEAR', 'FORECAST_MONTH']].astype('float')

    df_SO_weekly_last_5w_final = pd.merge(
        df_SO_weekly_last_5w,
        df_mapping_province_DC[['DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME','PROVINCE', 'DC']],
        on = ['DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME', 'PROVINCE']
    )

    # convert to Kcase and Billion VND
    df_SO_weekly_last_5w_final['FORECAST_SO_VOLUME'] = df_SO_weekly_last_5w['FORECAST_SO_VOLUME'] / 1000
    df_SO_weekly_last_5w_final['FORECAST_SO_VALUE'] = df_SO_weekly_last_5w['FORECAST_SO_VALUE'] / 1000000000

    df_weeklyPhasing_final = pd.merge(
        pd.merge(
            pd.merge(
                pd.concat(
                    [
                        df_SO_weekly_last_5w_final.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
                       'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'PERIOD_TAG'])[['FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME']].sum().reset_index(),
                        df_weeklyPhasing_agg.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
                       'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'PERIOD_TAG'])[['FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME']].sum().reset_index()
                    ],
                    ignore_index = True
                ),
                df_master_date.assign(
                    SO_DAYS = np.where(df_master_date['WORKING_DAY_DC'] == True, 1, 0)
                ).groupby(['PERIOD_TAG'])['SO_DAYS'].sum().reset_index(),
                on = ['PERIOD_TAG']
            ),
            df_stock_policy_byProvince_DPName,
            how = 'left',
            on = ['PROVINCE', 'DEMAND_PLANNING_STANDARD_SKU_NAME']
        ),
        pd.melt(
            df_manual_stock_policy, 
            id_vars = df_manual_stock_policy.columns[:2],
            value_vars = df_manual_stock_policy.columns[2:],
            var_name = 'REGION_NAME',
            value_name = 'MANUAL_STOCK_POLICY'
        ),
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME']
    )   

    df_weeklyPhasing_final['STOCK_POLICY'] = np.where(
        df_weeklyPhasing_final['STOCK_POLICY'].notnull(),
        df_weeklyPhasing_final['STOCK_POLICY'],
        np.where(
            df_weeklyPhasing_final['MANUAL_STOCK_POLICY'].notnull(),
            df_weeklyPhasing_final['MANUAL_STOCK_POLICY'],
            3
        )
    )

    # Creating Lag features
    def sales_lag(df):
        for i in range(1,6):
            df.sort_values(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'PERIOD_TAG'],inplace=True)
            df["lag_" + str(i)] = df.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC'])['FORECAST_SO_VOLUME'].shift(i)

    # Creating future features
    def sales_future(df):
        for i in range(1,4):
            df.sort_values(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'PERIOD_TAG'],inplace=True)
            df["future_" + str(i)] = df.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC'])['FORECAST_SO_VOLUME'].shift(-i)

    # Creating Lag features
    def so_days_lag(df):
        for i in range(1,6):
            df.sort_values(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'PERIOD_TAG'],inplace=True)
            df["so_lag_" + str(i)] = df.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC'])['SO_DAYS'].shift(i)

    # Creating future features
    def so_days_future(df):
        for i in range(1,4):
            df.sort_values(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'PERIOD_TAG'],inplace=True)
            df["so_future_" + str(i)] = df.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC'])['SO_DAYS'].shift(-i)

    sales_lag(df_weeklyPhasing_final)
    sales_future(df_weeklyPhasing_final)
    so_days_lag(df_weeklyPhasing_final)
    so_days_future(df_weeklyPhasing_final)

    df_weeklyPhasing_final[['FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME', 'lag_1',
           'lag_2', 'lag_3', 'lag_4', 'lag_5', 'future_1', 'future_2', 'future_3']] = df_weeklyPhasing_final[['FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME', 'lag_1',
           'lag_2', 'lag_3', 'lag_4', 'lag_5', 'future_1', 'future_2', 'future_3']].astype('float')

    df_weeklyPhasing_final['STOCK_POLICY'] = df_weeklyPhasing_final['STOCK_POLICY'].fillna(0)

    df_weeklyPhasing_final['CLOSING_STOCK'] = df_weeklyPhasing_final[['lag_1', 'lag_2', 'lag_3', 'lag_4', 'lag_5', 'future_1', 'future_2', 'future_3', 'FORECAST_SO_VOLUME']].sum(axis=1) / df_weeklyPhasing_final[['so_lag_1', 'so_lag_2', 'so_lag_3', 'so_lag_4',
           'so_lag_5', 'so_future_1', 'so_future_2', 'so_future_3', 'SO_DAYS']].sum(axis=1) * df_weeklyPhasing_final['STOCK_POLICY']

    # Creating OPENING STOCK = LAG_1 OF CLOSING_STOCK
    def opening_stock(df):
        df.sort_values(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
       'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC', 'PERIOD_TAG'],inplace=True)
        df["OPENING_STOCK"] = df.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
       'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC'])['CLOSING_STOCK'].shift(1)

    opening_stock(df_weeklyPhasing_final)
    df_weeklyPhasing_final['OPENING_STOCK'] = df_weeklyPhasing_final['OPENING_STOCK'].fillna(0)

    df_weeklyPhasing_final['FORECAST_SI_VOLUME'] = df_weeklyPhasing_final['FORECAST_SO_VOLUME'] + df_weeklyPhasing_final['CLOSING_STOCK'] - df_weeklyPhasing_final['OPENING_STOCK']

    df_weeklyPhasing_final = df_weeklyPhasing_final[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'DC',
           'PERIOD_TAG', 'FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME', 'STOCK_POLICY',
            'CLOSING_STOCK', 'OPENING_STOCK', 'FORECAST_SI_VOLUME']]
    
    return df_weeklyPhasing_final

# muf ios
def etl_muf_ios(df_s1, df_master_data, df_MUF_withDC, df_stock_policy_byProvince_DPName):
    df_muf_s1 = pd.merge(
        df_s1.assign(
            SI_VOLUME = (df_s1['GT.Sell In'].fillna(0) + df_s1['KM.Sell In'].fillna(0) + df_s1['DP.Final Sell Out'].fillna(0) + df_s1['Non-GT.Sell In'].fillna(0)) / 1000,
            SO_VOLUME = (df_s1['GT.Final Sell Out'].fillna(0) + df_s1['KM.Final Sell Out'].fillna(0) + df_s1['DP.Sell In'].fillna(0) + df_s1['Non-GT.Final Sell Out'].fillna(0)) / 1000,
            OPENING_STOCK = (df_s1['GT.Final Opening Stock'].fillna(0) + df_s1['KM.Final Opening Stock'].fillna(0) + df_s1['DP.Final Opening Stock'].fillna(0) + df_s1['Non-GT.Final Opening Stock'].fillna(0)) / 1000,
            CLOSING_STOCK = (df_s1['GT.Final Closing Stock'].fillna(0) + df_s1['KM.Final Closing Stock'].fillna(0) + df_s1['DP.Final Closing Stock'].fillna(0) + df_s1['Non-GT.Final Closing Stock'].fillna(0)) / 1000,
        )[[
            'Region', 'Channel Code', 'Year', 'Month', 'DC', 'Item Code', 
            'SI_VOLUME', 'SO_VOLUME', 'OPENING_STOCK', 'CLOSING_STOCK'
        ]].rename(
            columns = {
                'Region': 'REGION_NAME',
                'Channel Code': 'CHANNEL',
                'Year': 'FORECAST_YEAR',
                'Month': 'FORECAST_MONTH',
                'Item Code': 'PRODUCT_NUMBER'
            }
        ),
        df_master_data[
            (df_master_data['MARKET_TYPE'] == 'Local')
            & (df_master_data['ACTIVE_STATUS'] == 'Active')
        ][[
            'PRODUCT_NUMBER', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'SUB_DIVISION_NAME', 'GROUP_SKU'
        ]].drop_duplicates(),
        on = ['PRODUCT_NUMBER']
    ).groupby([
        'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 
        'REGION_NAME', 'CHANNEL', 'DC', 'FORECAST_YEAR', 'FORECAST_MONTH'
    ])[['SI_VOLUME', 'SO_VOLUME', 'OPENING_STOCK', 'CLOSING_STOCK']].sum().reset_index()

    df_muf_s2 = pd.merge(
        df_MUF_withDC[
            df_MUF_withDC['FORECAST_YEAR']*100 + df_MUF_withDC['FORECAST_MONTH'] > ( df_s1['Year']*100 + df_s1['Month'] ).max()
        ].groupby([
            'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 
            'REGION_NAME', 'PROVINCE', 'CHANNEL', 'DC', 'FORECAST_YEAR', 'FORECAST_MONTH'
        ])[['SO_VALUE', 'SO_VOLUME']].sum().reset_index(),
        df_stock_policy_byProvince_DPName,
        how = 'left', 
        on = ['PROVINCE', 'DEMAND_PLANNING_STANDARD_SKU_NAME']
    )

    df_muf_s2['STOCK_POLICY'] = np.where(
        df_muf_s2['STOCK_POLICY'].notnull(),
        df_muf_s2['STOCK_POLICY'],
        3
    )

    df_muf_s2['CLOSING_STOCK'] = df_muf_s2['SO_VOLUME'] / 30 * df_muf_s2['STOCK_POLICY']

    df_muf_s2 = df_muf_s2.groupby([
            'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 
            'REGION_NAME', 'CHANNEL', 'DC', 'FORECAST_YEAR', 'FORECAST_MONTH'
        ])[['SO_VALUE', 'SO_VOLUME', 'CLOSING_STOCK']].sum().reset_index()

    df_muf_ios = pd.concat(
        [
        df_muf_s1,
        df_muf_s2
        ],
        ignore_index = True
    )

    # Creating OPENING STOCK = LAG_1 OF CLOSING_STOCK
    def opening_stock(df):
        df.sort_values(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
       'REGION_NAME', 'CHANNEL', 'DC', 'FORECAST_YEAR', 'FORECAST_MONTH'],inplace=True)
        df["OPENING_STOCK_2"] = df.groupby(['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
       'REGION_NAME', 'CHANNEL', 'DC'])['CLOSING_STOCK'].shift(1)

        df['OPENING_STOCK'] = np.where(
            df['OPENING_STOCK'].notnull(), 
            df['OPENING_STOCK'],
            df['OPENING_STOCK_2']
        )

        df['OPENING_STOCK'] = df['OPENING_STOCK'].fillna(0)

    opening_stock(df_muf_ios)

    df_muf_ios['SI_VOLUME'] = np.where(
            df_muf_ios['SI_VOLUME'].notnull(), 
            df_muf_ios['SI_VOLUME'],
            df_muf_ios['SO_VOLUME'] + df_muf_ios['CLOSING_STOCK'] - df_muf_ios['OPENING_STOCK'] 
        )

    df_muf_ios = df_muf_ios[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'REGION_NAME', 'CHANNEL', 'DC', 'FORECAST_YEAR', 'FORECAST_MONTH',
           'SI_VOLUME', 'SO_VOLUME', 'OPENING_STOCK', 'CLOSING_STOCK']]
    
    return df_muf_ios

def etl_muf_adjustment(df_province_contribution_inRegion_byDPName, df_province_contribution_inRegion_byGroupSKU, df_province_contribution_inRegion_bySubDivision, df_province_contribution_inRegion_byDefault, df_muf_adjustment, df_default_groupSKU, df_manual_groupSKU, df_price_list_month, df_muf):
    # adjustment
    df_muf_adjustment_final = pd.merge(
        pd.merge(
            df_muf_adjustment,
            df_default_groupSKU,
            how = 'left',
            left_on = ['GROUP_SKU', 'CHANNEL', 'REGION_NAME'],
            right_on = ['Default Group SKU.Group SKU', 'Default Group SKU.Channel', 'Default Group SKU.Input Region Name']
        ),
        df_manual_groupSKU,
        how = 'left',
        left_on = ['GROUP_SKU', 'CHANNEL', 'REGION_NAME', 'MONTH'],
        right_on = ['Manual Group SKU.Group SKU', 'Manual Group SKU.Channel', 'Manual Group SKU.Input Region Name', 'Manual Group SKU.Month']
    ) 

    # add columns 
    df_muf_adjustment_final['DP Contribution'] = np.where(df_muf_adjustment_final['Manual Group SKU.Contribution'].notnull(), 
                                                     df_muf_adjustment_final['Manual Group SKU.Contribution'], 
                                                     df_muf_adjustment_final['Default Group SKU.Contribution']
                                                    )

    df_muf_adjustment_final['DP Name'] = np.where(df_muf_adjustment_final['Manual Group SKU.Demand Planning Standard SKU Name'].notnull(), 
                                                     df_muf_adjustment_final['Manual Group SKU.Demand Planning Standard SKU Name'], 
                                                     df_muf_adjustment_final['Default Group SKU.Demand Planning Standard SKU Name']
                                                    )

    df_muf_adjustment_final['Region Final'] = np.where(df_muf_adjustment_final['Manual Group SKU.Region Name'].notnull(), 
                                                     df_muf_adjustment_final['Manual Group SKU.Region Name'], 
                                                     df_muf_adjustment_final['Default Group SKU.Region Name']
                                                    )

    df_muf_adjustment_final['DEMAND_PLANNING_STANDARD_SKU_NAME'] = np.where(df_muf_adjustment_final['DEMAND_PLANNING_STANDARD_SKU_NAME'].notnull(), 
                                                     df_muf_adjustment_final['DEMAND_PLANNING_STANDARD_SKU_NAME'], 
                                                     df_muf_adjustment_final['DP Name']
                                                    )

    df_muf_adjustment_final['REGION_NAME'] = np.where(df_muf_adjustment_final['REGION_NAME'].notnull() & (df_muf_adjustment_final['REGION_NAME'] != 'NW'), 
                                                     df_muf_adjustment_final['REGION_NAME'], 
                                                     df_muf_adjustment_final['Region Final']
                                                    )

    df_muf_adjustment_final['ADJUSTMENT'] = np.where(df_muf_adjustment_final['DP Contribution'].notnull(), 
                                                     df_muf_adjustment_final['ADJUSTMENT'] * df_muf_adjustment_final['DP Contribution'], 
                                                     df_muf_adjustment_final['ADJUSTMENT']
                                                    )


    df_muf_adjustment_final = df_muf_adjustment_final[
        df_muf_adjustment_final['ADJUSTMENT'] != 0 
    ][[
        'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'SUB_DIVISION_NAME',
        'SITE', 'CHANNEL', 'REGION_NAME', 'PROVINCE', 'UPLIFT_TYPE', 'UOM',
        'MONTH', 'YEAR', 'NOTE', 'ADJUSTMENT']]

    df_muf_adjustment_final1 = pd.merge(
        df_muf_adjustment_final,
        df_province_contribution_inRegion_byDPName,
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME', 'CHANNEL'],
        suffixes = ['', '_byDPName']
    )

    df_muf_adjustment_final_byDPname = df_muf_adjustment_final1[
        df_muf_adjustment_final1['PROVINCE'].notnull()
    ]

    df_muf_adjustment_final2 = pd.merge(
        df_muf_adjustment_final1[
            df_muf_adjustment_final1['PROVINCE'].isnull()
        ],
        df_province_contribution_inRegion_byGroupSKU,
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'REGION_NAME', 'CHANNEL'],
        suffixes = ['', '_byGroupSKU']
    )    

    df_muf_adjustment_final_byGroupSKU = df_muf_adjustment_final2[
        df_muf_adjustment_final2['PROVINCE_byGroupSKU'].notnull()
    ]

    df_muf_adjustment_final3 = pd.merge(
        df_muf_adjustment_final2[
            df_muf_adjustment_final2['PROVINCE_byGroupSKU'].isnull()
        ],
        df_province_contribution_inRegion_bySubDivision,
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'REGION_NAME', 'CHANNEL'],
        suffixes = ['', '_bySubDivision']
    )

    df_muf_adjustment_final_bySubDivision = df_muf_adjustment_final3[
        df_muf_adjustment_final3['PROVINCE_bySubDivision'].notnull()
    ]

    df_muf_adjustment_final4 = pd.merge(
        df_muf_adjustment_final3[
            df_muf_adjustment_final3['PROVINCE_bySubDivision'].isnull()
        ],
        df_province_contribution_inRegion_byDefault,
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'REGION_NAME'],
        suffixes = ['', '_byDefault']
    )

    df_muf_adjustment_final_byDefault = df_muf_adjustment_final4[
        df_muf_adjustment_final4['PROVINCE_byDefault'].notnull()
    ]

    df_muf_adjustment_final_byDPname = df_muf_adjustment_final_byDPname.rename(columns = {'Contribution': 'PROVINCE_Contribution'})[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'REGION_NAME', 'PROVINCE', 'CHANNEL', 'UPLIFT_TYPE', 'YEAR', 'MONTH', 'ADJUSTMENT','PROVINCE_Contribution']].drop_duplicates()

    df_muf_adjustment_final_byGroupSKU['PROVINCE'] = df_muf_adjustment_final_byGroupSKU['PROVINCE_byGroupSKU'] 
    df_muf_adjustment_final_byGroupSKU['PROVINCE_Contribution'] = df_muf_adjustment_final_byGroupSKU['Contribution_byGroupSKU'] 
    df_muf_adjustment_final_byGroupSKU = df_muf_adjustment_final_byGroupSKU[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'REGION_NAME', 'PROVINCE', 'CHANNEL', 'UPLIFT_TYPE', 'YEAR', 'MONTH', 'ADJUSTMENT','PROVINCE_Contribution']].drop_duplicates()

    df_muf_adjustment_final_bySubDivision['PROVINCE'] = df_muf_adjustment_final_bySubDivision['PROVINCE_bySubDivision'] 
    df_muf_adjustment_final_bySubDivision['PROVINCE_Contribution'] = df_muf_adjustment_final_bySubDivision['Contribution_bySubDivision'] 
    df_muf_adjustment_final_bySubDivision = df_muf_adjustment_final_bySubDivision[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'REGION_NAME', 'PROVINCE', 'CHANNEL', 'UPLIFT_TYPE', 'YEAR', 'MONTH', 'ADJUSTMENT','PROVINCE_Contribution']].drop_duplicates()

    df_muf_adjustment_final_byDefault['PROVINCE'] = df_muf_adjustment_final_byDefault['PROVINCE_byDefault'] 
    df_muf_adjustment_final_byDefault['PROVINCE_Contribution'] = df_muf_adjustment_final_byDefault['Contribution_byDefault'] 
    df_muf_adjustment_final_byDefault = df_muf_adjustment_final_byDefault[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'REGION_NAME', 'PROVINCE', 'CHANNEL', 'UPLIFT_TYPE', 'YEAR', 'MONTH', 'ADJUSTMENT','PROVINCE_Contribution']].drop_duplicates()

    df_muf_adjustment_final = pd.concat([
        df_muf_adjustment_final_byDPname,
        df_muf_adjustment_final_byGroupSKU,
        df_muf_adjustment_final_bySubDivision,
        df_muf_adjustment_final_byDefault
    ], ignore_index = True)

    df_muf_adjustment_final['ADJUSTMENT'] = np.where(
        df_muf_adjustment_final['PROVINCE_Contribution'].notnull(),
        df_muf_adjustment_final['ADJUSTMENT'] * df_muf_adjustment_final['PROVINCE_Contribution'],
        df_muf_adjustment_final['ADJUSTMENT']
    )

    df_muf_adjustment_final = pd.merge(
        df_muf_adjustment_final[[
            'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
            'REGION_NAME', 'PROVINCE', 'CHANNEL', 'UPLIFT_TYPE', 'YEAR', 'MONTH', 'ADJUSTMENT'
        ]],
        df_price_list_month.rename(
            columns = {
                'Sub Division Name': 'SUB_DIVISION_NAME',
                'Group SKU': 'GROUP_SKU',
                'DPName': 'DEMAND_PLANNING_STANDARD_SKU_NAME',
                'Channel': 'CHANNEL',
                'Region': 'REGION_NAME'
            }
        ),
        on = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME', 'CHANNEL', 'YEAR', 'MONTH']
    )

    # input val
    df_muf_adjustment_val = df_muf_adjustment_final[df_muf_adjustment_final['UPLIFT_TYPE'] == 'Val']
    # calculate Sell Out - Vol (Kcase) / Val (Bio)
    df_muf_adjustment_val['SO_VALUE'] = df_muf_adjustment_val['ADJUSTMENT']
    df_muf_adjustment_val['SO_VOLUME'] = df_muf_adjustment_val['SO_VALUE'] * 1000000 / np.maximum(df_muf_adjustment_val['Price'], 1e-8) #to handle the zero division error

    # input vol
    df_muf_adjustment_vol = df_muf_adjustment_final[df_muf_adjustment_final['UPLIFT_TYPE'] == 'Vol']
    # calculate Sell Out - Vol (Kcase) / Val (Bio)
    df_muf_adjustment_vol['SO_VOLUME'] = df_muf_adjustment_vol['ADJUSTMENT']
    df_muf_adjustment_vol['SO_VALUE'] =  df_muf_adjustment_vol['ADJUSTMENT'] * df_muf_adjustment_vol['Price'] / 1000000

    # step 2: combine df_muf_adjustment_val & df_muf_adjustment_vol to create df_SO_uplift
    df_muf_adjustment_uplift = pd.concat([df_muf_adjustment_val, df_muf_adjustment_vol], ignore_index = False)

    df_muf_adjustment_uplift[['SO_VOLUME', 'SO_VALUE']] = df_muf_adjustment_uplift[['SO_VOLUME', 'SO_VALUE']].fillna(0)

    df_muf_adjustment_uplift = df_muf_adjustment_uplift[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'REGION_NAME', 'PROVINCE', 'CHANNEL', 'YEAR', 'MONTH', 'SO_VALUE', 'SO_VOLUME']].rename(
        columns = {
            'YEAR': 'FORECAST_YEAR',
            'MONTH': 'FORECAST_MONTH'
        }
    )

    df_muf_adjustment_uplift['BUILDING_BLOCKS'] = 'SOE-ADJUSTMENT'
    df_muf_adjustment_uplift['VIEW_BY'] = 'MUF'

    df_muf_adjustment_uplift_final = pd.concat(
        [
            df_muf_adjustment_uplift,
            df_muf
        ],
        ignore_index = True
    )
    
    return df_muf_adjustment_uplift_final

def etl_actualization(df_muf, df_past_innovation, df_forecast_week):
    df_actualization_base = df_muf[
        df_muf['BUILDING_BLOCKS'].isin(['BASELINE', 'BASELINE_ADJ', 'SEASONALITY'])
    ]

    df_actualization_innovation = pd.concat([
        df_muf[
            (df_muf['BUILDING_BLOCKS'] == 'Innovation')
            & (df_muf['FORECAST_YEAR'] * 100 + df_muf['FORECAST_MONTH'] >= ( df_forecast_week['YEAR'] * 100 + df_forecast_week['MONTH'] ).min())
        ],
        df_past_innovation.assign(
            BUILDING_BLOCKS = 'Innovation',
            VIEW_BY = 'MUF',
            FORECAST_YEAR = df_past_innovation['YEAR'].astype('float'),
            FORECAST_MONTH = df_past_innovation['MONTH'].astype('float'),
            SO_VALUE = df_past_innovation['SO_VALUE'] / 1000000000,
            SO_VOLUME = df_past_innovation['SO_QTY'] / 1000
        )[df_muf.columns]
        ],
        ignore_index = True
    )

    df_actualization_trade_past = pd.merge(
        df_muf[
            df_muf['BUILDING_BLOCKS'].isin(['BASELINE', 'BASELINE_ADJ', 'SEASONALITY'])
        ].groupby(
            ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
               'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH']
        )[['SO_VALUE', 'SO_VOLUME']].sum().reset_index().rename(
            columns = {
                'SO_VALUE': 'SO_VALUE_BASE',
                'SO_VOLUME': 'SO_VOLUME_BASE'
            }
        ),
        df_past_innovation.assign(
            BUILDING_BLOCKS = 'Innovation',
            VIEW_BY = 'MUF',
            FORECAST_YEAR = df_past_innovation['YEAR'].astype('float'),
            FORECAST_MONTH = df_past_innovation['MONTH'].astype('float'),
            SO_VALUE = df_past_innovation['SO_VALUE'] / 1000000000,
            SO_VOLUME = df_past_innovation['SO_QTY'] / 1000
        )[df_muf.columns].groupby(
            ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
               'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH']
        )[['SO_VALUE', 'SO_VOLUME']].sum().reset_index().rename(
            columns = {
                'SO_VALUE': 'SO_VALUE_INNOVATION',
                'SO_VOLUME': 'SO_VOLUME_INNOVATION'
            }
        ),
        how = 'left', 
        on = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
               'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH']
    )

    df_actualization_trade_past[['SO_VALUE_INNOVATION', 'SO_VOLUME_INNOVATION']] = df_actualization_trade_past[['SO_VALUE_INNOVATION', 'SO_VOLUME_INNOVATION']].fillna(0)
    df_actualization_trade_past['SO_VALUE'] = df_actualization_trade_past['SO_VALUE_BASE'] - df_actualization_trade_past['SO_VALUE_INNOVATION'] 
    df_actualization_trade_past['SO_VOLUME'] = df_actualization_trade_past['SO_VOLUME_BASE'] - df_actualization_trade_past['SO_VOLUME_INNOVATION'] 

    df_actualization_trade = pd.concat([
        df_muf[
            (df_muf['BUILDING_BLOCKS'] == 'Trade Activities')
            & (df_muf['FORECAST_YEAR'] * 100 + df_muf['FORECAST_MONTH'] >= ( df_master_date['YEAR'] * 100 + df_master_date['MONTH'] ).min())
        ],
        df_actualization_trade_past.assign(
            BUILDING_BLOCKS = 'Trade Activities',
            VIEW_BY = 'MUF'
        )[df_muf.columns]
        ],
        ignore_index = True
    )

    df_actualization_others = df_muf[
        (~ df_muf['BUILDING_BLOCKS'].isin(['BASELINE', 'BASELINE_ADJ', 'SEASONALITY', 'Innovation', 'Trade Activities']) )
        & (df_muf['FORECAST_YEAR'] * 100 + df_muf['FORECAST_MONTH'] >= ( df_forecast_week['YEAR'] * 100 + df_forecast_week['MONTH'] ).min())
    ]

    df_actualization = pd.concat([
        df_actualization_base,
        df_actualization_innovation,
        df_actualization_trade,
        df_actualization_others
        ],
        ignore_index = True
    )
    
    return df_actualization

def etl_soe(df_SO_weekly_last_5w, df_mapping_province_DC, df_weeklyPhasing_agg, df_stock_weekly_last_5w):
    df_SO_weekly_last_5w['PERIOD_TAG'] = pd.to_datetime(df_SO_weekly_last_5w['PERIOD_TAG'])
    df_stock_weekly_last_5w['PERIOD_TAG'] = pd.to_datetime(df_stock_weekly_last_5w['PERIOD_TAG'])
    
    df_soe_test = pd.merge(
            pd.merge(
            df_SO_weekly_last_5w,
            df_mapping_province_DC[['DEMAND_PLANNING_STANDARD_SKU_NAME', 'PROVINCE', 'DC']],
            on = ['DEMAND_PLANNING_STANDARD_SKU_NAME', 'PROVINCE']
        ).groupby(
            ['SUB_DIVISION_NAME', 'GROUP_SKU', 
            'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'DC',
            'YEAR', 'MONTH', 'ISO_WEEK_NUM', 'PERIOD_TAG']
        )[['SO_VOLUME', 'SO_VALUE']].sum().reset_index().rename(
                columns = {
                    'YEAR': 'FORECAST_YEAR', 
                    'MONTH': 'FORECAST_MONTH',
                    'ISO_WEEK_NUM': 'FORECAST_WEEK'           
                }
            ),
        df_weeklyPhasing_agg[
            df_weeklyPhasing_agg['PERIOD_TAG'].isin(df_SO_weekly_last_5w['PERIOD_TAG'].unique())
        ].assign(
            FORECAST_SO_VALUE = df_weeklyPhasing_agg['FORECAST_SO_VALUE'] * (10 ** 9),
            FORECAST_SO_VOLUME = df_weeklyPhasing_agg['FORECAST_SO_VOLUME'] * (10 ** 3),
            PRICE = df_weeklyPhasing_agg['FORECAST_SO_VALUE'] * (10 ** 6) / ( df_weeklyPhasing_agg['FORECAST_SO_VOLUME'] * (10 ** 3) )
        ).groupby(
            # agg by period_tag
            [
                # 'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'DC', 'FORECAST_YEAR', 'FORECAST_MONTH',
           'FORECAST_WEEK', 'WEEK_INDEX', 'PERIOD_TAG']
        )[['FORECAST_SO_VALUE', 'FORECAST_SO_VOLUME']].sum().reset_index(),
        how = 'left', 
        on = [
            # 'SUB_DIVISION_NAME', 'GROUP_SKU',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'DC', 
           'FORECAST_YEAR', 'FORECAST_MONTH', 'FORECAST_WEEK', 'PERIOD_TAG']
        )

    # df_soe_test = pd.merge(
    #     df_soe_test,
    #     df_forecast_week.groupby(
    #         ['YEAR', 'MONTH']
    #     )['ISO_WEEKNUM'].min().reset_index().rename(
    #         columns = {
    #             'YEAR': 'FORECAST_YEAR',
    #             'MONTH': 'FORECAST_MONTH',
    #             'ISO_WEEKNUM': 'FIRST_WEEK_OF_MONTH'
    #         }
    #     ),
    #     on = ['FORECAST_YEAR', 'FORECAST_MONTH']
    # )

    # df_soe_test['WEEK_INDEX'] = df_soe_test['FORECAST_WEEK'] - df_soe_test['FIRST_WEEK_OF_MONTH'] + 1

    df_soe_final = df_soe_test.groupby(
        ['SUB_DIVISION_NAME', 'GROUP_SKU', 'PRODUCT_NUMBER', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'CHANNEL', 'REGION_NAME', 'DC', 'FORECAST_YEAR', 'FORECAST_MONTH']
    ).agg({
        'PERIOD_TAG': ['min', 'max'],
        'WEEK_INDEX': 'max',
        'SO_VOLUME': 'sum',
        'FORECAST_SO_VOLUME': 'sum'
    }).reset_index()

    df_soe_final.columns = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'PRODUCT_NUMBER', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'CHANNEL', 'REGION_NAME', 'DC', 'FORECAST_YEAR', 'FORECAST_MONTH', 'FIRST_PERIOD_TAG', 'LAST_PERIOD_TAG', 'WEEK_INDEX', 'MTD_ACTUAL_SO_VOLUME', 'MTD_FORECAST_SO_VOLUME']

    df_soe_final[['MTD_ACTUAL_SO_VOLUME', 'MTD_FORECAST_SO_VOLUME']] = df_soe_final[['MTD_ACTUAL_SO_VOLUME', 'MTD_FORECAST_SO_VOLUME']].astype('float')

    df_soe_final['GAP_MTD_SO_VOLUME'] = df_soe_final['MTD_ACTUAL_SO_VOLUME'] - df_soe_final['MTD_FORECAST_SO_VOLUME'] 

    df_soe_final = pd.merge(
        pd.merge(
            pd.merge(
                df_soe_final,
                df_stock_weekly_last_5w.groupby(
                    # agg by period_tag, product_number
                    [
                        # 'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
                   'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'DC', 
                    'PERIOD_TAG']
                )[['OPENING_STOCK']].sum().reset_index().rename(
                    columns = {
                        'PERIOD_TAG': 'FIRST_PERIOD_TAG',
                        'OPENING_STOCK': 'FIRST_PERIOD_OPENING_STOCK'
                    }
                ),
                how = 'left',
                on = [
                    # 'SUB_DIVISION_NAME', 'GROUP_SKU',
                   'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'DC', 'FIRST_PERIOD_TAG']
            ),
            df_stock_weekly_last_5w.groupby(
                # agg by period_tag, product_number
                [
                    # 'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
               'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'DC', 
                'PERIOD_TAG']
            )[['OPENING_STOCK']].sum().reset_index().rename(
                columns = {
                    'PERIOD_TAG': 'LAST_PERIOD_TAG',
                    'OPENING_STOCK': 'LAST_PERIOD_OPENING_STOCK'
                }
            ),
            how = 'left',
            on = [
                # 'SUB_DIVISION_NAME', 'GROUP_SKU',
               'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'DC', 'LAST_PERIOD_TAG']
        ),
        df_stock_monthly_last_2m.groupby(
            # agg by period_tag, product_number
            [
                # 'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'DC', 
            'REPORT_YEAR', 'REPORT_MONTH']
        )[['OPENING_STOCK']].sum().reset_index().rename(
            columns = {
                'REPORT_YEAR': 'FORECAST_YEAR',
                'REPORT_MONTH': 'FORECAST_MONTH',
                'OPENING_STOCK': 'MONTH_OPENING_STOCK'
            }
        ),
        how = 'left', 
        on = ['PRODUCT_NUMBER', 'CHANNEL', 'REGION_NAME', 'DC', 
            'FORECAST_YEAR', 'FORECAST_MONTH']
    )

    df_soe_final['MTD_ACTUAL_SO_VOLUME'] = df_soe_final['MTD_ACTUAL_SO_VOLUME'] / 1000 
    df_soe_final['MTD_FORECAST_SO_VOLUME'] = df_soe_final['MTD_FORECAST_SO_VOLUME'] / 1000 
    df_soe_final['GAP_MTD_SO_VOLUME'] = df_soe_final['GAP_MTD_SO_VOLUME'] / 1000 

    df_soe_final['FIRST_PERIOD_OPENING_STOCK'] = df_soe_final['FIRST_PERIOD_OPENING_STOCK'] / 1000 
    df_soe_final['LAST_PERIOD_OPENING_STOCK'] = df_soe_final['LAST_PERIOD_OPENING_STOCK'] / 1000 
    df_soe_final['MONTH_OPENING_STOCK'] = df_soe_final['MONTH_OPENING_STOCK'] / 1000 
    
    return df_soe_final

def etl_soe_adjustment(df_soe_adjustment, df_price_list_month, df_weeklyPhasing_agg, df_province_contribution_inRegion_byDPName, df_province_contribution_inRegion_byGroupSKU, df_province_contribution_inRegion_bySubDivision, df_province_contribution_inRegion_byDefault):
    df_soe_adjustment = pd.merge(
        df_soe_adjustment,
        df_price_list_month.rename(
            columns = {
             'Sub Division Name': 'SUB_DIVISION_NAME', 
             'Group SKU': 'GROUP_SKU',
             'DPName': 'DEMAND_PLANNING_STANDARD_SKU_NAME',
             'Channel': 'CHANNEL',
             'Region': 'REGION_NAME',
             'MONTH': 'FORECAST_MONTH', 
             'YEAR': 'FORECAST_YEAR',
             'Price': 'PRICE'   
            }
        ), 
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'CHANNEL', 'REGION_NAME', 'FORECAST_YEAR', 'FORECAST_MONTH']
    )

    # unpivot 
    df_soe_adjustment_melted = pd.melt(
        df_soe_adjustment[[
            'SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'PRODUCT_NUMBER',
            'CHANNEL', 'REGION_NAME', 'UOM', 'PRICE', 'FORECAST_YEAR', 'FORECAST_MONTH',
            'W1_ADJUST', 'W2_ADJUST', 'W3_ADJUST', 'W4_ADJUST', 'W5_ADJUST', 'W6_ADJUST'
        ]].rename(columns = {
            'W1_ADJUST': 1, 
            'W2_ADJUST': 2, 
            'W3_ADJUST': 3, 
            'W4_ADJUST': 4, 
            'W5_ADJUST': 5, 
            'W6_ADJUST': 6
        }), 
        id_vars=['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'PRODUCT_NUMBER',
        'CHANNEL', 'REGION_NAME', 'UOM', 'PRICE', 'FORECAST_YEAR', 'FORECAST_MONTH'], 
        value_vars=[1,2,3,4,5,6]
    ).rename(
        columns = {
            'variable': 'WEEK_INDEX'
        }
    )

    df_soe_adjustment_melted['value'] = pd.to_numeric(df_soe_adjustment_melted['value'], errors='coerce').fillna(0)

    # VAL
    df_soe_adjustment_val = df_soe_adjustment_melted[
            df_soe_adjustment_melted['UOM'] == 'Val (Bio)'
        ].rename(
        columns = {
            'value': 'SO_VALUE_ADJUST'
        }
    )

    df_soe_adjustment_val['SO_VOLUME_ADJUST'] = df_soe_adjustment_val['SO_VALUE_ADJUST'] * 1000000 / np.maximum(df_soe_adjustment_val['PRICE'], 1e-8) #to handle the zero division error

    # VOL 
    df_soe_adjustment_vol = df_soe_adjustment_melted[
            df_soe_adjustment_melted['UOM'] == 'Vol (K.cases)'
        ].rename(
        columns = {
            'value': 'SO_VOLUME_ADJUST'
        }
    )

    df_soe_adjustment_vol['SO_VALUE_ADJUST'] = df_soe_adjustment_vol['SO_VOLUME_ADJUST'] * df_soe_adjustment_vol['PRICE'] / 1000000 

    df_soe_adjustment_final = pd.concat([
        df_soe_adjustment_val,
        df_soe_adjustment_vol
    ],
    ignore_index = True)

    df_soe_adjustment_final[['SO_VALUE_ADJUST', 'SO_VOLUME_ADJUST']] = df_soe_adjustment_final[['SO_VALUE_ADJUST', 'SO_VOLUME_ADJUST']].fillna(0)

    df_soe_adjustment_final1 = pd.merge(
        df_soe_adjustment_final,
        df_province_contribution_inRegion_byDPName,
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME', 'REGION_NAME', 'CHANNEL'],
        suffixes = ['', '_byDPName']
    )

    df_soe_adjustment_final_byDPname = df_soe_adjustment_final1[
        df_soe_adjustment_final1['PROVINCE'].notnull()
    ]

    df_soe_adjustment_final2 = pd.merge(
        df_soe_adjustment_final1[
            df_soe_adjustment_final1['PROVINCE'].isnull()
        ],
        df_province_contribution_inRegion_byGroupSKU,
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'GROUP_SKU', 'REGION_NAME', 'CHANNEL'],
        suffixes = ['', '_byGroupSKU']
    )    

    df_soe_adjustment_final_byGroupSKU = df_soe_adjustment_final2[
        df_soe_adjustment_final2['PROVINCE_byGroupSKU'].notnull()
    ]

    df_soe_adjustment_final3 = pd.merge(
        df_soe_adjustment_final2[
            df_soe_adjustment_final2['PROVINCE_byGroupSKU'].isnull()
        ],
        df_province_contribution_inRegion_bySubDivision,
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'REGION_NAME', 'CHANNEL'],
        suffixes = ['', '_bySubDivision']
    )

    df_soe_adjustment_final_bySubDivision = df_soe_adjustment_final3[
        df_soe_adjustment_final3['PROVINCE_bySubDivision'].notnull()
    ]

    df_soe_adjustment_final4 = pd.merge(
        df_soe_adjustment_final3[
            df_soe_adjustment_final3['PROVINCE_bySubDivision'].isnull()
        ],
        df_province_contribution_inRegion_byDefault,
        how = 'left',
        on = ['SUB_DIVISION_NAME', 'REGION_NAME'],
        suffixes = ['', '_byDefault']
    )

    df_soe_adjustment_final_byDefault = df_soe_adjustment_final4[
        df_soe_adjustment_final4['PROVINCE_byDefault'].notnull()
    ]

    df_soe_adjustment_final_byDPname = df_soe_adjustment_final_byDPname.rename(columns = {'Contribution': 'PROVINCE_Contribution'})[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH', 'WEEK_INDEX', 'SO_VALUE_ADJUST', 'SO_VOLUME_ADJUST', 'PROVINCE_Contribution']].drop_duplicates()

    df_soe_adjustment_final_byGroupSKU['PROVINCE'] = df_soe_adjustment_final_byGroupSKU['PROVINCE_byGroupSKU'] 
    df_soe_adjustment_final_byGroupSKU['PROVINCE_Contribution'] = df_soe_adjustment_final_byGroupSKU['Contribution_byGroupSKU'] 
    df_soe_adjustment_final_byGroupSKU = df_soe_adjustment_final_byGroupSKU[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH', 'WEEK_INDEX', 'SO_VALUE_ADJUST', 'SO_VOLUME_ADJUST', 'PROVINCE_Contribution']].drop_duplicates()

    df_soe_adjustment_final_bySubDivision['PROVINCE'] = df_soe_adjustment_final_bySubDivision['PROVINCE_bySubDivision'] 
    df_soe_adjustment_final_bySubDivision['PROVINCE_Contribution'] = df_soe_adjustment_final_bySubDivision['Contribution_bySubDivision'] 
    df_soe_adjustment_final_bySubDivision = df_soe_adjustment_final_bySubDivision[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH', 'WEEK_INDEX', 'SO_VALUE_ADJUST', 'SO_VOLUME_ADJUST', 'PROVINCE_Contribution']].drop_duplicates()

    df_soe_adjustment_final_byDefault['PROVINCE'] = df_soe_adjustment_final_byDefault['PROVINCE_byDefault'] 
    df_soe_adjustment_final_byDefault['PROVINCE_Contribution'] = df_soe_adjustment_final_byDefault['Contribution_byDefault'] 
    df_soe_adjustment_final_byDefault = df_soe_adjustment_final_byDefault[['SUB_DIVISION_NAME', 'GROUP_SKU', 'DEMAND_PLANNING_STANDARD_SKU_NAME',
           'PRODUCT_NUMBER', 'REGION_NAME', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH', 'WEEK_INDEX', 'SO_VALUE_ADJUST', 'SO_VOLUME_ADJUST', 'PROVINCE_Contribution']].drop_duplicates()

    df_soe_adjustment_final = pd.concat([
        df_soe_adjustment_final_byDPname,
        df_soe_adjustment_final_byGroupSKU,
        df_soe_adjustment_final_bySubDivision,
        df_soe_adjustment_final_byDefault
    ], ignore_index = True)

    df_soe_adjustment_final['SO_VALUE_ADJUST'] = df_soe_adjustment_final['SO_VALUE_ADJUST'] * df_soe_adjustment_final['PROVINCE_Contribution'] 
    df_soe_adjustment_final['SO_VOLUME_ADJUST'] = df_soe_adjustment_final['SO_VOLUME_ADJUST'] * df_soe_adjustment_final['PROVINCE_Contribution'] 

    df_weeklyPhasing_soe_adjustment = pd.merge(
        df_weeklyPhasing_agg,
        df_soe_adjustment_final[[
            'PRODUCT_NUMBER', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH', 'WEEK_INDEX', 'SO_VALUE_ADJUST', 'SO_VOLUME_ADJUST'
        ]],
        how = 'left', 
        on = ['PRODUCT_NUMBER', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH', 'WEEK_INDEX']
    )

    df_weeklyPhasing_soe_adjustment[['SO_VALUE_ADJUST', 'SO_VOLUME_ADJUST']] = df_weeklyPhasing_soe_adjustment[['SO_VALUE_ADJUST', 'SO_VOLUME_ADJUST']].fillna(0)

    df_weeklyPhasing_soe_adjustment['FORECAST_SO_VALUE'] = df_weeklyPhasing_soe_adjustment['FORECAST_SO_VALUE'] + df_weeklyPhasing_soe_adjustment['SO_VALUE_ADJUST']
    df_weeklyPhasing_soe_adjustment['FORECAST_SO_VOLUME'] = df_weeklyPhasing_soe_adjustment['FORECAST_SO_VOLUME'] + df_weeklyPhasing_soe_adjustment['SO_VOLUME_ADJUST']
    df_weeklyPhasing_soe_adjustment = df_weeklyPhasing_soe_adjustment[df_weeklyPhasing_agg.columns]

    df_weeklyPhasing_soe_adjustment = pd.merge(
        df_weeklyPhasing_agg,
        df_soe_adjustment_final[[
            'PRODUCT_NUMBER', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH', 'WEEK_INDEX', 'SO_VALUE_ADJUST', 'SO_VOLUME_ADJUST'
        ]],
        how = 'left', 
        on = ['PRODUCT_NUMBER', 'PROVINCE', 'CHANNEL', 'FORECAST_YEAR', 'FORECAST_MONTH', 'WEEK_INDEX']
    )

    df_weeklyPhasing_soe_adjustment[['SO_VALUE_ADJUST', 'SO_VOLUME_ADJUST']] = df_weeklyPhasing_soe_adjustment[['SO_VALUE_ADJUST', 'SO_VOLUME_ADJUST']].fillna(0)

    df_weeklyPhasing_soe_adjustment['FORECAST_SO_VALUE'] = df_weeklyPhasing_soe_adjustment['FORECAST_SO_VALUE'] + df_weeklyPhasing_soe_adjustment['SO_VALUE_ADJUST']
    df_weeklyPhasing_soe_adjustment['FORECAST_SO_VOLUME'] = df_weeklyPhasing_soe_adjustment['FORECAST_SO_VOLUME'] + df_weeklyPhasing_soe_adjustment['SO_VOLUME_ADJUST']
    df_weeklyPhasing_soe_adjustment = df_weeklyPhasing_soe_adjustment[df_weeklyPhasing_agg.columns]
    
    return df_weeklyPhasing_soe_adjustment

# Initialize the tkinter GUI
root = tk.Tk()
root.title('MCH DEMAND PLANNING FORECAST - SOP')
root.geometry("1600x900")  # Set the root dimensions
root.pack_propagate(False)  # Tells the root to not let the widgets inside it determine its size

# Initialize empty DataFrames
df_price_list = pd.DataFrame()
df_master_data = pd.DataFrame()
df_muf_input = pd.DataFrame()
df_contribution = pd.DataFrame()
df_region_contribution = pd.DataFrame()
df_mapping_province_DC = pd.DataFrame()
df_codeMappingMaster = pd.DataFrame()
df_mappingItemCode = pd.DataFrame() 
df_stock_policy = pd.DataFrame()

df_manual_stock_policy = pd.DataFrame()
df_s1 = pd.DataFrame()
df_muf_adjustment = pd.DataFrame()
df_soe_adjustment = pd.DataFrame()

# # COLLECT RAW
# print('COLLECTING DATA')
# df_baseline, df_master_calendar, df_groupSKU_master, df_groupSKU_byProvince, df_master_date, df_forecast_week, df_week, df_RR_by_PROVINCE_DPNAME, df_SO_weekly_last_5w, df_stock_weekly_last_5w, df_stock_monthly_last_2m, df_past_innovation  = etl_collect_data(client)

# # CALCULATE BASELINE
# df_baseline_forecast = etl_baseline(df_baseline, df_master_calendar)

# # CALCULATE CONTRIBUTION BY PROVINCE
# df_province_contribution_inRegion_byDPName = province_contribution_byDPName(df_groupSKU_byProvince)
# df_province_contribution_inRegion_byGroupSKU = province_contribution_byGroupSKU(df_groupSKU_byProvince)
# df_province_contribution_inRegion_bySubDivision = province_contribution_bySubDivision(df_groupSKU_byProvince)
# df_province_contribution_inRegion_byDefault = province_contribution_byDefault(df_groupSKU_byProvince)

# PRINT STATUS
print('SUCCESS')

# df_baseline = pd.DataFrame()
# df_baseline_forecast = pd.DataFrame()
# df_master_calendar = pd.DataFrame()
# df_groupSKU_master = pd.DataFrame()
# df_groupSKU_byProvince = pd.DataFrame()
# df_master_date = pd.DataFrame()
# df_forecast_week = pd.DataFrame()
# df_RR_by_PROVINCE_DPNAME = pd.DataFrame()
# df_SO_weekly_last_5w = pd.DataFrame()
    
df_price_list_month = pd.DataFrame()
df_contribution_melted = pd.DataFrame()
df_region_contribution_melted = pd.DataFrame()
df_manual_groupSKU = pd.DataFrame()
df_default_groupSKU = pd.DataFrame()
df_muf_input_melted = pd.DataFrame()
df_stock_policy_byProvince_DPName = pd.DataFrame()

df_muf = pd.DataFrame()
df_MUF_withDC = pd.DataFrame()
df_weeklyPhasing_agg = pd.DataFrame()
df_actualization = pd.DataFrame()
df_muf_adjustment_final = pd.DataFrame()
df_muf_ios = pd.DataFrame()

df_muf_final = pd.DataFrame()
df_weeklyPhasing_agg = pd.DataFrame()
df_weeklyPhasing_final = pd.DataFrame()

df_latest_weeklyPhasing = pd.DataFrame()
df_soe = pd.DataFrame()
df_weeklyPhasing_soe_adjustment = pd.DataFrame()
df_weeklyPhasing_soe_adjustment_final = pd.DataFrame()

# Dictionary to store the file paths for each tab
upload_file_paths = {
# MANUAL UPDATE DATA
    'df_price_list': [None, df_price_list],
    'df_master_data': [None, df_master_data],
# MANUAL MUF INPUT
    'df_muf_input': [None, df_muf_input],
# MANUAL CONTRIBUTION
    'df_contribution': [None, df_contribution],
    'df_region_contribution': [None, df_region_contribution],
# MANUAL MAPPING
    'df_mapping_province_DC': [None, df_mapping_province_DC],
    'df_codeMappingMaster': [None, df_codeMappingMaster],
    'df_mappingItemCode': [None, df_mappingItemCode],
# STOCK POLICY
    'df_stock_policy': [None, df_stock_policy],
    'df_manual_stock_policy': [None, df_manual_stock_policy],
    'df_s1': [None, df_s1],
    'df_muf_adjustment': [None, df_muf_adjustment],
    'df_soe_adjustment': [None, df_soe_adjustment],
}

# Dictionary to store the output data for each tab
processed_input_data = {
    'df_price_list_month': df_price_list_month, 
    'df_contribution_melted': df_contribution_melted,
    'df_region_contribution_melted': df_region_contribution_melted,
    'df_manual_groupSKU': df_manual_groupSKU,
    'df_default_groupSKU': df_default_groupSKU,
    'df_muf_input_melted': df_muf_input_melted,
    'df_stock_policy_byProvince_DPName': df_stock_policy_byProvince_DPName
}

output_data = {
    'df_muf': [None, df_muf],
    'df_actualization': [None, df_actualization],
    'df_muf_adjustment_final': [None, df_muf_adjustment_final],
    'df_MUF_withDC': [None, df_MUF_withDC],
    'df_muf_ios': [None, df_muf_ios], 
    # 'df_weeklyPhasing_agg': [None, df_weeklyPhasing_agg],   
}

soe_output = {
    'df_latest_weeklyPhasing': df_latest_weeklyPhasing,
    'df_soe': df_soe, 
    'df_weeklyPhasing_soe_adjustment': df_weeklyPhasing_soe_adjustment, 
    'df_weeklyPhasing_soe_adjustment_final': df_weeklyPhasing_soe_adjustment_final
}

final_output_data = {
    'df_muf_final': df_muf_final,
    'df_weeklyPhasing_agg': df_weeklyPhasing_agg,      
    'df_weeklyPhasing_final': df_weeklyPhasing_final,      
} 

# Dictionary to store the database data for each tab
database_data = {
    'df_baseline': df_baseline,
    'df_baseline_forecast': df_baseline_forecast,
    'df_master_calendar': df_master_calendar,
    'df_groupSKU_master': df_groupSKU_master,
    'df_groupSKU_byProvince': df_groupSKU_byProvince,
    'df_master_date': df_master_date,
    'df_forecast_week': df_forecast_week,
    'df_week': df_week,
    'df_RR_by_PROVINCE_DPNAME': df_RR_by_PROVINCE_DPNAME,
    'df_SO_weekly_last_5w': df_SO_weekly_last_5w,
    'df_stock_weekly_last_5w': df_stock_weekly_last_5w,
    'df_stock_monthly_last_2m': df_stock_monthly_last_2m, 
    'df_past_innovation': df_past_innovation, 
    'df_province_contribution_inRegion_byDPName': df_province_contribution_inRegion_byDPName, 
    'df_province_contribution_inRegion_byGroupSKU': df_province_contribution_inRegion_byGroupSKU, 
    'df_province_contribution_inRegion_bySubDivision': df_province_contribution_inRegion_bySubDivision, 
    'df_province_contribution_inRegion_byDefault': df_province_contribution_inRegion_byDefault, 
}

def process_input_data():
    try:
        df_price_list_month = etl_price(upload_file_paths['df_price_list'][1], database_data['df_forecast_week'])
        # clean & transform input
        df_contribution_melted, df_region_contribution_melted = etl_clean_transform_contribution_input(upload_file_paths['df_contribution'][1], upload_file_paths['df_region_contribution'][1])
        # contribution manual & default
        df_manual_groupSKU = etl_manual_groupSKU(df_contribution_melted, df_region_contribution_melted, database_data['df_groupSKU_master'])
        df_default_groupSKU = etl_default_groupSKU(database_data['df_groupSKU_master'], df_contribution_melted)
        # muf input
        df_muf_input_melted = etl_clean_transform_muf_input(upload_file_paths['df_muf_input'][1])
        df_stock_policy_byProvince_DPName = etl_stock_policy(upload_file_paths['df_stock_policy'][1], upload_file_paths['df_master_data'][1])
        
        # assign value in the dictionary
        processed_input_data['df_price_list_month'] = df_price_list_month.copy()
        processed_input_data['df_contribution_melted'] = df_contribution_melted.copy()
        processed_input_data['df_region_contribution_melted'] = df_region_contribution_melted.copy()
        processed_input_data['df_manual_groupSKU'] = df_manual_groupSKU.copy()
        processed_input_data['df_default_groupSKU'] = df_default_groupSKU.copy()
        processed_input_data['df_muf_input_melted'] = df_muf_input_melted.copy()
        processed_input_data['df_stock_policy_byProvince_DPName'] = df_stock_policy_byProvince_DPName.copy()
        
        messagebox.showinfo("Success", "Successfully Clean and Transform input data.")
    except:
        messagebox.showerror("Error", "No file is selected")
    
def simulation(file_key):
    try:
        if file_key == 'df_muf':
            df = etl_muf(processed_input_data['df_muf_input_melted'], processed_input_data['df_default_groupSKU'], processed_input_data['df_manual_groupSKU'], database_data['df_baseline_forecast'], processed_input_data['df_price_list_month'], upload_file_paths['df_master_data'][1], database_data['df_province_contribution_inRegion_byDPName'], database_data['df_province_contribution_inRegion_byGroupSKU'], database_data['df_province_contribution_inRegion_bySubDivision'], database_data['df_province_contribution_inRegion_byDefault'])
        if file_key == 'df_actualization':
            df = etl_actualization(output_data['df_muf'][1], database_data['df_past_innovation'], database_data['df_forecast_week'])     
        if file_key == 'df_muf_adjustment_final':
            df = etl_muf_adjustment(database_data['df_province_contribution_inRegion_byDPName'], database_data['df_province_contribution_inRegion_byGroupSKU'], database_data['df_province_contribution_inRegion_bySubDivision'], database_data['df_province_contribution_inRegion_byDefault'], upload_file_paths['df_muf_adjustment'][1], processed_input_data['df_default_groupSKU'], processed_input_data['df_manual_groupSKU'], processed_input_data['df_price_list_month'], output_data['df_muf'][1])       
        if file_key == 'df_MUF_withDC':
            if output_data['df_actualization'][1] is True:
                df = etl_MUF_withDC(output_data['df_muf_adjustment_final'][1], upload_file_paths['df_mapping_province_DC'][1])
            else:
                df = etl_MUF_withDC(output_data['df_muf'][1], upload_file_paths['df_mapping_province_DC'][1])
        if file_key == 'df_muf_ios':
            df = etl_muf_ios(upload_file_paths['df_s1'][1], upload_file_paths['df_master_data'][1], output_data['df_MUF_withDC'][1], processed_input_data['df_stock_policy_byProvince_DPName'])
        # if file_key == 'df_weeklyPhasing_agg':
        #     df = etl_weeklyPhasing_simulation(output_data['df_MUF_withDC'][1], database_data['df_RR_by_PROVINCE_DPNAME'], upload_file_paths['df_master_data'][1], upload_file_paths['df_codeMappingMaster'][1], upload_file_paths['df_mappingItemCode'][1], database_data['df_master_date'], database_data['df_forecast_week'])
        # assign value in the dictionary
        output_data[file_key][1] = df.copy()

        clear_data_output(file_key)
        output_treeview = output_treeviews[file_key]
        output_treeview["column"] = list(df.columns)
        output_treeview["show"] = "headings"
        for column in output_treeview["columns"]:
            output_treeview.heading(column, text=column)

        df_rows = df.head(50).to_numpy().tolist()
        for row in df_rows:
            output_treeview.insert("", "end", values=row)
        messagebox.showinfo("Success", "Successfully simulate forecast.")
    except:
        messagebox.showerror("Error", "Missing or bad input")

# Function to browse and select a file for a specific tab
def browse_file(file_key):
    filename = filedialog.askopenfilename(
        initialdir="/",
        title="Select A File",
        filetype=(("All Files", "*.*"), ("xlsx files", "*.xlsx"))
    )
    if filename:
        if file_key in list(upload_file_paths.keys()):
            upload_file_paths[file_key][0] = filename
            label_file = tab_labels[file_key]
            label_file["text"] = filename
        if file_key in list(output_data.keys()):
            output_data[file_key][0] = filename
            label_file = output_labels[file_key]
            label_file["text"] = filename
            
        # for file_key in upload_file_paths:
        #     upload_file_paths[file_key][0] = filename
        #     label_file = tab_labels[file_key]
        #     label_file["text"] = filename
        # for file_key in output_data:
        #     output_data[file_key][0] = filename
        #     label_file = output_labels[file_key]
        #     label_file["text"] = filename
            
# Function to load the file data into the Treeview for a specific tab
def load_excel_data(file_key):
    file_path = upload_file_paths[file_key][0]
    if file_path:
        try:
            excel_filename = r"{}".format(file_path)
            if excel_filename[-4:] == ".csv":
                df = pd.read_csv(excel_filename)
            else:
                df = pd.read_excel(excel_filename, sheet_name=file_key)
                
            upload_file_paths[file_key][1] = df.copy()
            clear_data(file_key)
            treeview = tab_treeviews[file_key]
            treeview["column"] = list(df.columns)
            treeview["show"] = "headings"
            for column in treeview["columns"]:
                treeview.heading(column, text=column)

            df_rows = df.head(50).to_numpy().tolist()
            for row in df_rows:
                treeview.insert("", "end", values=row)
            messagebox.showinfo("Success", "Successfully load/refresh data.")
        except ValueError:
            messagebox.showerror("Information", "The file you have chosen is invalid")
        except FileNotFoundError:
            messagebox.showerror("Information", f"No such file as {file_path}")
    else:
        messagebox.showwarning("Information", "No file selected")

        
# Create a Tab widget to hold the tabs
tab_widget = ttk.Notebook(root)
tab_widget.pack(fill='both', expand=True)

# Create a parent tab called "DATABASE"
database_tab = ttk.Frame(tab_widget)
tab_widget.add(database_tab, text="DATABASE")

# Create a Notebook widget to hold the tabs within the "DATABASE" tab
database_tab_widget = ttk.Notebook(database_tab)
database_tab_widget.pack(fill='both', expand=True)

# Create subtabs for each DataFrame inside the "DATABASE" tab
for file_key, value in database_data.items():
    # Create a subtab for each DataFrame
    subtab_frame = ttk.Frame(database_tab_widget)
    database_tab_widget.add(subtab_frame, text=file_key)

    # Frame for TreeView in the subtab
    file_frame = tk.LabelFrame(subtab_frame, text="Database Data")
    file_frame.place(height=500, width=1800)
    
    # Function to handle export button click
    def export_file_database(file_key):
        file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
        if file_path:
            dataframe = database_data[file_key]
            dataframe.to_csv(file_path, index=False, encoding='utf-8-sig')
            messagebox.showinfo("Export Successful", "File exported successfully.")

    # Frame for export data dialog in each tab
    export_frame = tk.LabelFrame(subtab_frame, text="Action")
    export_frame.place(height=100, width=400, rely=0.6, relx=0)
    
    # Create the export button in the subtab
    export_button = ttk.Button(export_frame, text="Export data", command=lambda key=file_key: export_file_database(key))
    export_button.place(rely=0.65, relx=0)

    # Treeview Widget in the subtab
    tv = ttk.Treeview(file_frame)
    tv.place(relheight=1, relwidth=1)

    treescrolly = tk.Scrollbar(file_frame, orient="vertical", command=tv.yview)
    treescrollx = tk.Scrollbar(file_frame, orient="horizontal", command=tv.xview)
    tv.configure(xscrollcommand=treescrollx.set, yscrollcommand=treescrolly.set)
    treescrollx.pack(side="bottom", fill="x")
    treescrolly.pack(side="right", fill="y")

    # Display the DataFrame in the subtab
    tv["column"] = list(value.columns)
    tv["show"] = "headings"
    for column in tv["columns"]:
        tv.heading(column, text=column)

    df_rows = value.head(50).to_numpy().tolist()
    for row in df_rows:
        tv.insert("", "end", values=row)
        
    # Create a label for the license statement
    license_label = tk.Label(subtab_frame, text="This app is developed by MASAN GROUP and is intended solely for use by members within the organization. Unauthorized access or use is strictly prohibited.", font=("Arial", 12))
    license_label.pack(anchor="center", side = 'bottom')
        
# Create a parent tab called "MANUAL INPUT"
upload_tab = ttk.Frame(tab_widget)
tab_widget.add(upload_tab, text="MANUAL INPUT")

# Create a Notebook widget to hold the tabs within the "Upload" tab
upload_tab_widget = ttk.Notebook(upload_tab)
upload_tab_widget.pack(fill='both', expand=True)

# Function to clear the data in the Treeview for a specific tab
def clear_data(file_key):
    treeview = tab_treeviews[file_key]
    treeview.delete(*treeview.get_children())

tab_labels = {}
tab_treeviews = {}
for file_key in upload_file_paths:
    tab_frame = ttk.Frame(upload_tab_widget)
    upload_tab_widget.add(tab_frame, text=file_key)

    # Frame for TreeView in the subtab
    upload_frame = tk.LabelFrame(tab_frame, text="Uploaded Data")
    upload_frame.place(height=500, width=1800)

    # Frame for open file dialog in each tab
    file_frame = tk.LabelFrame(tab_frame, text="Action")
    file_frame.place(height=100, width=400, rely=0.6, relx=0)

    # Button to browse file in each tab
    browse_button = tk.Button(file_frame, text="Browse A File", command=lambda key=file_key: browse_file(key))
    browse_button.place(rely=0.65, relx=0)

    # Button to load file data in each tab
    load_button = tk.Button(file_frame, text="Load/Refresh", command=lambda key=file_key: load_excel_data(key))
    load_button.place(rely=0.65, relx=0.3)
    
    # Button to process input data
    processed_input_button = ttk.Button(file_frame, text="Processed Input Data", command=process_input_data)
    processed_input_button.place(rely=0.65, relx=0.6)

    # Label to display selected file in each tab
    label_file = ttk.Label(file_frame, text="No File Selected")
    label_file.place(rely=0, relx=0)

    tab_labels[file_key] = label_file

    # Treeview widget in each tab
    tab_treeview = ttk.Treeview(upload_frame)
    tab_treeview.place(relheight=1, relwidth=1)
    treescrolly = tk.Scrollbar(upload_frame, orient="vertical", command=tab_treeview.yview)
    treescrollx = tk.Scrollbar(upload_frame, orient="horizontal", command=tab_treeview.xview)
    tab_treeview.configure(xscrollcommand=treescrollx.set, yscrollcommand=treescrolly.set)
    treescrollx.pack(side="bottom", fill="x")
    treescrolly.pack(side="right", fill="y")

    tab_treeviews[file_key] = tab_treeview
    
    # Create a label for the license statement
    license_label = tk.Label(tab_frame, text="This app is developed by MASAN GROUP and is intended solely for use by members within the organization. Unauthorized access or use is strictly prohibited.", font=("Arial", 12))
    license_label.pack(anchor="center", side = 'bottom')
    
# Function to clear the data in the Treeview for a specific tab
def clear_data_output(file_key):
    output_treeview = output_treeviews[file_key]
    output_treeview.delete(*output_treeview.get_children())

# Create other parental tab called SIMULATION
simulation_tab = ttk.Frame(tab_widget)
tab_widget.add(simulation_tab, text="FORECAST SIMULATION")

# Create a Notebook widget to hold the output tabs within the "Simulation" tab
output_tab_widget = ttk.Notebook(simulation_tab)
output_tab_widget.pack(fill='both', expand=True)

output_labels = {}
output_treeviews = {}
# Create output tabs for each output DataFrame inside the "Simulation" tab
for file_key in output_data.keys():
    output_tab = ttk.Frame(output_tab_widget)
    output_tab_widget.add(output_tab, text=file_key)
    
    # Frame for TreeView in the subtab
    output_frame = tk.LabelFrame(output_tab, text="Forecast Result")
    output_frame.place(height=500, width=1800)

    # Frame for simulation file dialog in each tab
    file_frame = tk.LabelFrame(output_tab, text="Action & Note")
    file_frame.place(height=200, width=600, rely=0.6, relx=0)

    # # Button to process input data
    # processed_input_button = ttk.Button(file_frame, text="Processed Input Data", command=process_input_data)
    # processed_input_button.place(rely=0.65, relx=0)

    # Button to simulate in each tab
    simulation_button = tk.Button(file_frame, text="Simulation", command=lambda key=file_key: simulation(key))
    simulation_button.place(rely=0.65, relx=0)
    
    # Function to handle export button click
    def export_file_simulation(file_key):
        file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
        if file_path:
            dataframe = output_data[file_key][1]
            dataframe.to_csv(file_path, index=False, encoding='utf-8-sig')
            messagebox.showinfo("Export Successful", "File exported successfully.")
    
    # Create the export button in the subtab
    export_button = ttk.Button(file_frame, text="Export data", command=lambda key=file_key: export_file_simulation(key))
    export_button.place(rely=0.65, relx=0.15)
    
    # Function to handle export button click
    def write_to_existing_excel_simulation(file_key):
        file_path = output_data[file_key][0]
        dataframe = output_data[file_key][1]
        with pd.ExcelWriter(file_path, engine='openpyxl', mode='a') as writer:
            # Write the new DataFrame to a new sheet
            dataframe.to_excel(writer, sheet_name=file_key, index=False)        
            messagebox.showinfo("Export Successful", "File exported successfully.")
    
    # Create the export button in the subtab
    write_button = ttk.Button(file_frame, text="Write to existing file", command=lambda key=file_key: write_to_existing_excel_simulation(key))
    write_button.place(rely=0.65, relx=0.3)
    
    # Label to display selected file in each tab
    label_file = ttk.Label(file_frame, text="No File Selected")
    label_file.place(rely=0, relx=0)

    output_labels[file_key] = label_file
    
    # Input form
    if file_key == 'df_muf':
        # Text input for note
        inputtxt_df_muf = tk.Text(file_frame, 
                       height = 3, 
                       width = 20) 
        inputtxt_df_muf.place(rely=0.25, relx=0.5, anchor='center')
    if file_key == 'df_MUF_withDC':
        # Text input for note
        inputtxt_df_MUF_withDC = tk.Text(file_frame, 
                       height = 3, 
                       width = 20) 
        inputtxt_df_MUF_withDC.place(rely=0.25, relx=0.5, anchor='center')
    if file_key == 'df_muf_adjustment_final':
        # Text input for note
        inputtxt_df_muf_adjustment_final = tk.Text(file_frame, 
                       height = 3, 
                       width = 20) 
        inputtxt_df_muf_adjustment_final.place(rely=0.25, relx=0.5, anchor='center')
    
    def upload_to_bigquery(file_key, client):
        try:
            if file_key == 'df_muf':
                inp = inputtxt_df_muf.get(1.0, "end-1c")   # Retrieve the note text when the button is clicked
                table_id = 'mch-dwh-409503.MCH_Output.MUF_REVIEW'
            if file_key == 'df_MUF_withDC':
                inp = inputtxt_df_MUF_withDC.get(1.0, "end-1c")   
                table_id = 'mch-dwh-409503.MCH_Output.MUF_WITH_DC_REVIEW'
            if file_key == 'df_muf_adjustment_final':
                inp = inputtxt_df_muf_adjustment_final.get(1.0, "end-1c")   
                table_id = 'mch-dwh-409503.MCH_Output.MUF_REVIEW'
            # assign value for upload review version on BigQuery
            version = 'REVIEW'
            event_time = datetime.now()

            df = output_data[file_key][1].copy()
            df['event_time'] = event_time
            df['version'] = version
            df['note'] = event_time.strftime("%Y/%m/%d") + ' - ' + inp

            # upload to bigquery
            client.load_table_from_dataframe(
                df, table_id
            )
            messagebox.showinfo("Success", "Successfully upload to BigQuery Database.")
        except:
            messagebox.showerror("Error", "Failed to upload to BigQuery Database")

    # Button to upload results to BigQuery
    upload_button = ttk.Button(file_frame, text="Upload to BigQuery",
                               command=lambda key=file_key: upload_to_bigquery(key, client))
    upload_button.place(rely=0.65, relx=0.52)
    
    # Treeview widget in each tab
    output_treeview = ttk.Treeview(output_frame)
    output_treeview.place(relheight=1, relwidth=1)
    treescrolly = tk.Scrollbar(output_frame, orient="vertical", command=output_treeview.yview)
    treescrollx = tk.Scrollbar(output_frame, orient="horizontal", command=output_treeview.xview)
    output_treeview.configure(xscrollcommand=treescrollx.set, yscrollcommand=treescrolly.set)
    treescrollx.pack(side="bottom", fill="x")
    treescrolly.pack(side="right", fill="y")

    output_treeviews[file_key] = output_treeview
    
    # Create a label for the license statement
    license_label = tk.Label(output_tab, text="This app is developed by MASAN GROUP and is intended solely for use by members within the organization. Unauthorized access or use is strictly prohibited.", font=("Arial", 12))
    license_label.pack(anchor="center", side = 'bottom')
    
# Function to clear the final data in the Treeview for a specific tab
def clear_data_final_output(file_key):
    final_output_treeview = final_output_treeviews[file_key]
    final_output_treeview.delete(*final_output_treeview.get_children())
    
# Create other parental tab called REVIEWER
reviewer_tab = ttk.Frame(tab_widget)
tab_widget.add(reviewer_tab, text="REVIEWER")

# Create a Notebook widget to hold the tabs within the "Upload" tab
reviewer_tab_widget = ttk.Notebook(reviewer_tab)
reviewer_tab_widget.pack(fill='both', expand=True)

final_output_treeviews = {}
for file_key in final_output_data:
    tab_frame = ttk.Frame(reviewer_tab_widget)
    reviewer_tab_widget.add(tab_frame, text=file_key)
    
    # Frame for TreeView in the subtab
    review_frame = tk.LabelFrame(tab_frame, text="Review Data")
    review_frame.place(height=500, width=1800)
    
    if file_key in ['df_muf_final']:
        text_file_frame = "Choose the version"
        height_file_frame = 500
        width_file_frame = 600
    else: 
        text_file_frame = "Action"    
        height_file_frame = 100
        width_file_frame = 400
    
    # Frame for open file dialog in each tab
    file_frame = tk.LabelFrame(tab_frame, text=text_file_frame)
    file_frame.place(height=height_file_frame, width=width_file_frame, rely=0.6, relx=0)
    
    def upload_to_bigquery_final(file_key, client):
        try:
            if file_key == 'df_muf_final':
                table_id = 'mch-dwh-409503.MCH_Output.MUF_FINAL'
            if file_key == 'df_weeklyPhasing_agg':
                table_id = 'mch-dwh-409503.MCH_Output.WEEKLY_PHASING_FINAL'
            if file_key == 'df_weeklyPhasing_final':
                table_id = 'mch-dwh-409503.MCH_Output.SELL_IN_SELL_OUT_FORECAST_FINAL'

            # upload to bigquery
            client.load_table_from_dataframe(
                final_output_data[file_key], table_id
            )
            messagebox.showinfo("Success", "Successfully upload to BigQuery Database.")
        except:
            messagebox.showerror("Error", "Failed to upload to BigQuery Database")
            
    if file_key in ['df_muf_final']:
        # Create a list of division names
        divisions = [
            "Beer", "Convenience Foods", "Cooking Noodle",
            "Nutrition", "Tea", "Coffee",
            "Home Care", "Personal Care", "Seasoning",
            "Processed Meats", "Ready Meals", "Refreshment Drinks",
            "Rice Base"
        ]
        for i, division in enumerate(divisions):
            if i < 7:
                row_id = i 
                col_id = 0
            else: 
                row_id = i - 7
                col_id = 2
            label = tk.Label(file_frame, text=division)
            label.grid(row=row_id, column=col_id, padx=10, pady=10)

            entry_df_muf_final = tk.Entry(file_frame)
            entry_df_muf_final.grid(row=row_id, column=col_id + 1, padx=10, pady=10)

            # Store the entry widget in a variable corresponding to the division
            if division == "Beer":
                beer_entry_df_muf_final = entry_df_muf_final
            elif division == "Convenience Foods":
                convenience_foods_entry_df_muf_final = entry_df_muf_final
            elif division == "Cooking Noodle":
                cooking_noodle_entry_df_muf_final = entry_df_muf_final
            elif division == "Nutrition":
                nutrition_entry_df_muf_final = entry_df_muf_final
            elif division == "Tea":
                tea_entry_df_muf_final = entry_df_muf_final
            elif division == "Coffee":
                coffee_entry_df_muf_final = entry_df_muf_final
            elif division == "Home Care":
                home_care_entry_df_muf_final = entry_df_muf_final
            elif division == "Personal Care":
                personal_care_entry_df_muf_final = entry_df_muf_final
            elif division == "Seasoning":
                seasoning_entry_df_muf_final = entry_df_muf_final
            elif division == "Processed Meats":
                processed_meats_entry_df_muf_final = entry_df_muf_final
            elif division == "Ready Meals":
                ready_meals_entry_df_muf_final = entry_df_muf_final
            elif division == "Refreshment Drinks":
                refreshment_drinks_entry_df_muf_final = entry_df_muf_final
            elif division == "Rice Base":
                rice_base_entry_df_muf_final = entry_df_muf_final

        def submit_button_click(file_key, client):
            if file_key in ['df_muf_final']:
                # Retrieve the values from the input boxes
                beer_version = beer_entry_df_muf_final.get()
                convenience_foods_version = convenience_foods_entry_df_muf_final.get()
                cooking_noodle_version = cooking_noodle_entry_df_muf_final.get()
                nutrition_version = nutrition_entry_df_muf_final.get()
                tea_version = tea_entry_df_muf_final.get()
                coffee_version = coffee_entry_df_muf_final.get()
                home_care_version = home_care_entry_df_muf_final.get()
                personal_care_version = personal_care_entry_df_muf_final.get()
                seasoning_version = seasoning_entry_df_muf_final.get()
                processed_meats_version = processed_meats_entry_df_muf_final.get()
                ready_meals_version = ready_meals_entry_df_muf_final.get()
                refreshment_drinks_version = refreshment_drinks_entry_df_muf_final.get()
                rice_base_version = rice_base_entry_df_muf_final.get()

                query = f'''
                SELECT 
                *
                FROM `mch-dwh-409503.MCH_Output.MUF_REVIEW` 
                where version = 'REVIEW'
                and (
                (Sub_Division_Name = 'Beer' and note = '{beer_version}')
                or (Sub_Division_Name = 'Convenience Foods' and note = '{convenience_foods_version}')
                or (Sub_Division_Name = 'Cooking Noodle' and note = '{cooking_noodle_version}')
                or (Sub_Division_Name = 'Nutrition' and note = '{nutrition_version}')
                or (Sub_Division_Name = 'Tea' and note = '{tea_version}')
                or (Sub_Division_Name = 'Coffee' and note = '{coffee_version}')
                or (Sub_Division_Name = 'Home Care' and note = '{home_care_version}')
                or (Sub_Division_Name = 'Personal Care' and note = '{personal_care_version}')
                or (Sub_Division_Name = 'Seasoning' and note = '{seasoning_version}')
                or (Sub_Division_Name = 'Processed Meats' and note = '{processed_meats_version}')
                or (Sub_Division_Name = 'Ready Meals' and note = '{ready_meals_version}')
                or (Sub_Division_Name = 'Refreshment Drinks' and note = '{refreshment_drinks_version}')
                or (Sub_Division_Name = 'Rice base' and note = '{rice_base_version}')
                ) 
                '''

                # Print the values for testing
                print("Beer Version:", beer_version)
                print("Convenience Foods Version:", convenience_foods_version)
                print("Cooking Noodle Version:", cooking_noodle_version)
                print("Nutrition Version:", nutrition_version)
                print("Tea Version:", tea_version)
                print("Coffee Version:", coffee_version)
                print("Home Care Version:", home_care_version)
                print("Personal Care Version:", personal_care_version)
                print("Seasoning Version:", seasoning_version)
                print("Processed Meats Version:", processed_meats_version)
                print("Ready Meals Version:", ready_meals_version)
                print("Refreshment Drinks Version:", refreshment_drinks_version)
                print("Rice Base Version:", rice_base_version)

                print(query)

                df = client.query(query).to_dataframe()

                version = 'FINAL'
                event_time = datetime.now()
                df['event_time'] = event_time
                df['version'] = version

                # assign value in the dictionary
                final_output_data[file_key] = df.copy()

                clear_data_final_output(file_key)
                final_output_treeview = final_output_treeviews[file_key]
                final_output_treeview["column"] = list(df.columns)
                final_output_treeview["show"] = "headings"
                for column in final_output_treeview["columns"]:
                    final_output_treeview.heading(column, text=column)

                df_rows = df.head(50).to_numpy().tolist()
                for row in df_rows:
                    final_output_treeview.insert("", "end", values=row)
                    
            if file_key == 'df_weeklyPhasing_agg':
                df_MUF_with_DC = etl_MUF_withDC(final_output_data['df_muf_final'], upload_file_paths['df_mapping_province_DC'][1])
                df = etl_weeklyPhasing(df_MUF_with_DC, database_data['df_RR_by_PROVINCE_DPNAME'], upload_file_paths['df_master_data'][1], upload_file_paths['df_codeMappingMaster'][1], upload_file_paths['df_mappingItemCode'][1], database_data['df_master_date'], database_data['df_forecast_week'])

                version = 'FINAL'
                event_time = datetime.now()
                df['event_time'] = event_time
                df['version'] = version
                
                # assign value in the dictionary
                final_output_data[file_key] = df.copy()

                clear_data_final_output(file_key)
                final_output_treeview = final_output_treeviews[file_key]
                final_output_treeview["column"] = list(df.columns)
                final_output_treeview["show"] = "headings"
                for column in final_output_treeview["columns"]:
                    final_output_treeview.heading(column, text=column)

                df_rows = df.head(50).to_numpy().tolist()
                for row in df_rows:
                    final_output_treeview.insert("", "end", values=row)
            messagebox.showinfo("Success", "Successfully review final forecast.")


            if file_key == 'df_weeklyPhasing_final':
                df = etl_conversion_si(processed_input_data['df_stock_policy_byProvince_DPName'], upload_file_paths['df_manual_stock_policy'][1], database_data['df_SO_weekly_last_5w'], final_output_data['df_weeklyPhasing_agg'], upload_file_paths['df_mapping_province_DC'][1], database_data['df_master_date'])
                version = 'FINAL'
                event_time = datetime.now()
                df['event_time'] = event_time
                df['version'] = version
                
                # assign value in the dictionary
                final_output_data[file_key] = df.copy()

                clear_data_final_output(file_key)
                final_output_treeview = final_output_treeviews[file_key]
                final_output_treeview["column"] = list(df.columns)
                final_output_treeview["show"] = "headings"
                for column in final_output_treeview["columns"]:
                    final_output_treeview.heading(column, text=column)

                df_rows = df.head(50).to_numpy().tolist()
                for row in df_rows:
                    final_output_treeview.insert("", "end", values=row)
            messagebox.showinfo("Success", "Successfully review final forecast.")

        # Create the Submit button
        submit_button = tk.Button(file_frame, text="Review", command=lambda key=file_key: submit_button_click(key, client))
        submit_button.grid(row=len(divisions), column=0, pady=10)

        # Button to upload results to BigQuery
        upload_button = ttk.Button(file_frame, text="Upload to BigQuery",
                                   command=lambda key=file_key: upload_to_bigquery_final(key, client))
        upload_button.grid(row=len(divisions), column=1, pady=10)
        
    if file_key in ['df_weeklyPhasing_agg']: 
        # Create the Submit button
        submit_button = tk.Button(file_frame, text="Split to Week", command=lambda key=file_key: submit_button_click(key, client))
        submit_button.grid(row=0, column=0, pady=10)
        # Button to upload results to BigQuery
        upload_button = ttk.Button(file_frame, text="Upload to BigQuery",
                                   command=lambda key=file_key: upload_to_bigquery_final(key, client))
        upload_button.grid(row=0, column=1, pady=10)
    
    if file_key in ['df_weeklyPhasing_final']:
        # Create the Submit button
        submit_button = tk.Button(file_frame, text="Convert to Sell In", command=lambda key=file_key: submit_button_click(key, client))
        submit_button.grid(row=0, column=0, pady=10)
        # Button to upload results to BigQuery
        upload_button = ttk.Button(file_frame, text="Upload to BigQuery",
                                   command=lambda key=file_key: upload_to_bigquery_final(key, client))
        upload_button.grid(row=0, column=1, pady=10)

    # Treeview widget in each tab
    final_output_treeview = ttk.Treeview(review_frame)
    final_output_treeview.place(relheight=1, relwidth=1)
    treescrolly = tk.Scrollbar(review_frame, orient="vertical", command=final_output_treeview.yview)
    treescrollx = tk.Scrollbar(review_frame, orient="horizontal", command=final_output_treeview.xview)
    final_output_treeview.configure(xscrollcommand=treescrollx.set, yscrollcommand=treescrolly.set)
    treescrollx.pack(side="bottom", fill="x")
    treescrolly.pack(side="right", fill="y")

    final_output_treeviews[file_key] = final_output_treeview
    
    # Create a label for the license statement
    license_label = tk.Label(tab_frame, text="This app is developed by MASAN GROUP and is intended solely for use by members within the organization. Unauthorized access or use is strictly prohibited.", font=("Arial", 12))
    license_label.pack(anchor="center", side = 'bottom')

# SOE
# Function to clear the final data in the Treeview for a specific tab
def clear_data_soe_output(file_key):
    soe_output_treeview = soe_output_treeviews[file_key]
    soe_output_treeview.delete(*soe_output_treeview.get_children())
    
# Create other parental tab called REVIEWER
soe_tab = ttk.Frame(tab_widget)
tab_widget.add(soe_tab, text="SOE")

# Create a Notebook widget to hold the tabs within the "Upload" tab
soe_tab_widget = ttk.Notebook(soe_tab)
soe_tab_widget.pack(fill='both', expand=True)

soe_output_treeviews = {}
for file_key in soe_output:
    tab_frame = ttk.Frame(soe_tab_widget)
    soe_tab_widget.add(tab_frame, text=file_key)

    # Frame for TreeView in the subtab
    soe_frame = tk.LabelFrame(tab_frame, text="Review Data")
    soe_frame.place(height=500, width=1800)

    # Frame for open file dialog in each tab
    file_frame = tk.LabelFrame(tab_frame, text="Action")
    file_frame.place(height=100, width=400, rely=0.6, relx=0)
    
    # Function to handle export button click
    def export_file_soe_output(file_key):
        file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
        if file_path:
            dataframe = soe_output[file_key]
            dataframe.to_csv(file_path, index=False, encoding='utf-8-sig')
            messagebox.showinfo("Export Successful", "File exported successfully.")
    
    def submit_button_click_soe(file_key, client):
        if file_key == 'df_latest_weeklyPhasing':
            query = '''
                SELECT 
                  * 
                FROM `mch-dwh-409503.MCH_Output.WEEKLY_PHASING_FINAL` t1
                INNER JOIN (
                  SELECT
                    SUB_DIVISION_NAME, 
                    MAX(EVENT_TIME) AS EVENT_TIME
                  FROM `mch-dwh-409503.MCH_Output.WEEKLY_PHASING_FINAL` 
                  GROUP BY 1  
                ) t2 
                USING (SUB_DIVISION_NAME, EVENT_TIME)
            '''
            print(query)

            df = client.query(query).to_dataframe()
            # version = 'FINAL'
            # event_time = datetime.now()
            # df['event_time'] = event_time
            # df['version'] = version

            # assign value in the dictionary
            soe_output[file_key] = df.copy()

            clear_data_soe_output(file_key)
            soe_output_treeview = soe_output_treeviews[file_key]
            soe_output_treeview["column"] = list(df.columns)
            soe_output_treeview["show"] = "headings"
            for column in soe_output_treeview["columns"]:
                soe_output_treeview.heading(column, text=column)

            df_rows = df.head(50).to_numpy().tolist()
            for row in df_rows:
                soe_output_treeview.insert("", "end", values=row)
        
        if file_key == 'df_soe':
            df = etl_soe(database_data['df_SO_weekly_last_5w'], upload_file_paths['df_mapping_province_DC'][1], soe_output['df_latest_weeklyPhasing'], database_data['df_stock_weekly_last_5w'])
            # assign value in the dictionary
            soe_output[file_key] = df.copy()

            clear_data_soe_output(file_key)
            soe_output_treeview = soe_output_treeviews[file_key]
            soe_output_treeview["column"] = list(df.columns)
            soe_output_treeview["show"] = "headings"
            for column in soe_output_treeview["columns"]:
                soe_output_treeview.heading(column, text=column)

            df_rows = df.head(50).to_numpy().tolist()
            for row in df_rows:
                soe_output_treeview.insert("", "end", values=row)
        if file_key == 'df_weeklyPhasing_soe_adjustment':
            df = etl_soe_adjustment(upload_file_paths['df_soe_adjustment'][1], processed_input_data['df_price_list_month'], soe_output['df_latest_weeklyPhasing'], database_data['df_province_contribution_inRegion_byDPName'], database_data['df_province_contribution_inRegion_byGroupSKU'], database_data['df_province_contribution_inRegion_bySubDivision'], database_data['df_province_contribution_inRegion_byDefault'])
            event_time = datetime.now()
            df['event_time'] = event_time

            # assign value in the dictionary
            soe_output[file_key] = df.copy()

            clear_data_soe_output(file_key)
            soe_output_treeview = soe_output_treeviews[file_key]
            soe_output_treeview["column"] = list(df.columns)
            soe_output_treeview["show"] = "headings"
            for column in soe_output_treeview["columns"]:
                soe_output_treeview.heading(column, text=column)

            df_rows = df.head(50).to_numpy().tolist()
            for row in df_rows:
                soe_output_treeview.insert("", "end", values=row)
        if file_key == 'df_weeklyPhasing_soe_adjustment_final':
            df = etl_conversion_si(processed_input_data['df_stock_policy_byProvince_DPName'], upload_file_paths['df_manual_stock_policy'][1], database_data['df_SO_weekly_last_5w'], soe_output['df_weeklyPhasing_soe_adjustment'], upload_file_paths['df_mapping_province_DC'][1], database_data['df_master_date'])
            event_time = datetime.now()
            df['event_time'] = event_time

            # assign value in the dictionary
            soe_output[file_key] = df.copy()

            clear_data_soe_output(file_key)
            soe_output_treeview = soe_output_treeviews[file_key]
            soe_output_treeview["column"] = list(df.columns)
            soe_output_treeview["show"] = "headings"
            for column in soe_output_treeview["columns"]:
                soe_output_treeview.heading(column, text=column)

            df_rows = df.head(50).to_numpy().tolist()
            for row in df_rows:
                soe_output_treeview.insert("", "end", values=row)
    def upload_to_bigquery_soe(file_key, client):
        try:
            if file_key == 'df_weeklyPhasing_soe_adjustment':
                table_id = 'mch-dwh-409503.MCH_Output.WEEKLY_PHASING_FINAL'
            if file_key == 'df_weeklyPhasing_soe_adjustment_final':
                table_id = 'mch-dwh-409503.MCH_Output.SELL_IN_SELL_OUT_FORECAST_FINAL'

            # upload to bigquery
            client.load_table_from_dataframe(
                soe_output[file_key], table_id
            )
            messagebox.showinfo("Success", "Successfully upload to BigQuery Database.")
        except:
            messagebox.showerror("Error", "Failed to upload to BigQuery Database")
    
    if file_key in ['df_latest_weeklyPhasing']:
        # Create the Submit button
        submit_button = tk.Button(file_frame, text="Retrieve latest Weekly Forecast", command=lambda key=file_key: submit_button_click_soe(key, client))
        submit_button.grid(row=0, column=0, pady=10)

        # Create the export button in the subtab
        export_button = ttk.Button(file_frame, text="Export data", command=lambda key=file_key: export_file_soe_output(key))
        export_button.grid(row=0, column=1, pady=10)
        
    if file_key in ['df_soe']:
        # Create the Submit button
        submit_button = tk.Button(file_frame, text="Calculate Gap MTD SO", command=lambda key=file_key: submit_button_click_soe(key, client))
        submit_button.grid(row=0, column=0, pady=10)

        # Create the export button in the subtab
        export_button = ttk.Button(file_frame, text="Export data", command=lambda key=file_key: export_file_soe_output(key))
        export_button.grid(row=0, column=1, pady=10)
        
    if file_key in ['df_weeklyPhasing_soe_adjustment']:  
        # Create the Submit button
        submit_button = tk.Button(file_frame, text="Adjust Weekly Forecast", command=lambda key=file_key: submit_button_click_soe(key, client))
        submit_button.grid(row=0, column=0, pady=10)
        
        # Button to upload results to BigQuery
        upload_button = ttk.Button(file_frame, text="Upload to BigQuery",
                                   command=lambda key=file_key: upload_to_bigquery_final(key, client))
        upload_button.grid(row=0, column=1, pady=10)
        
        # Create the export button in the subtab
        export_button = ttk.Button(file_frame, text="Export data", command=lambda key=file_key: export_file_soe_output(key))
        export_button.grid(row=0, column=2, pady=10)

    if file_key in ['df_weeklyPhasing_soe_adjustment_final']:  
        # Create the Submit button
        submit_button = tk.Button(file_frame, text="Convert to Sell In", command=lambda key=file_key: submit_button_click_soe(key, client))
        submit_button.grid(row=0, column=0, pady=10)
        
        # Button to upload results to BigQuery
        upload_button = ttk.Button(file_frame, text="Upload to BigQuery",
                                   command=lambda key=file_key: upload_to_bigquery_final(key, client))
        upload_button.grid(row=0, column=1, pady=10)
        
        # Create the export button in the subtab
        export_button = ttk.Button(file_frame, text="Export data", command=lambda key=file_key: export_file_soe_output(key))
        export_button.grid(row=0, column=2, pady=10)

    # Treeview widget in each tab
    soe_output_treeview = ttk.Treeview(soe_frame)
    soe_output_treeview.place(relheight=1, relwidth=1)
    treescrolly = tk.Scrollbar(soe_frame, orient="vertical", command=soe_output_treeview.yview)
    treescrollx = tk.Scrollbar(soe_frame, orient="horizontal", command=soe_output_treeview.xview)
    soe_output_treeview.configure(xscrollcommand=treescrollx.set, yscrollcommand=treescrolly.set)
    treescrollx.pack(side="bottom", fill="x")
    treescrolly.pack(side="right", fill="y")

    soe_output_treeviews[file_key] = soe_output_treeview
    
    # Create a label for the license statement
    license_label = tk.Label(tab_frame, text="This app is developed by MASAN GROUP and is intended solely for use by members within the organization. Unauthorized access or use is strictly prohibited.", font=("Arial", 12))
    license_label.pack(anchor="center", side = 'bottom')

# Run the GUI
root.mainloop()
