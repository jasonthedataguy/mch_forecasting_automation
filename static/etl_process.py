# for data processing
import pandas as pd
import numpy as np
import glob 
import os 
import datetime
from datetime import date, datetime, timedelta

# COLLECT DATA
def etl_collect_clean(df_contribution, df_region_contribution):
    # COLLECT DATA 
    # master data
    df_master = pd.read_csv('static/database/Master Data.csv')

    # historical sell out (Agg monthly)
    df_so = pd.read_csv('static/database/SO/CSV/SO_2022.csv') 

    # historical sell out (Agg monthly)
    df_si = pd.read_csv('static/database/SI/CSV/SI 2022.csv') 

    # master calendar => rename columns
    df_master_calendar = pd.read_excel('static/database/Master Calendar.xlsx').rename(columns = {'Year Month ID': 'yearMonth', 'Master_Month.SO Day 2M': 'cnt_so_days_last2m', 'No. SO Day': 'cnt_so_days'})

    # # region contribution & contribution (Manual Input)
    # df_contribution = pd.read_csv('static/database/Contribution - Manual Input.csv')
    # df_region_contribution = pd.read_csv('static/database/Region Contribution - Manual Input.csv')
    
    #unpivot
    df_contribution_melted = pd.melt(df_contribution, 
                                     id_vars=['Demand Planning Standard SKU Name', 'Group SKU', 'Channel', 'Sub Division Name', 'Duplication'], 
                                     value_vars=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
                                    ).rename(columns = {'variable': 'Month', 'value': 'Contribution'})


    df_region_contribution_melted = pd.melt(df_region_contribution, 
                                            id_vars=['DP Name', 'Region Name', 'Channel Code'], 
                                            value_vars=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
                                           ).rename(columns = {'variable': 'Month', 'value': 'Contribution'})

    # collect ref region list & region_sub_manual & region_manual
    df_region_list = pd.read_excel('static/database/Region List.xlsx')
    # df_region_sub_manual = pd.read_excel('static/database/Region Sub Manual.xlsx')

    # collect price data 
    df_price_list = pd.read_excel('static/database/Price List.xlsx')
    
    # collect master date
    df_master_date = pd.read_csv('static/database/Master Date.csv')

    # collect data input manual
    directory = r'static/database/MUF Input/'
    files = glob.glob(os.path.join(directory, '*.xlsm'))
    columns_to_trim = [' 1 ', ' 2 ', ' 3 ', ' 4 ', ' 5 ', ' 6 ', ' 7 ', ' 8 ', ' 9 ', ' 10 ', ' 11 ', ' 12 ']
    # merge two dictionaries togetger
    def Merge(dict1, dict2):
        res = {**dict1, **dict2}
        return res
    df_muf_input_list = [pd.read_excel(f, sheet_name = 'Data Base', skiprows=2).rename(columns=Merge({col: col.strip() for col in columns_to_trim}, 
                                                                                                     {'Sub Division Name': 'Sub Division',
                                                                                                      'Building Block': 'Building Blocks'}) 
                                                                                      ) for f in files
                        ]
    df_muf_input = pd.concat(df_muf_input_list, ignore_index = True)

    # collect data Code Mapping Master & Mapping Item Code
    directory = r'static/database/Category Input/'
    files = glob.glob(os.path.join(directory, '*.xlsm'))
    df_codeMappingMaster_list = [pd.read_excel(f, sheet_name = 'Code Mapping Master', skiprows=1) for f in files]
    df_codeMappingMaster = pd.concat(df_codeMappingMaster_list, ignore_index = True)

    df_mappingItemCode = pd.read_excel('static/database/Mapping Item code.xlsx')
    
    return df_master, df_so, df_si, df_master_calendar, df_contribution_melted, df_region_contribution_melted, df_region_list, df_price_list, df_master_date, df_codeMappingMaster, df_mappingItemCode

# BASELINE
def etl_baseline(df_so, df_master, df_master_calendar):
    # extract year and month from yearMonth column
    df_master_calendar['year'] = df_master_calendar['yearMonth'].astype(str).str[:4].astype('int')
    df_master_calendar['month'] = df_master_calendar['yearMonth'].astype(str).str[4:6].astype('int')

    # calculate cnt_days in month
    df_master_calendar['cnt_days'] = np.where(df_master_calendar['month'].isin([1,3,5,7,8,10,12]), 31,
                                              np.where(df_master_calendar['month'].isin([4,6,9,11]), 30,
                                                       np.where(df_master_calendar['month'].isin([2]) & (df_master_calendar['year'] % 4 == 0), 29, 28
                                                               )
                                                      )
                                             )

    # calculate cnt_days_year, cnt_so_days_year and % contribution
    df_master_calendar = pd.merge(df_master_calendar,
                                  df_master_calendar.groupby(['year'])['cnt_so_days'].sum().reset_index().rename(columns = {'cnt_so_days': 'cnt_so_days_year'}),
                                  on = ['year']
                                 )
    df_master_calendar['cnt_days_year'] = np.where(df_master_calendar['year'] % 4 == 0, 366, 365)
    df_master_calendar['%cnt_days'] = df_master_calendar['cnt_days'] / df_master_calendar['cnt_days_year'] 
    df_master_calendar['%cnt_so_days'] = df_master_calendar['cnt_so_days'] / df_master_calendar['cnt_so_days_year'] 

    # gap in contribution of SO days vs calendar days
    df_master_calendar['gap_contribution_so_days'] = df_master_calendar['%cnt_so_days'] - df_master_calendar['%cnt_days'] 


    df_test = pd.merge(df_so[
                                ['Product Number', 'Product Name', 'Sub Division Desc', 'Brand Desc', 'Channel Code', 'Channel Name', 'Region Name', 
                                'Year', 'Calendar Month', 'SS_Sales Order Value (Dist. Price) Excluded SCT', 'SS_S-Out Order Qty (Cases)']
                              ].rename(columns = {'SS_Sales Order Value (Dist. Price) Excluded SCT': 'so_value',
                                                  'SS_S-Out Order Qty (Cases)': 'so_volume'
                                                 }), 
                         df_master[['Product Number', 'Group SKU', 'Demand Planning Standard SKU Name', 'Sub Division Name']],
                         on = ['Product Number']
                        )

    # calculate baseline by Group SKU, Channel, Region => if possible we should break down by 'Sub Type Code', 'Sub Type Name' and 'Province'
    df_baseline_year = df_test.groupby(['Sub Division Desc', 'Group SKU', 
                                        'Channel Code', 'Region Name', 
                                        'Year'])[['so_value', 'so_volume']].sum().reset_index().rename(columns = {'so_value': 'so_value_year', 'so_volume': 'so_volume_year'})

    # calculate cnt_days in year
    df_baseline_year['cnt_days_year'] = np.where(df_baseline_year['Year'] % 4 == 0, 366, 365)

    # daily consumption rate
    df_baseline_year['daily_consumption_rate'] = df_baseline_year['so_volume_year'] / df_baseline_year['cnt_days_year']

    # baseline month => actual contribution by Group SKU & month
    df_baseline_month = df_test.groupby(['Sub Division Desc', 'Group SKU', 
                                        'Channel Code', 'Region Name', 
                                        'Year', 'Calendar Month'])[['so_value', 'so_volume']].sum().reset_index().rename(columns = {'so_value': 'so_value_month', 'so_volume': 'so_volume_month'})

    df_actual_monthly_contribution = pd.merge(df_baseline_month,
                                              df_baseline_year,
                                              on = ['Sub Division Desc', 'Group SKU', 'Channel Code', 'Region Name', 'Year']
                                             )

    # calculate % actual monthly contribution
    df_actual_monthly_contribution['%actual_monthly_contribution'] = df_actual_monthly_contribution['so_volume_month'] / df_actual_monthly_contribution['so_volume_year'] 

    # join df_baseline with df_master_calendar to calculate baseline_adj (based on Gap month contribution between calendar month vs. SO working day)
    # predict for 2023 using 2022 daily consumption rate
    df_baseline = pd.merge(df_baseline_year.assign(year = 2023),
                             df_master_calendar[['year', 'month', 'cnt_days', 'cnt_so_days', '%cnt_days', '%cnt_so_days', 'gap_contribution_so_days']][df_master_calendar['year'] == 2023],
                             on = ['year']
                            )

    df_baseline = pd.merge(df_baseline, 
                             df_actual_monthly_contribution[['Sub Division Desc', 'Group SKU', 
                                                             'Channel Code', 'Region Name', 
                                                             'Year', 'Calendar Month', 
                                                             '%actual_monthly_contribution',
                                                             'so_value_month', 'so_volume_month']],
                             how = 'left', 
                             left_on = ['Sub Division Desc', 'Group SKU', 'Channel Code', 'Region Name', 'Year', 'month'],
                             right_on = ['Sub Division Desc', 'Group SKU', 'Channel Code', 'Region Name', 'Year', 'Calendar Month']
                            )

    # gap in contribution of actual vs SO days
    df_baseline['gap_contribution_actual_vs_so_days'] = df_baseline['%actual_monthly_contribution'] - df_baseline['%cnt_so_days']
    # seasonal factor
    df_baseline['seasonal_factor'] = df_baseline['gap_contribution_actual_vs_so_days'] / df_baseline['%cnt_so_days']

    # baseline consumption
    df_baseline['baseline'] = df_baseline['daily_consumption_rate'] * df_baseline['cnt_days']
    # baseline adjusted (gap between SO DAY contribution VS CALENDAR DAY contribution)
    df_baseline['baseline_adj'] = df_baseline['baseline'] * df_baseline['%cnt_so_days'] /  df_baseline['%cnt_days'] - df_baseline['baseline']
    # seasonality
    df_baseline['seasonality'] = df_baseline['so_volume_month'] - df_baseline['baseline'] - df_baseline['baseline']

    df_baseline = df_baseline[[
    'Sub Division Desc', 'Group SKU', 'Channel Code', 'Region Name', 'year', 'month',
    'so_value_month', 'so_volume_month', 'baseline', 'baseline_adj', 'seasonality'
    ]]
    
    return df_baseline

# GROUP SKU MASTER / REGION GROUP SKU / REGION SUB DIVISION
def etl_groupSKU_master(df_so, df_master): # missing last 3 month condition to filter df_master
    # group SKU master
    df_groupSKU_master = pd.merge(df_so, 
                         df_master[['Product Number', 'Active Status', 'Group SKU', 'Demand Planning Standard SKU Name', 'Local / Export / Others', 'Sub Division Name']],
                         how = 'left',
                         on = ['Product Number']
                        )

    # conditions to create new column Channel
    conditions = [
        ( (df_groupSKU_master['Sub Division Name'] == 'Home Care') & (df_groupSKU_master['Sub Type Code'].str.contains('HRC')) ),
        ( (df_groupSKU_master['Sub Division Name'] == 'Home Care') & (df_groupSKU_master['Channel Code'] == 'KA0') ),
        ( (df_groupSKU_master['Sub Division Name'] == 'Home Care') & (df_groupSKU_master['Sub Type Code'].fillna('').str.contains('C1')) ),
        ( (df_groupSKU_master['Sub Division Name'] == 'Home Care') & (df_groupSKU_master['Sub Type Code'] == 'NPP_GT') & (~ df_groupSKU_master['Region Name'].isin(['Miền Trung', 'Miền Bắc'])) ),
        ( (df_groupSKU_master['Sub Division Name'] == 'Home Care') & (df_groupSKU_master['Sub Type Code'] == 'HORECA') ),
        ( df_groupSKU_master['Sub Type Code'].fillna('').str.contains('C1') ),
        ( df_groupSKU_master['Channel Code'] == 'B2H' )
    ]

    choices = ['NETCO_HRC', 'NETCO_HRC', 'NETCO_C1', 'NETCO_C1', 'NETCO_HRC', 'GT0_C1', 'GT0']

    fallback_column = 'Channel Code'

    # create new column channel based on conditions / choices / fallback
    df_groupSKU_master['Channel'] = np.select(conditions, choices, df_groupSKU_master[fallback_column])
    
    return df_groupSKU_master


def etl_regionContribution_groupSKU(df_groupSKU_master, df_region_sub_manual):
    dimensions = ['Group SKU', 'Region Name', 'Channel']
    dimensions_total = list(set(dimensions) - set(['Region Name']))
    measures = ['SS_Sales Order Value (Dist. Price) Excluded SCT', 'SS_S-Out Order Qty (Cases)']
    rename_measures = ['value_SO', 'volume_SO']
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

    df_region_groupSKU['Contribution'] = np.where( df_region_groupSKU['total_value_SO'] == 0, 0, df_region_groupSKU['value_SO'] / df_region_groupSKU['total_value_SO'] )

    df_region_groupSKU = pd.concat([df_region_groupSKU, df_region_sub_manual.rename(columns = {'Channel Code': 'Channel'})],
                                   axis = 0,
                                   ignore_index = True
                                  )
    return df_region_groupSKU

def etl_regionContribution_subDiv(df_groupSKU_master, df_region_sub_manual):
    dimensions = ['Sub Division Name', 'Region Name', 'Channel']
    dimensions_total = list(set(dimensions) - set(['Region Name']))
    measures = ['SS_Sales Order Value (Dist. Price) Excluded SCT', 'SS_S-Out Order Qty (Cases)']
    rename_measures = ['value_SO', 'volume_SO']
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

    df_region_subDivision['Contribution'] = np.where( df_region_subDivision['total_value_SO'] == 0, 0, df_region_subDivision['value_SO'] / df_region_subDivision['total_value_SO'] )

    df_region_subDivision = pd.concat([df_region_subDivision, df_region_sub_manual.rename(columns = {'Channel Code': 'Channel'})],
                                   axis = 0,
                                   ignore_index = True
                                  )
    return df_region_subDivision

# MANUAL GROUP SKU 
def etl_manual_groupSKU(df_contribution_melted, df_region_contribution_melted, df_region_groupSKU, df_region_subDivision):

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

    df_manual_groupSKU = pd.concat([df_manual_groupSKU_region, df_manual_groupSKU_NW], ignore_index = True)
    
    # drop duplicates due to multiple left join
    df_manual_groupSKU = df_manual_groupSKU.drop_duplicates()
    
    df_manual_groupSKU.columns = 'Manual Group SKU.' + df_manual_groupSKU.columns
    
    return df_manual_groupSKU

# DEFAULT GROUP SKU 
def etl_default_groupSKU(df_groupSKU_master, df_contribution_melted):
    # calculate groupSKU NW from df_groupSKU_master
    dimensions = ["Demand Planning Standard SKU Name", "Group SKU", "Channel", "Sub Division Name", "Region Name"]
    # contribution is by Group SKU / Channel / Sub Division Name
    dimensions_total_NW = ["Group SKU", "Channel", "Sub Division Name"]
    dimensions_total_region = ["Group SKU", "Channel", "Region Name"]
    measures = ['SS_Sales Order Value (Dist. Price) Excluded SCT']
    rename_measures = ['value_SO']
    rename_total_measures = ['total_' + i for i in rename_measures]
    rename_measure_dictionary = dict(zip(measures, rename_measures))
    rename_total_measure_dictionary = dict(zip(rename_measures, rename_total_measures))

    df_default_groupSKU_NW = df_groupSKU_master.groupby(dimensions)[measures].sum().reset_index().rename(columns = rename_measure_dictionary)

    df_default_groupSKU_NW = pd.merge(df_default_groupSKU_NW,
                                   df_default_groupSKU_NW.groupby(dimensions_total_NW)[rename_measures].sum().reset_index().rename(columns = rename_total_measure_dictionary),
                                   how = 'left',
                                   on = dimensions_total_NW
                                  )

    df_default_groupSKU_NW['Contribution'] = np.where(df_default_groupSKU_NW['total_value_SO'] == 0, 0, df_default_groupSKU_NW['value_SO'] / df_default_groupSKU_NW['total_value_SO'])

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

    df_default_groupSKU_region['Contribution'] = np.where(df_default_groupSKU_region['total_value_SO'] == 0, 0, df_default_groupSKU_region['value_SO'] / df_default_groupSKU_region['total_value_SO'])

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

# PRICE MONTH 
def etl_price(df_price_list, df_master_date):
    # calculate median of month from year and week
    df_master_date_final = df_master_date.groupby(['Iso Year', 'Iso Weeknum'])['Month'].median().reset_index()
    # create column Week (yearWeek)
    df_master_date_final['Week'] = (df_master_date_final['Iso Year']*100 + df_master_date_final['Iso Weeknum']).astype('str')

    df_channel_list = pd.DataFrame({'Channel': ['MT0', 'GT0', 'KA0', 'GT0_C1', 'NETCO_C1', 'NETCO_HRC']})
    df_region_list = pd.DataFrame({'Region': ['Miền Bắc', 'Miền Đông', 'Miền Trung', 'Miền Tây', 'HCM', 'NW']})


    # unpivot price_list
    df_price_list_melted = pd.melt(df_price_list, 
                                     id_vars=['Sub Division Name', 'Group SKU', 'DPName', 'Item Code', 'Product Name', 'Channel', 'Region'], 
                                     value_vars=list(set(df_price_list.columns) - set(['Sub Division Name', 'Group SKU', 'DPName', 'Item Code', 'Product Name', 'Channel', 'Region']))
                                    ).rename(columns = {'variable': 'Week', 'value': 'List Price'})

    # append df_channel_list 
    df_price_list_melted = pd.concat([df_price_list_melted, df_channel_list], ignore_index = True)

    # pivot
    df_price_list_melted_pivotChannel = pd.pivot_table(df_price_list_melted, 
                                           values='List Price', 
                                           index=['Sub Division Name', 'Group SKU', 'DPName', 'Item Code', 'Product Name', 'Region', 'Week'], 
                                           columns='Channel', 
                                           aggfunc='mean').reset_index()

    # create new columns for channel in df_channel_list in case the df doesnt have it
    for channel in df_channel_list['Channel'].unique():
        if channel not in df_price_list_melted_pivotChannel.columns:
            df_price_list_melted_pivotChannel[channel] = np.nan

    df_price_list_melted_pivotChannel = df_price_list_melted_pivotChannel.rename(columns = {'GT0_C1': 'GT0_C1/', 'KA0': 'KA0/', 'MT0': 'MT0/', 'NETCO_C1': 'NETCO_C1/', 'NETCO_HRC': 'NETCO_HRC/'})

    # add columns to calculate price for each channel => base is GT0
    # MT0
    df_price_list_melted_pivotChannel['MT0'] = np.where(df_price_list_melted_pivotChannel['MT0/'].isnull(), 
                                                        df_price_list_melted_pivotChannel['GT0'], 
                                                        df_price_list_melted_pivotChannel['MT0/']
                                                       )

    # GT0_C1
    df_price_list_melted_pivotChannel['GT0_C1'] = np.where(df_price_list_melted_pivotChannel['GT0_C1/'].isnull(), 
                                                        df_price_list_melted_pivotChannel['GT0'], 
                                                        df_price_list_melted_pivotChannel['GT0_C1/']
                                                       )

    # KA0
    df_price_list_melted_pivotChannel['KA0'] = np.where(df_price_list_melted_pivotChannel['KA0/'].isnull(), 
                                                        df_price_list_melted_pivotChannel['GT0'], 
                                                        df_price_list_melted_pivotChannel['KA0/']
                                                       )

    # NETCO_C1
    df_price_list_melted_pivotChannel['NETCO_C1'] = np.where(df_price_list_melted_pivotChannel['NETCO_C1/'].isnull(), 
                                                        df_price_list_melted_pivotChannel['GT0'], 
                                                        df_price_list_melted_pivotChannel['NETCO_C1/']
                                                       )

    # NETCO_HRC
    df_price_list_melted_pivotChannel['NETCO_HRC'] = np.where(df_price_list_melted_pivotChannel['NETCO_HRC/'].isnull(), 
                                                        df_price_list_melted_pivotChannel['GT0'], 
                                                        df_price_list_melted_pivotChannel['NETCO_HRC/']
                                                       )

    # df_price_list_melted_2 
    df_price_list_melted_2 = pd.melt(df_price_list_melted_pivotChannel, 
                                     id_vars=['Sub Division Name', 'Group SKU', 'DPName', 'Item Code', 'Product Name', 'Region', 'Week'], 
                                     value_vars=df_channel_list['Channel'].to_list()
                                    ).rename(columns = {'variable': 'Channel', 'value': 'Price'})

    # append df_region_list 
    df_price_list_melted_2 = pd.concat([df_price_list_melted_2, df_region_list], ignore_index = True)

    # pivot
    df_price_list_melted_2_pivotRegion = pd.pivot_table(df_price_list_melted_2, 
                                           values='Price', 
                                           index=['Sub Division Name', 'Group SKU', 'DPName', 'Item Code', 'Product Name', 'Week', 'Channel'], 
                                           columns='Region', 
                                           aggfunc='sum').reset_index()

    # create new columns for region in df_region_list in case the df doesnt have it
    for region in df_region_list['Region'].unique():
        if region not in df_price_list_melted_2_pivotRegion.columns:
            df_price_list_melted_2_pivotRegion[region] = np.nan

    df_price_list_melted_2_pivotRegion = df_price_list_melted_2_pivotRegion.rename(columns = {'HCM': 'HCM/', 'Miền Trung': 'Miền Trung/', 'Miền Tây': 'Miền Tây/', 'Miền Đông': 'Miền Đông/'})

    # add columns to calculate price for each region => base is Miền Bắc
    # HCM
    df_price_list_melted_2_pivotRegion['HCM'] = np.where(df_price_list_melted_2_pivotRegion['HCM/'].isnull(), 
                                                        df_price_list_melted_2_pivotRegion['Miền Bắc'], 
                                                        df_price_list_melted_2_pivotRegion['HCM/']
                                                       )

    # Miền Trung
    df_price_list_melted_2_pivotRegion['Miền Trung'] = np.where(df_price_list_melted_2_pivotRegion['Miền Trung/'].isnull(), 
                                                        df_price_list_melted_2_pivotRegion['Miền Bắc'], 
                                                        df_price_list_melted_2_pivotRegion['Miền Trung/']
                                                       )

    # Miền Tây
    df_price_list_melted_2_pivotRegion['Miền Tây'] = np.where(df_price_list_melted_2_pivotRegion['Miền Tây/'].isnull(), 
                                                        df_price_list_melted_2_pivotRegion['Miền Bắc'], 
                                                        df_price_list_melted_2_pivotRegion['Miền Tây/']
                                                       )

    # Miền Đông
    df_price_list_melted_2_pivotRegion['Miền Đông'] = np.where(df_price_list_melted_2_pivotRegion['Miền Đông/'].isnull(), 
                                                        df_price_list_melted_2_pivotRegion['Miền Bắc'], 
                                                        df_price_list_melted_2_pivotRegion['Miền Đông/']
                                                       )

    # df_price_list_melted_3 
    df_price_list_melted_3 = pd.melt(df_price_list_melted_2_pivotRegion, 
                                     id_vars=['Sub Division Name', 'Group SKU', 'DPName', 'Item Code', 'Product Name', 'Week', 'Channel'], 
                                     value_vars=df_region_list['Region'].to_list()
                                    ).rename(columns = {'variable': 'Region', 'value': 'Price'})

    df_price_list_melted_3 = pd.merge(df_price_list_melted_3,
                                      df_price_list_melted,
                                      how = 'left',
                                      on = ['Item Code', 'Week', 'Channel', 'Region'],
                                      suffixes=('', '_y')
                                     )

    df_price_list_melted_3['final_price'] = np.where(df_price_list_melted_3['List Price'].isnull(), df_price_list_melted_3['Price'], df_price_list_melted_3['List Price'])

    df_price_list_melted_3 = pd.merge(df_price_list_melted_3, 
                                      df_master_date_final,
                                      how = 'left',
                                      on = ['Week']
                                     )

    df_price_list_month = df_price_list_melted_3.groupby([
        "Sub Division Name", "Group SKU", "DPName", "Channel", "Region", "Month", "Iso Year"
        ])['final_price'].mean().reset_index().rename(columns = {'final_price': 'Price'})

    # create column price id
    df_price_list_month['Price ID'] =  df_price_list_month['Group SKU'] + ':' + df_price_list_month['Channel'] + ':' + df_price_list_month['Iso Year'].astype('int').astype('str') + df_price_list_month['Month'].astype('str') + ':' + df_price_list_month['Region'] + ':' + df_price_list_month['DPName']
    
    return df_price_list_month

# MUF DATA
def etl_muf_data(df_muf_input, df_manual_groupSKU, df_default_groupSKU, df_price_list_month):
    # Clean the dataframe
    # remove null values in Year/Uplift type/Building Blocks/Group SKU
    df_muf_input = df_muf_input[df_muf_input['Year'].notnull() & df_muf_input['Uplift type'].notnull() & df_muf_input['Building Blocks'].notnull() & df_muf_input['Group SKU'].notnull()]

    # replace null values in column 'Risk %' by 0
    df_muf_input['Risk %'] = df_muf_input['Risk %'].fillna(0).replace(to_replace=['100%', '1'], value=[1, 1])

    # unpivot 
    df_muf_input_melted = pd.melt(df_muf_input, 
                                  id_vars=['Group SKU', 'Sub Division', 'Site', 'Building Blocks', 'Year', 
                                           'Channel', 'Uplift type', 'Region',  'Risk %'], 
                                  value_vars=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
                                 ).rename(columns = {'variable': 'Month'})

    # group by id_vars and calculate sum 
    df_muf_input_final = df_muf_input_melted.groupby(['Group SKU', 'Sub Division', 'Site', 'Building Blocks', 
                                 'Year', 'Channel', 'Uplift type', 'Region', 'Month', 'Risk %'])['value'].sum().reset_index()

    # using df_groupSKU_default and df_manual_groupSKU_final to map with df_muf_input_final
    df_muf_input_final = pd.merge(pd.merge(df_muf_input_final,
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
    df_muf_input_final['DP Contribution'] = np.where(df_muf_input_final['Default Group SKU.Contribution'].notnull(), 
                                                     df_muf_input_final['Default Group SKU.Contribution'], 
                                                     df_muf_input_final['Manual Group SKU.Contribution']
                                                    )

    df_muf_input_final['DP Name'] = np.where(df_muf_input_final['Manual Group SKU.Demand Planning Standard SKU Name'].notnull(), 
                                                     df_muf_input_final['Manual Group SKU.Demand Planning Standard SKU Name'], 
                                                     df_muf_input_final['Default Group SKU.Demand Planning Standard SKU Name']
                                                    )

    df_muf_input_final['Region Final'] = np.where(df_muf_input_final['Manual Group SKU.Region Name'].notnull(), 
                                                     df_muf_input_final['Manual Group SKU.Region Name'], 
                                                     df_muf_input_final['Default Group SKU.Region Name']
                                                    )

    # change datatype of column Month
    df_muf_input_final['Month'] = df_muf_input_final['Month'].astype('float64')

    df_muf_input_final = pd.merge(df_muf_input_final,
                                 df_price_list_month[['Group SKU', 'Channel', 'Iso Year', 'Month', 'Region', 'DPName', 'Price']], 
                                 how = 'left',
                                 left_on = ['Group SKU', 'Channel', 'Year', 'Month', 'Region Final', 'DP Name'],
                                 right_on = ['Group SKU', 'Channel', 'Iso Year', 'Month', 'Region', 'DPName']
                                )

    # select only needed columns
    df_muf_input_final = df_muf_input_final[['Group SKU', 'Building Blocks', 'Year',
           'Channel', 'Uplift type','Month', 'Risk %', 'value',
           'DP Contribution', 'Price', 'DP Name', 'Region Final']]

    # input val
    df_muf_input_val = df_muf_input_final[df_muf_input_final['Uplift type'] == 'Val']
    # calculate Sell Out - Vol (Kcase) / Val (Bio)
    df_muf_input_val['Sell Out - Val (Bio)'] = df_muf_input_val['value'] * df_muf_input_val['DP Contribution']
    df_muf_input_val['Sell Out - Vol (Kcase)'] = df_muf_input_val['Sell Out - Val (Bio)'] * 1000000 / np.maximum(df_muf_input_val['Price'], 1e-8) #to handle the zero division error
    # rename column DP name
    df_muf_input_val = df_muf_input_val.rename(columns = {'DP Name': 'Group SKU to DP Name.Demand Planning Standard SKU'})

    # input vol
    df_muf_input_vol = df_muf_input_final[df_muf_input_final['Uplift type'] == 'Vol']
    # calculate Sell Out - Vol (Kcase) / Val (Bio)
    df_muf_input_vol['Sell Out - Vol (Kcase)'] = df_muf_input_vol['value'] * df_muf_input_vol['DP Contribution']
    df_muf_input_vol['Sell Out - Val (Bio)'] =  df_muf_input_vol['Sell Out - Vol (Kcase)'] * df_muf_input_vol['Price'] / 1000000
    # rename column DP name
    df_muf_input_vol = df_muf_input_vol.rename(columns = {'DP Name': 'Group SKU to DP Name.Demand Planning Standard SKU'})

    # step 2: combine df_muf_input_val & df_muf_input_vol to create df_SO_uplift
    df_SO_uplift = pd.concat([df_muf_input_val, df_muf_input_vol], ignore_index = False)

    # input percent
    df_muf_input_percent = df_muf_input_melted[df_muf_input_melted['Uplift type'].isin(['%Vol', '%Val'])].groupby(['Group SKU', 'Sub Division', 'Site', 'Building Blocks', 
                                 'Year', 'Channel', 'Uplift type', 'Region', 'Month', 'Risk %'])['value'].sum().reset_index()

    # change datatype of column Month
    df_muf_input_percent['Month'] = df_muf_input_percent['Month'].astype('float64')

    df_muf_input_percent = pd.merge(df_muf_input_percent, 
                                     df_SO_uplift,
                                     how = 'inner',
                                     on = ['Group SKU', 'Channel', 'Year', 'Month'],
                                     suffixes = ['', '_sourceSO']
                                    )

    # add column to flag whether we validate for uplift 
    df_muf_input_percent['Uplift Validate'] = np.where( (df_muf_input_percent['Region'] == 'NW') | (df_muf_input_percent['Region'] == df_muf_input_percent['Region Final']),
                                                                                                    True, 
                                                                                                    False
                                                        )

    # Filter to get data with Uplift validate only 
    df_muf_input_percent = df_muf_input_percent[df_muf_input_percent['Uplift Validate'] == True]

    # uplift by percent
    df_muf_input_percent = df_muf_input_percent.groupby(['Year', 'Group SKU', 'Channel', 'Month', 'Region Final', 
                          'Group SKU to DP Name.Demand Planning Standard SKU', 'Sell Out - Val (Bio)', 'Sell Out - Vol (Kcase)', 
                          'Building Blocks', 'Uplift type', 'Risk %', 'Price', 'Uplift Validate'
                         ])['value'].sum().reset_index().pivot(
        index = ['Year', 'Group SKU', 'Channel', 'Month', 'Region Final', 
                          'Group SKU to DP Name.Demand Planning Standard SKU', 'Sell Out - Val (Bio)', 'Sell Out - Vol (Kcase)', 
                          'Building Blocks', 'Risk %', 'Price', 'Uplift Validate'], 
        columns = ['Uplift type'], 
        values = 'value'
    ).reset_index()

    # fill na by 0
    df_muf_input_percent[['%Val', '%Vol']] = df_muf_input_percent[['%Val', '%Vol']].fillna(0)

    # calculate value / vol uplift
    df_muf_input_percent['Sell Out - Vol (Kcase) / uplift'] = df_muf_input_percent['Sell Out - Vol (Kcase)'] * df_muf_input_percent['%Vol'] + df_muf_input_percent['Sell Out - Val (Bio)'] * df_muf_input_percent['%Val'] * 1000000 / np.maximum(df_muf_input_val['Price'], 1e-8) #to handle the zero division error
    df_muf_input_percent['Sell Out - Val (Bio) / uplift'] = df_muf_input_percent['Sell Out - Val (Bio)'] * df_muf_input_percent['%Val'] + df_muf_input_percent['Sell Out - Vol (Kcase)'] * df_muf_input_percent['%Vol'] * np.maximum(df_muf_input_val['Price'], 1e-8) / 1000000 #to handle the zero division error

    df_muf_input_percent = df_muf_input_percent[
                                                ['Group SKU', 'Building Blocks', 'Channel', 'Year', 'Month', 'Risk %', 'Price', 'Region Final', 
                                                 'Group SKU to DP Name.Demand Planning Standard SKU', 'Sell Out - Vol (Kcase) / uplift', 'Sell Out - Val (Bio) / uplift']
                                               ].rename(columns = {'Sell Out - Vol (Kcase) / uplift': 'Sell Out - Vol (Kcase)',
                                                                    'Sell Out - Val (Bio) / uplift': 'Sell Out - Val (Bio)'
                                                                   }
                                                       )

    df_SO_view = pd.concat([df_SO_uplift,
                            df_muf_input_percent],
                            ignore_index = True
                           )

    df_MUF = pd.concat(
        [df_SO_view.assign(
                        viewBy = 'MUF',
                        valForecast = lambda x: (1 - x['Risk %']) * x['Sell Out - Val (Bio)'],
                        volForecast = lambda x: (1- x['Risk %']) * x['Sell Out - Vol (Kcase)']
                    ),
         df_SO_view[df_SO_view['Risk %'] != 0].assign(
                        viewBy = 'Opportunity',
                        valForecast = lambda x: x['Risk %'] * x['Sell Out - Val (Bio)'],
                        volForecast = lambda x: x['Risk %'] * x['Sell Out - Vol (Kcase)']
                    )
        ], 
        ignore_index = True
    ).rename(columns = {'Group SKU to DP Name.Demand Planning Standard SKU': 'DP Name',
                        'Region Final': 'Region Name',
                        'viewBy': 'View By'}
            ).groupby(["Year", "Group SKU", "Channel", "Month", "DP Name", "Region Name", "Price", "View By"]
                     )[['valForecast', 'volForecast']].sum().reset_index().rename(columns = {'valForecast': 'Sell Out - Val (Bio)',
                                                                                             'volForecast': 'Sell Out - Vol (Kcase)'})
    return df_MUF

# DC CONTRIBUTION 
def etl_DC_contribution(df_si, df_master):
    df_DC_contribution = pd.merge(df_si, 
             df_master[['Product Number', 'Group SKU', 'Demand Planning Standard SKU Name', 'Supply Strategy']].drop_duplicates(),
             how = 'left', 
             on = ['Product Number']
            )
    return df_DC_contribution

# change datatype
# df_DC_contribution['PS_S-In Actual Value (Dist. Price) Excluded SCT'] = df_DC_contribution['PS_S-In Actual Value (Dist. Price) Excluded SCT'].replace(",", "").astype('float64')

def etl_DC_contribution_by_DPName(df_DC_contribution):
    dimensions = ['Inventory Org Name', 'Demand Planning Standard SKU Name', 'Group SKU', 'Region Name', 'Channel Code']
    dimensions_total = list(set(dimensions) - set(['Inventory Org Name']))
    measures = ['PS_S-In Actual Value (Dist. Price) Excluded SCT', 'PS_S-In Actual Qty (Cases)']
    rename_measures = ['value_SI', 'volume_SI']
    rename_total_measures = ['total_' + i for i in rename_measures]
    rename_measure_dictionary = dict(zip(measures, rename_measures))
    rename_total_measure_dictionary = dict(zip(rename_measures, rename_total_measures))
    # Region_Group SKU
    df_DC_contribution_final = df_DC_contribution.groupby(dimensions)[measures].sum().reset_index().rename(columns = rename_measure_dictionary)

    df_DC_contribution_final = pd.merge(
                                df_DC_contribution_final,
                                df_DC_contribution_final.groupby(dimensions_total)[rename_measures].sum().reset_index().rename(columns = rename_total_measure_dictionary),
                                on = dimensions_total
                            )

    df_DC_contribution_final['Contribution'] = np.where( df_DC_contribution_final['total_volume_SI'] == 0, 0, df_DC_contribution_final['volume_SI'] / df_DC_contribution_final['total_volume_SI'] )

    df_DC_contribution_final['DC'] = np.where( df_DC_contribution_final['Inventory Org Name'] != 'Unspecified', df_DC_contribution_final['Inventory Org Name'].str[:3], 'Unspecified' ) 
    
    return df_DC_contribution_final[['DC', 'Demand Planning Standard SKU Name', 'Group SKU', 'Region Name', 'Channel Code', 'Contribution']]

def etl_DC_contribution_by_groupSKU(df_DC_contribution):
    dimensions = ['Inventory Org Name', 'Group SKU', 'Region Name', 'Channel Code']
    dimensions_total = list(set(dimensions) - set(['Inventory Org Name']))
    measures = ['PS_S-In Actual Value (Dist. Price) Excluded SCT', 'PS_S-In Actual Qty (Cases)']
    rename_measures = ['value_SI', 'volume_SI']
    rename_total_measures = ['total_' + i for i in rename_measures]
    rename_measure_dictionary = dict(zip(measures, rename_measures))
    rename_total_measure_dictionary = dict(zip(rename_measures, rename_total_measures))
    # Region_Group SKU
    df_DC_contribution_final = df_DC_contribution.groupby(dimensions)[measures].sum().reset_index().rename(columns = rename_measure_dictionary)

    df_DC_contribution_final = pd.merge(
                                df_DC_contribution_final,
                                df_DC_contribution_final.groupby(dimensions_total)[rename_measures].sum().reset_index().rename(columns = rename_total_measure_dictionary),
                                on = dimensions_total
                            )

    df_DC_contribution_final['Contribution'] = np.where( df_DC_contribution_final['total_volume_SI'] == 0, 0, df_DC_contribution_final['volume_SI'] / df_DC_contribution_final['total_volume_SI'] )

    df_DC_contribution_final['DC'] = np.where( df_DC_contribution_final['Inventory Org Name'] != 'Unspecified', df_DC_contribution_final['Inventory Org Name'].str[:3], 'Unspecified' ) 

    return df_DC_contribution_final[['DC', 'Group SKU', 'Region Name', 'Channel Code', 'Contribution']]

def etl_DC_contribution_by_subDivision(df_DC_contribution):
    dimensions = ['Inventory Org Name', 'Sub Division Desc', 'Region Name', 'Channel Code']
    dimensions_total = list(set(dimensions) - set(['Inventory Org Name']))
    measures = ['PS_S-In Actual Value (Dist. Price) Excluded SCT', 'PS_S-In Actual Qty (Cases)']
    rename_measures = ['value_SI', 'volume_SI']
    rename_total_measures = ['total_' + i for i in rename_measures]
    rename_measure_dictionary = dict(zip(measures, rename_measures))
    rename_total_measure_dictionary = dict(zip(rename_measures, rename_total_measures))
    # Region_Group SKU
    df_DC_contribution_final = df_DC_contribution.groupby(dimensions)[measures].sum().reset_index().rename(columns = rename_measure_dictionary)

    df_DC_contribution_final = pd.merge(
                                df_DC_contribution_final,
                                df_DC_contribution_final.groupby(dimensions_total)[rename_measures].sum().reset_index().rename(columns = rename_total_measure_dictionary),
                                on = dimensions_total
                            )

    df_DC_contribution_final['Contribution'] = np.where( df_DC_contribution_final['total_volume_SI'] == 0, 0, df_DC_contribution_final['volume_SI'] / df_DC_contribution_final['total_volume_SI'] )
    
    df_DC_contribution_final['DC'] = np.where( df_DC_contribution_final['Inventory Org Name'] != 'Unspecified', df_DC_contribution_final['Inventory Org Name'].str[:3], 'Unspecified' ) 

    return df_DC_contribution_final[['DC', 'Sub Division Desc', 'Region Name', 'Channel Code', 'Contribution']]

# WEEKLY PHASING
def etl_MUF_withDC(df_master_date, df_MUF, df_DC_contribution_by_DPName, df_DC_contribution_by_groupSKU, df_DC_contribution_by_subDivision):
    df_master_date['weight'] = np.where(df_master_date['Day'] <= 7, 0.7, 
                                        np.where(df_master_date['Day'] <= 21, 1,
                                                1.3)
                                       )

    df_MUF_withDC = pd.merge(
                        pd.merge(
                            df_MUF, 
                            df_DC_contribution_by_DPName,
                            how = 'left',
                            left_on = ['DP Name', 'Group SKU', 'Region Name', 'Channel'],
                            right_on = ['Demand Planning Standard SKU Name', 'Group SKU', 'Region Name', 'Channel Code'],
                            suffixes = ['', '_by_DPName']
                            ),
                        df_DC_contribution_by_groupSKU,
                        how = 'left',
                        left_on = ['Group SKU', 'Region Name', 'Channel'],
                        right_on = ['Group SKU', 'Region Name', 'Channel Code'],
                        suffixes = ['', '_by_groupSKU']
                        )

    df_MUF_withDC['DC'] = np.where(df_MUF_withDC['DC'].notnull(), 
                                   df_MUF_withDC['DC'], 
                                   df_MUF_withDC['DC_by_groupSKU']
                                  )

    df_MUF_withDC['Contribution_byDC'] = np.where(df_MUF_withDC['Contribution'].notnull(), 
                                   df_MUF_withDC['Contribution'], 
                                   df_MUF_withDC['Contribution_by_groupSKU']
                                            )
    return df_MUF_withDC

def etl_WeeklyPhasing(df_MUF_withDC, df_master_date, df_master, df_codeMappingMaster, df_mappingItemCode):

    df_WUF_withDC = pd.merge(
        df_MUF_withDC,
        pd.merge(
            df_master_date[
                (df_master_date['Working Day NPP'] == 'Yes') 
                # & (df_master_date['Iso Weeknum'] <= 10)   
                # & (df_master_date['Year'] == 2023)
            ].groupby(['Iso Weeknum', 'Year', 'Month'])['weight'].sum().reset_index(),
            df_master_date[
            (df_master_date['Working Day NPP'] == 'Yes') 
            # & (df_master_date['Iso Weeknum'] <= 10)     
            # & (df_master_date['Year'] == 2023)
            ].groupby(['Year', 'Month'])['weight'].sum().reset_index(),
            how = 'left',
            on = ['Year', 'Month']
        ).assign(week_rate = lambda x: x['weight_x'] / x['weight_y'])[['Iso Weeknum', 'Year', 'Month', 'week_rate']],
        how = 'left',
        on = ['Year', 'Month']
    )

    df_WUF_withDC['value_SO_WeeklyForecast'] = df_WUF_withDC['Sell Out - Val (Bio)'] * df_WUF_withDC['Contribution_byDC'] * df_WUF_withDC['week_rate'] 
    df_WUF_withDC['volume_SO_WeeklyForecast'] = df_WUF_withDC['Sell Out - Vol (Kcase)'] * df_WUF_withDC['Contribution_byDC'] * df_WUF_withDC['week_rate'] 

    df_WUF_withDC = df_WUF_withDC.groupby(['Group SKU', 'DP Name', 'Channel', 'Region Name', 'Price', 'View By', 'DC', 'Year', 'Iso Weeknum'])[['value_SO_WeeklyForecast', 'volume_SO_WeeklyForecast']].sum().reset_index()

    df_WUF_withDC['yearWeek'] = df_WUF_withDC['Year'] * 100 + df_WUF_withDC['Iso Weeknum']

    # mapping productNumber
    df_mapping_productNumber = pd.merge(
                                    pd.melt(df_codeMappingMaster, 
                                          id_vars=['Group SKU', 'Sub Division Name', 'DPName', 'Channel', 'Start Week', 'End Week'], 
                                          value_vars=['Miền Bắc', 'Miền Trung', 'HCM', 'Miền Đông', 'Miền Tây']
                                         ).rename(columns = {'variable': 'Region Name', 'value': 'Product Number'}),
                                    df_master[['Product Number', 'Group SKU', 'Supply Strategy']].drop_duplicates(),
                                    how = 'left',
                                    on = ['Product Number', 'Group SKU']
                                )

    # logic mapping: if Supply Stratrgy = null => X mappingItemCode X codeMappingMaster; elif Supply Strategy = 'Plan B' => X codeMappingMaster 
    df_weeklyPhasing = pd.concat([
        pd.merge(
            pd.merge(
                df_mapping_productNumber[df_mapping_productNumber['Supply Strategy'].isnull()],
                df_mappingItemCode.rename(columns = {'Item code': 'Region Name'}),
                on = ['Region Name']
            ),
            df_WUF_withDC,
            left_on = ['Group SKU', 'DPName', 'Channel', 'Region Name', 'DC'],
            right_on = ['Group SKU', 'DP Name', 'Channel', 'Region Name', 'DC']
        ),
        pd.merge(
            df_mapping_productNumber[df_mapping_productNumber['Supply Strategy'] == 'Plan B'],
            df_WUF_withDC,
            left_on = ['Group SKU', 'DPName', 'Channel', 'Region Name'],
            right_on = ['Group SKU', 'DP Name', 'Channel', 'Region Name']
        )
        ], 
        ignore_index = True
    )

    # filter period applied for each productNumber
    df_weeklyPhasing = df_weeklyPhasing[ (df_weeklyPhasing['yearWeek'] >= df_weeklyPhasing['Start Week']) & (df_weeklyPhasing['yearWeek'] <= df_weeklyPhasing['End Week']) ] 

    df_weeklyPhasing = pd.melt(
        df_weeklyPhasing,
        id_vars = ['Product Number', 'Group SKU', 'Sub Division Name', 'DP Name', 'Channel', 'Region Name', 'DC', 'Price', 'View By', 'Year', 'Iso Weeknum', 'yearWeek'],
        value_vars = ['value_SO_WeeklyForecast', 'volume_SO_WeeklyForecast']
    ).rename(columns = {'variable': 'UOM'})

    df_weeklyPhasing['UOM'] = np.where(df_weeklyPhasing['UOM'] == 'value_SO_WeeklyForecast', 'Val (Bio)', 'Vol (Kcase)')

    df_weeklyPhasing['Sell Type'] = 'Sell Out'
    
    return df_weeklyPhasing
