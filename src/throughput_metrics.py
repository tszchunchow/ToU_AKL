import os
import glob
import pandas as pd 
import geopandas as gpd
from utils import *
import logging

def mx_to_ijk(df, metrics, model_code, mode):
    df = pd.melt(df, id_vars = ['origin'], 
                value_vars = df.columns.tolist().remove('origin'), 
                var_name = 'destination', 
                value_name = f'{metrics}_{model_code}_{mode}')
    return df

def get_mx(metrics):

    if metrics in ('Trips', 'Time', 'Distance', 'Toll', 'Fare'):
        counter = 0

        for period in PERIOD_MAP:
            period_code = PERIOD_MAP[period]
            for scenario in SCENARIO_MAP:
                scen_code = SCENARIO_MAP[scenario]
                for mode in MODES_LIST:
                    if not ((metrics == 'Toll') & (mode == 'PT')) or ((metrics == 'Fare') & (mode == 'Car')):
                        mx_code = scen_code*100 + MX_MAP[f"{period}_{mode}_All_{metrics}"]
                        model_code = str(YEAR) + str(period_code) + str(scen_code)

                        for file in glob.glob(MX_DIR + '/*/CSV/*.csv'):
                            if str(mx_code) in file:
                                temp_df = pd.read_csv(file, skiprows=10).rename(columns={'p/q/[val]': 'origin'})
                                temp_df = mx_to_ijk(temp_df, metrics, model_code, mode)
                                temp_df['origin'] = temp_df['origin'].astype(int)
                                temp_df['destination'] = temp_df['destination'].astype(int)

                                if counter == 0:
                                    mx_df = temp_df.copy()
                                else:
                                    mx_df = mx_df.merge(temp_df, on=['origin', 'destination'])
                    
                                counter += 1

        return mx_df
                     
    else:
        return 'Invalid Input'  # or raise an error
