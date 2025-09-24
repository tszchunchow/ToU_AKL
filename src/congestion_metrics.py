import os
import glob
import pandas as pd 
import geopandas as gpd
import duckdb

from utils import *

def read_nodes_and_links():
    
    msm_links_dfs = {}
    counter = 0 

    for period in PERIOD_MAP:
        period_code = PERIOD_MAP[period]
        temp_dfs = []
        for scenario in BASE_SCENARIO_MAP:
            scen_code = BASE_SCENARIO_MAP[scenario]
            model_code = str(YEAR) + str(period_code) + str(scen_code)
            for file in glob.glob(SHAPEFILE_DIR + '/*/*/*.shp'):
                if model_code in file:
                    if file.endswith('emme_links.shp'):
                        temp_dfs.append(gpd.read_file(file)[ROAD_LINKS_BASE_COLS])
                    elif file.endswith('emme_nodes.shp'):
                        # do once only
                        msm_nodes_df = gpd.read_file(file)
                        counter += 1

        msm_links_dfs[period] = pd.concat(temp_dfs).drop_duplicates(subset=['ID', 'INODE', 'JNODE'])
    
    return msm_links_dfs, msm_nodes_df

def map_nodes_to_zone(msm_nodes_df):
    return msm_nodes_df.sjoin(MSM_ZONES, how='left', predicate='within')[['ID', 'MSM2018', 'Sector_Name']].rename(columns={'ID': 'NODE_ID'})

def map_links(msm_links_dfs, node_zone_map):
    mapped_links_dfs = {}
    for period in PERIOD_MAP:
        mapped_links_dfs[period] = msm_links_dfs[period].copy().merge(node_zone_map, left_on='JNODE', right_on='NODE_ID')
    return mapped_links_dfs

def filter_road_links(msm_links_dfs):
    msm_road_links_dfs = {}
    for period in PERIOD_MAP:
        temp_df = msm_links_dfs[period][msm_links_dfs[period]['TYPE'] > 10]
        msm_road_links_dfs[period] = temp_df.merge(LINK_TYPE_MAP, left_on='TYPE', right_on='Link_Type', how='left')

    return msm_road_links_dfs

def rename_columns(suffix, original_cols = ROAD_LINKS_VOL_COLS, exceptions = ["ID"]):
    new_cols = {}
    for col in original_cols:
        if col not in exceptions:
            new_cols[col] = col + "_" + suffix
        else: 
            new_cols[col] = col 
    return new_cols

def get_road_volume_time(links_dfs):
    link_vol_dfs = {}
    for period in PERIOD_MAP:
        period_code = PERIOD_MAP[period]
        base_df = links_dfs[period].copy()
        for scenario in SCENARIO_MAP:
            scen_code = SCENARIO_MAP[scenario]
            model_code = str(YEAR) + str(period_code) + str(scen_code)
            print("Merging for model scenario: " + model_code)
            for file in glob.glob(SHAPEFILE_DIR + '/*/*/*.shp'):
                if model_code in file:
                    if file.endswith('emme_links.shp'):
                        temp_df = gpd.read_file(file)[ROAD_LINKS_VOL_COLS].rename(columns=rename_columns(model_code, ROAD_LINKS_VOL_COLS))
                        base_df = base_df.merge(temp_df, on='ID', how='left')
                        
        link_vol_dfs[period] = base_df
        del base_df

    return link_vol_dfs

def congested_link(row, suffix = ""):
    vcv_col = "@vcv" + suffix
    if row[vcv_col] >= 0.82 and 11<= row['TYPE'] <= 17:
        return 1
    elif row[vcv_col] >= 0.9 and 18 <= row['TYPE'] <=21 :
        return 1
    elif row[vcv_col] >= 0.58 and 23 <= row['TYPE'] <= 27:
        return 1
    elif row[vcv_col] >= 0.46 and row['TYPE']==22:
        return 1
    else:
        return 0

def calculate_congestion_values(road_links_dfs, export = True):
    for period in PERIOD_MAP:
        period_code = PERIOD_MAP[period]
        
        road_links_df = road_links_dfs[period].copy()
        road_links_df['LANE_KM'] = road_links_df['LANES']*road_links_df['LENGTH']
        for scenario in SCENARIO_MAP:
            scen_code = SCENARIO_MAP[scenario]
            model_code = str(YEAR) + str(period_code) + str(scen_code)

            road_links_df['CONGESTED_' + model_code] = road_links_df.apply(lambda row: congested_link(row, "_"+ model_code), axis=1)
            road_links_df['CONG_ROAD_KM_' + model_code] = road_links_df['LENGTH']*road_links_df['CONGESTED_' + model_code]
            road_links_df['CONG_LANE_KM_' + model_code] = road_links_df['LANES']*road_links_df['LENGTH']*road_links_df['CONGESTED_' + model_code]

            road_links_df['VKT_' + model_code] = road_links_df['LENGTH'] * road_links_df['VOLAU_' + model_code]
            road_links_df['VHT_' + model_code] = road_links_df['VOLAU_' + model_code] * road_links_df['TIMAU_' + model_code] / 60
            road_links_df['CONG_VKT_' + model_code] = road_links_df['LENGTH'] * road_links_df['VOLAU_' + model_code] *road_links_df['CONGESTED_' + model_code]
            road_links_df['CONG_VHT_' + model_code] = road_links_df['VOLAU_' + model_code] * road_links_df['TIMAU_' + model_code]*road_links_df['CONGESTED_' + model_code] / 60
        
        road_links_dfs[period] = road_links_df
        
        if export:
            output_dir = './outputs/congestion_metrics'
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            road_links_df.to_file(f'{output_dir}/msm_links_{period}.gpkg')

    return road_links_dfs

def generate_congestion_metrics(road_links_dfs, export = True):
    congestion_metrics_dfs = {}
    for period in PERIOD_MAP:
        period_code = PERIOD_MAP[period]
        road_links_df = road_links_dfs[period].copy()

        agg_metrics = {}
        agg_metrics['LENGTH'] = 'sum'
        agg_metrics['LANE_KM'] = 'sum'

        for scenario in SCENARIO_MAP:
            scen_code = SCENARIO_MAP[scenario]
            model_code = str(YEAR) + str(period_code) + str(scen_code)

            agg_metrics.update({'VKT_' + model_code: 'sum', 'VHT_' + model_code: 'sum',
                                'CONG_ROAD_KM_' + model_code :'sum', 'CONG_LANE_KM_' + model_code :'sum',
                                'CONG_VKT_' + model_code: 'sum', 'CONG_VHT_' + model_code: 'sum'})
            
            
        congestion_metrics = road_links_df.groupby(['Group_2', 'Sector_Name']).agg(agg_metrics).reset_index()
        del road_links_df

        # for scenario in SCENARIO_MAP:
        #     scen_code = SCENARIO_MAP[scenario]
        #     model_code = str(YEAR) + str(period_code) + str(scen_code)
        #     congestion_metrics['CONG_LENGTH_%_' + model_code] = congestion_metrics['CONG_ROAD_KM_' + model_code]/congestion_metrics['LENGTH']
        #     congestion_metrics['CONG_LANE_%_' + model_code] = congestion_metrics['CONG_LANE_KM_' + model_code]/congestion_metrics['LANE_KM']

        congestion_metrics_dfs[period] = congestion_metrics
        if export:
            output_dir = './outputs/congestion_metrics'
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            congestion_metrics.to_parquet(f'{output_dir}/congestion_metrics_{period}.parquet')

    return congestion_metrics_dfs