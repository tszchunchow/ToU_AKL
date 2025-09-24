# MAPS
import pandas as pd
import geopandas as gpd

# MAPS
INPUT_DIR = '../inputs'
SHAPEFILE_DIR = '/Users/tszchun.chow/Library/CloudStorage/OneDrive-SharedLibraries-AucklandTransport/Congestion Charging - General/3. Policy & Strategy/Scheme analysis/Phase 1 Modelling outputs/007_Shapefiles'
MX_DIR = '/Users/tszchun.chow/Library/CloudStorage/OneDrive-SharedLibraries-AucklandTransport/Congestion Charging - General/3. Policy & Strategy/Scheme analysis/Modelling outputs/008_Matrices'
OUTPUT_DIR = '../outputs'
CONGESTION_METRICS_DIR = OUTPUT_DIR + "/congestion_metrics"
THROUGHPUT_METRICS_DIR = OUTPUT_DIR + "/throughput_metrics"
ACCESSIBILITY_METRICS_DIR = OUTPUT_DIR + "/accessibility_metrics"
OPTION_COMPARISON_PATH = '/Users/tszchun.chow/Library/CloudStorage/OneDrive-SharedLibraries-AucklandTransport/Congestion Charging - General/Lead Advisor (shared folder)/02 Analysis (Transport Modelling)/Options Assessment/Options_Comparison.xlsx'

BASE_SCENARIO_MAP = {'Do Minimum': 16,
                     'Do Minimum 3C': 76}

TEST_MAP = {'Do Minimum': 16,
                'Option 1A': 50,
                'Option 3C': 70
                }

SCENARIO_MAP = {'2018': 12,
                'Do Minimum': 16,
                'Do Minimum 3C': 76,
                'Option 1A': 50,
                'Option 1B': 52,
                'Option 1C': 54,
                'Option 2A': 56, 
                'Option 2B': 58,
                'Option 3A': 64,
                'Option 3B': 66,
                'Option 3C': 74, 
                'Option 3D': 70, 
                'Option 3E': 72,
                'Option 2C': 78,
                'Option 3F': 80,
                'Option 3G': 82
                }
SCENARIO_LIST = [x for x in SCENARIO_MAP]

PERIOD_MAP = {'AM': 1, 'IP': 2, 'PM': 3}
PERIOD_LIST = [x for x in PERIOD_MAP]

REVERSE_PERIOD_MAP = {v: k for k, v in PERIOD_MAP.items()}
REVERSE_SCENARIO_MAP = {v: k for k, v in SCENARIO_MAP.items()}

scenario_P1_df = pd.read_csv('../inputs/scenario_map/Phase1-0_Scenario_Map.csv')
SCENARIO_P1_MAP = scenario_P1_df[['Scenario_Code', 'Slot No.']].set_index('Scenario_Code')['Slot No.'].to_dict()
REVERSE_SCENARIO_P2_MAP = {v: k for k, v in SCENARIO_P1_MAP.items()}

scenario_P2_df = pd.read_csv('../inputs/scenario_map/Phase2-0_Scenario_Map.csv')
SCENARIO_P2_MAP = scenario_P2_df[['Scenario_Code', 'Slot No.']].set_index('Scenario_Code')['Slot No.'].to_dict()
REVERSE_SCENARIO_P2_MAP = {v: k for k, v in SCENARIO_P2_MAP.items()}

scenario_P2_1_df = pd.read_csv('../inputs/scenario_map/Phase2-1_Scenario_Map.csv')
SCENARIO_P2_1_MAP = scenario_P2_1_df[['Scenario_Code', 'Slot No.']].set_index('Scenario_Code')['Slot No.'].to_dict()
REVERSE_SCENARIO_P2_1_MAP = {v: k for k, v in SCENARIO_P2_1_MAP.items()}

LINK_TYPE_MAP = pd.read_csv(INPUT_DIR + '/msm_link_types/link_types.csv')

LOCAL_BOARD_AREAS = gpd.read_file(INPUT_DIR + '/local_board_areas/akl_lba.geojson').to_crs('epsg:2193')
LOCAL_BOARD_AREAS['Local Board Area'] = LOCAL_BOARD_AREAS['TALB2021_V1_00_NAME'].str.replace(" Local Board Area", "")
LOCAL_BOARD_AREAS['LBA_2'] = LOCAL_BOARD_AREAS['TALB2021_V1_00_NAME_ASCII'].str.replace(" Local Board Area", "").str.replace("-", " - ")

SECTOR_MAP = {1: 'Rodney', 2: 'Rodney', 
              3: 'Hibiscus Coast', 
              4: 'North Shore', 5: 'North Shore',
              6: 'Waitakere', 7: 'Waitakere',
              8: 'CBD', 9:'City Fringe',
              10: 'Isthmus West', 11:'Isthmus Central', 12:'Isthmus East',
              13: 'East Auckland', 14: 'East Auckland',
              15: 'South Auckland', 16: 'South Auckland', 17: 'South Auckland',
              18: 'Other Areas'}

SECTOR_MSMZONE_MAP = pd.read_csv(INPUT_DIR + '/msm_zones/msmzone_lba_sector_crosswalk.csv')
# SECTOR_MSMZONE_MAP['Sector'] = SECTOR_MSMZONE_MAP['Sector'].str.split("ga").str[-1].astype(int).fillna(18)
# SECTOR_MSMZONE_MAP['Sector_Name'] = SECTOR_MSMZONE_MAP['Sector'].map(SECTOR_MAP).fillna('Other Areas')


MSM_ZONES = gpd.read_file(INPUT_DIR + '/msm_zones/MSM2018_zones.shp').merge(SECTOR_MSMZONE_MAP, on='MSM2018')
# MSM_ZONES['geometry'] = MSM_ZONES['geometry'].force_2d()

ROAD_LINKS_BASE_COLS = ['ID', 'INODE', 'JNODE', 'LENGTH', 'TYPE', 'MODES', 'LANES', 'geometry']
ROAD_LINKS_VOL_COLS =  ['ID', 'VOLAX', 'VOLAU', 'VOLAD', 'TIMAU', '@vcv']

YEAR = 26

MX_DF = pd.read_csv(INPUT_DIR + '/mx_map/mx_map.csv')
MX_DF['Desc'] = MX_DF['Period'] + "_" + MX_DF["Mode"] + "_" + MX_DF['Trip_Purpose'] + "_" + MX_DF['Value']
MX_DF = MX_DF.set_index('Desc')[['MX_ID']]
MX_MAP = MX_DF.to_dict()['MX_ID']

MX_DF_2 = pd.read_csv(INPUT_DIR + '/mx_map/mx_map_2.csv')
MX_DF_2 = MX_DF_2.fillna("")
MX_DF_2['Desc'] = MX_DF_2["Mode"] + "_" + MX_DF_2['Trip_Purpose'] + "_" + MX_DF_2['Value'] + "_" + MX_DF_2['Congested']
MX_DF_2 = MX_DF_2.set_index('Desc')[['MX_ID']]
MX_MAP_2 = MX_DF_2.to_dict()['MX_ID']

MODES_LIST = ['Car', 'PT']