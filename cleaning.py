import os
import sys
import pandas as pd
from glob import glob
from pathlib import Path
from shapely.geometry import Point
import geopandas as gpd
from tqdm import tqdm
tqdm.pandas()


def list_icrs(afsis_path):
    icrs = [name.stem for name in afsis_path.glob('Bruker*/**/*') if name.is_file()]
    return set(icrs)


def remove_unpaired(afsis_path, unpaired_icrs):
    for name in afsis_path.glob('Bruker*/*'):
        if name.is_file() and (name.stem in unpaired_icrs):
            name.unlink()
            

def prep_csv(csv_path, opus_ssns, renamings={}):
    df = pd.read_csv(csv_path).rename(columns=renamings)
    ssns = set(df['SSN'])
    print(csv_path)
    print(f'DF size before filtering: {df.shape[0]}')
    rows_without_opus = df['SSN'].apply(lambda x: x in opus_ssns)
    print(f'DF size after filtering: {rows_without_opus.sum()}')
    df[rows_without_opus].to_csv(csv_path, index=False)
    return ssns

def row_inside(row, africa_df):
    lat = row['Latitude']
    lon = row['Longitude']
    # print(lat, lon)
    point = Point((lon, lat))
    return africa_df.geometry.apply(lambda x: x.contains(point)).any()


def clean_georefs(shapefile_path, georefs_path):
    georefs_df = pd.read_csv(georefs_path)
    africa_df = gpd.read_file(shapefile_path)
    print(georefs_df.shape[0])
    print(georefs_df.iloc[0])

    points_inside_africa = georefs_df.progress_apply(lambda row: row_inside(row, africa_df), axis=1)
    
    georefs_df[points_inside_africa].to_csv(georefs_path, index=False)
    

def remove_opus_without_georefs(opus_ssns, georefs_path, afsis_path):
    georefs_df = pd.read_csv(georefs_path)
    georefs_ssns = set(georefs_df['SSN'])
    opus_without_georefs = opus_ssns - georefs_ssns 
    remove_unpaired(afsis_path, opus_without_georefs)


def main(afsis_path):
    opus_ssns = list_icrs(afsis_path/'Dry_Chemistry/ICRAF/')

    wetchem_ssns = set()

    paths = ['Georeferences/georeferences.csv', 
        'Wet_Chemistry/CROPNUTS/Wet_Chemistry_CROPNUTS.csv',
        'Wet_Chemistry/ICRAF/Wet_Chemistry_ICRAF.csv',
        'Wet_Chemistry/RRES/Wet_Chemistry_RRES.csv']
    georefs_path = afsis_path / 'Georeferences/georeferences.csv'

    clean_georefs('/home/tom/Desktop/africa/Africa.shp', georefs_path)
    remove_opus_without_georefs(opus_ssns, georefs_path, afsis_path)


    for path in paths:
        full_path = afsis_path / path
        if str(full_path.stem) == 'wet_chemistry_new.csv':
            wetchem_ssns = wetchem_ssns.union(prep_csv(full_path), opus_ssns, {"ICRAF ID": "SSN"})
        else:
            wetchem_ssns =  wetchem_ssns.union(prep_csv(full_path, opus_ssns))

    unpaired_ssns = opus_ssns - wetchem_ssns
    remove_unpaired(afsis_path, unpaired_ssns)

if __name__ == '__main__':
    afsis_path = Path(sys.argv[1])
    main(afsis_path)