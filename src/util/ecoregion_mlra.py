from pandas.core.frame import DataFrame
from src.util.tools import engine_conn_string
import pandas as pd
import geopandas as gpd
import logging

"""
select * from 
(select geo.mlra_name, dh."PrimaryKey"
from gis.mlra_v42_wgs84 as geo
join public."dataHeader" as dh 
on ST_WITHIN(dh.wkb_geometry, geo.geom));
"""

def mlra()-> gpd.GeoDataFrame:
    """ queries the mlra table into a geopandas dataframe
    """
    try:
        tmp = gpd.GeoDataFrame.from_postgis('select mlrarsym, mlra_name, geom from gis.mlra_v42_wgs84', 
                engine_conn_string("postgresql"), 
                geom_col='geom')
        return tmp
    except Exception as e:
        logging.error(e)

def header_pk_geometry(dataframe:pd.DataFrame) -> gpd.GeoDataFrame :
    """ spatial join between geoindicators and select fields in
    dataheader(primary key and wkb_geometry) to make geoindicators
    spatially explicit.
    
    PARAMS:
    dataframe: pandas dataframe. original unmodified dataframe
    """
    try:
        geoms = gpd.GeoDataFrame.from_postgis("select \"PrimaryKey\",wkb_geometry from public.\"dataHeader\";",eng, geom_col="wkb_geometry")
        fin = geoms.merge(dataframe, on="PrimaryKey")
        return fin
    except Exception as e:
        logging.error(e)

def geoindicators_mlra( 
    spatial_geoindicators:gpd.GeoDataFrame, 
    mlra_df:gpd.GeoDataFrame, 
    columns_df:pd.DataFrame) -> gpd.GeoDataFrame:
    """ spatial join between spatially explicit geoindicators
    and mlra. returns geoindicators + mlrarsym and mlra_name fields
    requires geoIndicators primary keys to be already present 
    on the DB. 

    PARAMS:
    spatial_geoindicators: geoPandas dataframe. product of header_pk_geometry()
    mlra_df: geoPandas dataframe. product of mlra()
    columns_df: pandas dataframe. original unmodified dataframe 
    """
    cols = [i for i in columns_df.columns]
    cols.extend(['mlrarsym','mlra_name'])
    final = gpd.sjoin(spatial_geoindicators, mlra_df, op="intersects")
    return final.loc[:,cols]

