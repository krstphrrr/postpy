from src.util.tools import engine_conn_string
import pandas as pd
import geopandas as gpd

"""
select * from 
(select geo.mlra_name, dh."PrimaryKey"
from gis.mlra_v42_wgs84 as geo
join public."dataHeader" as dh 
on ST_WITHIN(dh.wkb_geometry, geo.geom));
"""

def returnFields(which_map, df = None):
    """
    utility function to add data from fields produced by a spatial join, 
    to datasets incoming for ingestion.

    """
    maps = {
        "mlra":"mlra_v42_wgs84",
        "ecolevel1":"us_eco_level_4",
        "ecolevel2":"us_eco_level_4",
        "ecolevel3":"us_eco_level_4",
        "ecolevel4":"us_eco_level_4",
        "ecolevels":"us_eco_level_4"
    }
    which_field= {
        "mlra":"mlra_name",
        "ecolevel1":"na_l1name",
        "ecolevel2":"na_l2name",
        "ecolevel3":"us_l3name",
        "ecolevel4":"us_l4name",
        "ecolevels":""
    }
    result_set = {
    "mlra":["PrimaryKey", which_field[which_map]],
    "ecolevel1":["PrimaryKey",which_field[which_map]],
    "ecolevel2":["PrimaryKey",which_field[which_map]],
    "ecolevel3":["PrimaryKey",which_field[which_map]],
    "ecolevel4":["PrimaryKey",which_field[which_map]],
    "ecolevels":["PrimaryKey",
                  which_field["ecolevel1"],
                  which_field["ecolevel2"],
                  which_field["ecolevel3"],
                  which_field["ecolevel4"]]
    }
    if df is None:
        """
        returns a datafrane with all the pk's currently in header + the field data corresponding 
        to ecoregion level or mlra. useful for adding the fields to already existing pg data or
        debugging.

        """
        poly = gpd.GeoDataFrame.from_postgis(f'select * from gis.{maps[which_map]}', engine_conn_string("postgresql"), geom_col='geom')
        points = gpd.GeoDataFrame.from_postgis('select * from "dataHeader"', engine_conn_string("postgresql"), geom_col='wkb_geometry')
        join = gpd.sjoin(poly,points, how="inner", op="intersects")
        return join.loc[:,result_set[which_map]]

    elif df is not None and isinstance(df,pd.DataFrame):
        """
        if supplied dataframe with primarykeys OR/AND a geometry field, 
        it will return a dataframe with the additional fields: na_l1name, 
        na_l2name, us_l3name, us_l4name and mlra. 
        """
        
        poly = gpd.GeoDataFrame.from_postgis(f'select * from gis.{maps[which_map]}', engine_conn_string("postgresql"), geom_col='geom')
        points = gpd.GeoDataFrame.from_postgis('select * from "dataHeader"', engine_conn_string("postgresql"), geom_col='wkb_geometry')
        dbjoin = gpd.sjoin(poly,points, how="inner", op="intersects")

        # test for df.pk and/or df.geometry
        # join 
        # return df with fields
        