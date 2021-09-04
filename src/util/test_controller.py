# from src.util.ecoregion_mlra import mlra
import pandas as pd
import logging
from src.util.ecoregion_mlra import mlra, header_pk_geometry, geoindicators_mlra
from src.projects.talltables_handler import model_handler
from src.projects.models.header import dataHeader 
from src.projects.models.gap import dataGap
from src.projects.models.lpi import dataLPI
from src.projects.models.height import dataHeight
from src.projects.models.soilstability import dataSoilStability
from src.projects.models.geospecies import geoSpecies
from src.projects.models.geoindicators import geoIndicators
from src.projects.models.speciesinventory import dataSpeciesInventory


def controller(json):
  """
  HARDCODED GAP
  """
  tmp = model_handler(json,dataGap,"dataGap","postgresql")
  # setting up mlra fields
  m = mlra()
  h = header_pk_geometry(tmp)
  df = geoindicators_mlra(h,m,tmp)

  logging.info(df.iloc[:2,:])

  return None


def joiner(srcdf, geodf):
  ret = pd.merge(srcdf, geodf, how="inner", on=["PrimaryKey"])
  return ret

