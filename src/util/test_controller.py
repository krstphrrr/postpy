# from src.util.ecoregion_mlra import mlra
import pandas as pd
import logging
from src.util.ecoregion_mlra import mlra, header_pk_geometry, geoindicators_mlra, \
  ecoregions, geoindicators_ecoregions
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
  HARDCODED geoindicators
  """
  tmp = model_handler(json,geoIndicators,"geoIndicators","postgresql")
  # setting up mlra 
  mlra_df = mlra()
  # setting up ecoregions
  ecoregion_df = ecoregions()

  # making geoindicators spatially explicit
  spatial_geoind = header_pk_geometry(tmp.checked_df)

  # adding mlra fields to spatial geoind
  mlra_geo = geoindicators_mlra(spatial_geoind, mlra_df, tmp.checked_df)

  # adding ecoregion fields to expanded spatial geoind
  ecoregion_geo = geoindicators_ecoregions(mlra_geo, ecoregion_df, tmp.checked_df)

  # logging.info(ecoregion_geo.iloc[:,148:])

  return ecoregion_geo.iloc[:,148:]


