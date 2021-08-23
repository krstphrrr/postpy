from src.util.ecoregion_mlra import returnFields
import pandas as pd

from src.util.ecoregion_mlra import returnFields
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
  mlra = returnFields('mlra')
  eco = returnFields('ecolevels')
  tmp = joiner(tmp.checked_df, mlra)
  tmp = joiner(tmp,eco)

  return tmp


def joiner(srcdf, geodf):
  ret = pd.merge(srcdf, geodf, how="inner", on=["PrimaryKey"])
  return ret

