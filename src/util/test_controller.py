import pandas as pd
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
  HARDCODED HEADER
  """
  tmp = model_handler(json,dataHeader,"dataHeader","postgresql")

  return tmp