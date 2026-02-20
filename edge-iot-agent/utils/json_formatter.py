import json 
from datetime import datetime # , timezone
import numpy as np

def encoder(obj):
    # Si l'objet est un float NumPy (float32, float64, etc.)
    if isinstance(obj, np.floating):
      return float(obj)
    
    # Par sécurité, si c'est un entier NumPy
    if isinstance(obj, np.integer):
      return int(obj)
    
    if isinstance(obj, np.ndarray):
      return float(obj[0])
        
    raise TypeError(f"Type {type(obj)} non supporté par l'encodeur")

def init_json(timestamp) -> dict:
  json_data = dict()
  json_data["timestamp"] = int(datetime.fromisoformat(timestamp).timestamp())
  return json_data

def convert_json(json_data):
  return json.dumps(json_data, default=encoder)