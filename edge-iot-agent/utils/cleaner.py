import numpy as np
from scipy.signal import medfilt

# TODO : si valeur hr a 0 (ca va arriver) ne pas enregistrer dans l'historique et quand meme traiter l'erreur
# TODO : envoyer les erreurs sur un topic different
# TODO : faire les bornes et thresholds


MAX_LEN_HISTORY = 50

history = dict()

'''
for i in DATA_TYPES:
      new_value = data[i]
      if i != "energy_kwh":
        try:
          new_value = cleaner.clean_none_value(new_value, values[room][i])
          (new_value, old_value) = cleaner.median_filter(new_value, i, values[room][i])
          logger.info(f"Filtre median appliqué pour : {i}")
          if old_value != None: logger.warning(f"Changement : {old_value} -> {new_value}")

        except KeyError: # 1er passage, pas d'historique
          pass
        except Exception as e:
          logger.error(f"{e}")
      
      json_data[i] = new_value


      # Sauvegarde de l'historique
      try:
        try: # En temps normal
          values[room][i].append(new_value)
          if len(values[room][i]) > HISTORY_MAX_DEPTH:
            values[room][i] = values[room][i][-HISTORY_MAX_DEPTH:]
        except: # Si l'historique de la data existe pas
          values[room][i] = [new_value]
      except: # Si l'historique de piece existe pas
        values[room] = dict()
        values[room][i] = [new_value]
'''

# DATA_TYPES = ["temperature", "humidity", "co2", "pm25", "co", "tvoc", "temp_ext", "humidity_ext"]
# VALID_SENSORS_RANGES = {
#     "temperature": (-20, 50),    # °C - réaliste intérieur
#     "temp_ext": (-20, 50),
#     "humidity": (0, 100),        # % - physique
#     "humidity_ext": (0, 100),        # % - physique
#     "co2": (300, 5000),          # ppm - min atmosphérique, max capteur
#     "pm25": (0, 500),            # µg/m³ - max capteur
#     "co": (0, 100),              # ppm - max sécurité
#     "tvoc": (0, 10000),          # µg/m³ - max capteur
# }
# THRESHOLDS = {
#   "temperature": 3.0,    # Un saut de 3°C en 5s est physiquement quasi-impossible indoors
#   "temp_ext": 3.0,
#   "humidity": 5.0,       # L'humidité peut varier vite (douche/cuisine), mais pas de 5% par seconde
#   "humidity_ext": 5.0,        # % - physique
#   "co2": 250.0,          # Le CO2 monte vite avec l'occupation, mais 250ppm est une marge de bruit sûre
#   "pm25": 50.0,          # Très volatil (fumée/cuisine), un seuil trop bas (10) créera trop d'alertes
#   "co": 2.0,             # Le CO doit être stable près de 0. Un saut de 2ppm est déjà suspect
#   "tvoc": 1500.0
# }

def get_dynamic_threshold(history, k=5):
  median = np.median(history)
  mad = np.median(np.abs(history - median))
  return k * (mad * 1.4826)



def median_filter(new_data: float, data_type: str, hist: list, return_med_filt = False):
# global last_values, nb_outliers
  history = hist

  # if (data_type not in DATA_TYPES) : return (new_data, None)

  if (new_data == None) : return (None, None)

  try:
    start_idx = len(history)
    history.append(new_data)
    # min_limit, max_limit = VALID_SENSORS_RANGES[data_type]

    # ==== filtre des valeurs initiales


    # if len(history) == 1 :
    #   if new_data < min_limit or new_data > max_limit:
    #     return (None, new_data)
    #   return (new_data, None)
    
    # if new_data < min_limit or new_data > max_limit:
    #   new_data = history[-2]

    # if len(history) < 20:
    #   thresh = THRESHOLDS[data_type]
    #   if abs(new_data - history[-2]) > (thresh * 5):
    #     return (history[-2], new_data)
    #   else:
    #     return (new_data, None)
    
    if len(history) >= 20:
      arr_full = np.array(history, dtype=float)
      med_filtered = medfilt(history, kernel_size=5)

      threshold = get_dynamic_threshold(history)
      # print(f"[INFO] [THRESHOLD] [{data_type}] {threshold}")

      raw_new = arr_full[start_idx:]
      filtered_new = med_filtered[start_idx:]

      diff = np.abs(raw_new - filtered_new)
      outliers_detectes = diff > threshold

      outliers = raw_new[outliers_detectes].tolist()

      if not(outliers): return (new_data, None)
      elif new_data == outliers[-1]: return (med_filtered[-1], outliers[0] if len(outliers) >0 else None)
      else: return (new_data, None)

      # if len(outliers) > 0: print(f"[INFO] [{data_type}] Détection de {len(outliers)} outliers {outliers} dans {new_data}")

      # return (med_filtered[-1], outliers[0] if len(outliers) > 0 else None)
    else:
      return (new_data, None)
  except Exception as e:
    print(f"ERROR : {e}")
    return (new_data, None)

def avg(data: list) -> float:
  count = len(data) - data.count(None)
  if count == 0: return None
  mean = sum(dat for dat in data if dat != None)/count
  return round(mean, 3)

def recur_rmv_none_values(histo):
  if len(histo) == 1: return (histo[0], histo)
  else:
    if histo[0] != None:
      return (histo[0], [histo[0]] + recur_rmv_none_values(histo[1:])[1])
    else:
      n,m = recur_rmv_none_values(histo[1:])
      return (n, [n] + m)

def recur_rmv_none_values_opp(histo):
  if len(histo) == 1: return (histo[0], histo)
  else:
    if histo[-1] != None:
      return (histo[-1], recur_rmv_none_values_opp(histo[:-1])[1] + [histo[-1]])
    else:
      n,m = recur_rmv_none_values_opp(histo[:-1])
      return (n, m + [n])

def remove_none_values(histo):
  if histo.count(None) == len(histo): return histo
  cleaned_data = recur_rmv_none_values(histo)[1]
  if histo[-1] == None:
    cleaned_data = recur_rmv_none_values_opp(cleaned_data)[1]
  return cleaned_data

def clean_none_value(value, histo):
  if value != None: return value
  elif len(histo) < 1: return 0.0
  else: return histo[-1]

def clean_value(value, type):
  try:
    history[type][0]
  except:
    history[type] = list()

  new_value = clean_none_value(value, history[type])
  (new_value, old_value) = median_filter(new_value, type, history[type])
  if old_value != None: print(f"Changement de valeur : {old_value} -> {new_value}")

  if new_value > 0.0:
    history[type].append(value)
    if len(history[type]) > MAX_LEN_HISTORY:
      history[type] = history[type][-MAX_LEN_HISTORY:]

  return new_value