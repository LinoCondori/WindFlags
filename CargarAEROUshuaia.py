import pandas as pd
import numpy as np
from glob import glob
import os
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import BaseDeDatos_Lib_v02 as BD


engine_wind = create_engine('postgresql://postgres:vag@10.30.19.5:5432/GAWUSH_DATABASE')
Tabla_Wind = 'AeroUshuaia'

if __name__ == '__main__':
    AeroUshuaia = pd.read_excel('USHUAIA AERO 2023.xlsx')
    AeroUshuaia['DateTime'] = AeroUshuaia.Fecha + pd.to_timedelta(AeroUshuaia.Hora + 3, unit='H')
    AeroUshuaia['VdGrad'] = AeroUshuaia.Vdir * 10
    AeroUshuaia['ViMS'] = AeroUshuaia.Vint * 5 / 18
    AeroUshuaia = AeroUshuaia[['DateTime', 'VdGrad', 'ViMS']]
    AeroUshuaia.set_index('DateTime', inplace=True)
    BD.Consulta_de_Existencia_Y_Envio_DIAxDIA(AeroUshuaia, engine_wind, Tabla_Wind)
    print("cargado")

