import pandas as pd
import numpy as np
from glob import glob
import os
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import BaseDeDatos_Lib_v02 as BD



engine_wind = create_engine('postgresql://postgres:vag@10.30.19.5:5432/GAWUSH_DATABASE')
Tabla_Wind = 'SIAP'
Tabla_Wind_Aero = 'AeroUshuaia'


inicio = pd.to_datetime('2024-01-01 00:00')
fin = pd.to_datetime('2024-10-01 00:00')

engine_Final = create_engine('postgresql://postgres:vag@10.30.19.5:5432/GAWUSH_PROCESADOS')
Tabla_Data_Final = 'WindFlags'


def Flag_Wind(df, flag, Vmin, Vmax, AngMin, AngMax):
    if(AngMin<AngMax):
        df_aux = ((df.ViMS>=Vmin) & (df.ViMS< Vmax) & (df.VdGrad > AngMin) & (df.VdGrad <=AngMax))*(flag+1000)
    if (AngMin > AngMax):
        df_aux = ((df.ViMS >= Vmin) & (df.ViMS < Vmax) & ((df.VdGrad > AngMin) | (df.VdGrad <= AngMax))) * (flag + 1000)
    df_aux = pd.DataFrame(df_aux, columns={'FlagWind'})
    df_aux.replace(0, np.nan, inplace=True)
    return df_aux.dropna()

def Flag_Final(df):
    #df.Flag = (df.FlagManual.isna()*(df.FlagSiap==1999))*df.FlagMeteo*1 + (df.FlagManual.isna())*(df.FlagSiap!=1999)*df.FlagSiap
    df.Flag.update(df.FlagSiap.loc[df.index < pd.to_datetime('2023-11-01')])
    df.Flag.update(df.FlagMeteo.loc[(df.index >= pd.to_datetime('2023-11-01')) & (df.index < pd.to_datetime('2024-01-03'))])
    df.Flag.update(df.FlagSiap.loc[df.index >= pd.to_datetime('2024-01-03')])
    return df


if __name__ == '__main__':
    SIAP = BD.buscarEnBaseDeDatos(engine_wind, Tabla_Wind, inicio, fin)
    WindFlags = BD.buscarEnBaseDeDatos(engine_Final, Tabla_Data_Final, inicio, fin)
    WindFlags.set_index("DateTime", inplace=True)
    WindFlags = WindFlags.reindex(pd.date_range(start=inicio, end=fin, freq='T', closed='left'),)
    WindFlags.index.names = ['DateTime']

    AeroUshuaia = BD.buscarEnBaseDeDatos(engine_wind, Tabla_Wind_Aero, inicio, fin)

    #Clasificar de acuerdo al viento
    #MeteoAux = pd.DataFrame(index=pd.date_range(start=inicio, end=fin, freq='T', closed='left'), columns=['FlagWind'])
    AeroUshuaia['FlagWind'] = np.nan
    AeroUshuaia.FlagWind.update(Flag_Wind(AeroUshuaia, 188, 0, 2.5, 0, 360).FlagWind)
    AeroUshuaia.FlagWind.update(Flag_Wind(AeroUshuaia, 189, 2.5, 99, 300, 200).FlagWind)
    AeroUshuaia.FlagWind.update(Flag_Wind(AeroUshuaia, 0, 2.5, 99, 200, 300).FlagWind)
    AeroUshuaia.rename(columns={'FlagWind': 'FlagMeteo'}, inplace=True)
    AeroUshuaia.set_index("DateTime", inplace=True)
    AeroUshuaia.index.names = ['DateTime']
    AeroUshuaia = AeroUshuaia.reindex(
        index=pd.date_range(start=AeroUshuaia.index[0], end=AeroUshuaia.index[-1], freq='T',), method='nearest' )


    WindFlags.FlagMeteo = AeroUshuaia.FlagMeteo#.update(AeroUshuaia.FlagMeteo)

    FlagAux = pd.DataFrame(index=pd.date_range(start=inicio, end=fin, freq='T', closed='left'), columns=['FlagWind'])
    FlagAux.FlagWind.update(Flag_Wind(SIAP, 188, 0, 2.5, 0, 360).FlagWind)
    FlagAux.FlagWind.update(Flag_Wind(SIAP, 189, 2.5, 99, 300, 200).FlagWind)
    FlagAux.FlagWind.update(Flag_Wind(SIAP, 0, 2.5, 99, 200, 300).FlagWind)
    FlagAux.FlagWind.fillna(1999, inplace=True)
    FlagAux.FlagWind.name = 'FlagSiap'
    WindFlags.FlagSiap.update(FlagAux.FlagWind)

    #BD.Consulta_de_Existencia_Y_Envio_DIAxDIA(WindFlags,engine_Final,Tabla_Data_Final)

    WindFlags = Flag_Final(WindFlags)
    WindFlags.Flag = pd.to_numeric(WindFlags.Flag)
    axes1 = plt.subplot(311)
    axes1.set_title('Viento (Rapidez) [m/s]')
    axes1.set_xlim([inicio, fin])
    axes1.set_ylim([0, 40])
    plt.scatter(SIAP.DateTime, SIAP.ViMS, c='grey', s=10, alpha=0.5)
    plt.scatter(AeroUshuaia.index, AeroUshuaia.ViMS, c='blue', s=10, alpha=0.5)


    axes2 = plt.subplot(312, sharex=axes1)
    axes2.set_title('Viento (Direccion) [Grados]')
    axes2.set_xlim([inicio, fin])
    plt.plot(SIAP.DateTime, SIAP.VdGrad, linestyle='', marker='.')
    plt.plot(AeroUshuaia.index, AeroUshuaia.VdGrad, linestyle='', marker='.')

    axes3 = plt.subplot(313, sharex=axes1)
    axes3.set_title('Viento con Banderas')
    axes3.set_xlim([inicio, fin])
    plt.scatter(WindFlags.index, WindFlags.FlagSiap, c='blue', s=15, alpha=0.1)
    plt.scatter(WindFlags.index, WindFlags.FlagMeteo, c='orange', s=10, alpha=0.1)

    plt.show()

    BD.Consulta_de_Existencia_Y_Envio_DIAxDIA(WindFlags, engine_Final, Tabla_Data_Final)

