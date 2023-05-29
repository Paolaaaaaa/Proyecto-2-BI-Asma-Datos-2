import pandas as pd


def transform_data2021(df):
    columnas_de_interes_2021 = [
    '"NPCEP5"',
    '"NOMBRE_LOCALIDAD"',
    '"NPCEP4"',
    '"MPIO"',
    '"NPCHP18AA"',
    '"NPCHP18AB"',
    '"NPCHP18AC"',
    '"NPCHP18AD"',
    '"NPCHP18AE"',
    '"NPCHP18AF"',
    '"NPCHP18AG"',
    '"NPCHP18AH"',
    '"NPCHP18AI"',
    '"NPCHP18AI1"',
    '"NPCHP18AJ"',
    '"NPCHP18AK"',
    '"NPCHP18AL"',
    '"NPCHP18AM"',
    '"NPCHP18AN"',
    '"NPCHP18AO"']
    df_asma_asma_2021 = df[columnas_de_interes_2021]
    df_asma_bogota_2021=df_asma_asma_2021[df_asma_asma_2021['"MPIO"']=="11001"]
    df_asma_bogota_2021['"ANIO"']="2021"

    df_asma_bogota_2021.rename(columns = {
        '"NPCEP5"':'"sexo"',
    '"NOMBRE_LOCALIDAD"':'"localidad"',
    '"NPCEP4"':'"edad"',
    '"MPIO"':'"municipio"',
    '"NPCHP18AA"':'"Transmilenio"', 
    '"NPCHP18AB"':'"SITP"',
    '"NPCHP18AC"':'"buseta_o_colectivo"', 
    '"NPCHP18AD"': '"automovil_particular"',
    '"NPCHP18AE"': '"Taxi"',
    '"NPCHP18AF"': '"Motocicleta"',
    '"NPCHP18AG"': '"Bicicleta"',
    '"NPCHP18AH"': '"Ruta_escolar"',
    '"NPCHP18AI"': '"A_pie"',
    '"NPCHP18AI1"': '"Bus_intermunicipal"',
    '"NPCHP18AJ"': '"bicitaxi_o_mototaxi"',
    '"NPCHP18AK"': '"particulares_plataformas_o_aplicaciones"',
    '"NPCHP18AL"': '"vehículo_patineta_moto_electicos"',
    '"NPCHP18AM"': '"caballo"',
    '"NPCHP18AN"': '"otros"',
    '"NPCHP18AO"': '"no_se_desplaza"',
                    }, inplace = True)
    

    return df_asma_bogota_2021


def transform_data2017(df):
    columnas_de_interes_2017 = ['"LOCALIDAD_TEX"','"NPCEP5"','"NPCEP4"','"DPTOMPIO"','"NPCFP14B"', '"NPCHP18A"',  '"NPCHP18B"', '"NPCHP18C"', '"NPCHP18D"', '"NPCHP18E"', '"NPCHP18F"', '"NPCHP18G"', '"NPCHP18H"', '"NPCHP18I"', '"NPCHP18J"' , '"NPCHP18K"', '"NPCHP18L"' ,'"NPCHP18M"']
    df_asma_asma_2017 = df[columnas_de_interes_2017]
    df_asma_bogota_2017=df_asma_asma_2017[df_asma_asma_2017['"DPTOMPIO"']=="11001"]
    df_asma_bogota_2017['"ANIO"']="2017"
    df_asma_bogota_2017['"no_se_desplaza"']= "1"
    df_asma_bogota_2017['"vehículo_patineta_moto_electicos"']= "1"
    df_asma_bogota_2017['"particulares_plataformas_o_aplicaciones"']= "1"


    df_asma_bogota_2017.rename(columns = {
        '"NPCEP5"':'"sexo"',
    '"LOCALIDAD_TEX"':'"localidad"',
    '"NPCEP4"':'"edad"',
    '"DPTOMPIO"':'"municipio"',
    '"NPCHP18A"':'"Transmilenio"', 
    '"NPCHP18B"':'"SITP"',
    '"NPCHP18C"':'"buseta_o_colectivo"', 
    '"NPCHP18D"': '"automovil_particular"',
    '"NPCHP18E"': '"Taxi"',
    '"NPCHP18F"': '"Motocicleta"',
    '"NPCHP18G"': '"Bicicleta"',
    '"NPCHP18H"': '"Ruta_escolar"',
    '"NPCHP18I"': '"A_pie"',
    '"NPCHP18AJ"': '"Bus_intermunicipal"',
    '"NPCHP18K"': '"bicitaxi_o_mototaxi"',
    '"NPCHP18L"': '"caballo"',
    '"NPCHP18M"': '"otros"'
   
                    }, inplace = True)

    return df_asma_bogota_2017

def join_df (df_2017,df_2021):
    frames = [df_2017,df_2021]

    return pd.concat(frames)