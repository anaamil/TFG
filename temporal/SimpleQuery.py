import pandas as pd
import sys
from glob import glob
def is_isomeric(smiles):
    return any(c in smiles for c in ['\\', '/', '@'])

def acceso_data(molecula):
    try:
        archivos_directorio = glob('RepoRT/processed_data/*/*_success.tsv')
        resultado=[]
        for file in archivos_directorio:
            df = pd.read_csv(file, sep='\t', header=0, encoding='utf-8')
            if 'formula' in df.columns and 'inchikey.std' in df.columns:
                condicion=df['molecula'] == molecula
                if not df[condicion].empty and is_isomeric(df['smiles.std'].iloc[0]):
                    resultado.append(df[condicion])
        if resultado:
            for i in resultado:
                print(i)
        else:
            print(f"No se han encontrado coincidencias con los filtros de búsqueda para la molécula {molecula}")
    except Exception as e:
        print(f"Error de acceso a los archivos: {e}")

acceso_data(sys.argv[1])
#formula molécula='C37H44O10'