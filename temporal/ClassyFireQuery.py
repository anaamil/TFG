import pandas as pd
from glob import glob
import numpy as np
from temporal import Gradient_data


def is_isomeric(smiles):
    """
    Checks if a SMILES string contains isomeric information.

    Args:
        smiles (str): SMILES string to be checked.

    Returns:
        bool: True if the SMILES string contains isomeric information, False otherwise.
    """
    return any(c in smiles for c in ['\\', '/', '@'])


def access_data(pattern="", location=".*", training=True):
    """
    Accesses RepoRT data based on a specified molecule pattern and column.

    This function searches for files in the '../data/*/' directory containing molecule and retention time data.
    It reads the data from these files, filters it based on the provided molecule pattern and column location,
    and merges it with an alternative parents dataset.

    Args:
        pattern (str, optional): Molecule pattern or name to search for in the data. Default value "", representing
        all types of molecules.
        location (str, optional): Column name to search for the pattern. Default value ".*", representing all columns.
        training (bool, optional): Indicates whether training data processing is performed. Default value True.

    Returns:
        DataFrame: Processed DataFrame containing the merged data with its chromatographic information.
    """
    try:
        directory = glob("data/*/*.tsv")
        results = []
        column = None
        alt = pd.read_csv('../RepoRT_classified.tsv', sep='\t', header=0, encoding='utf-8', dtype=object)
        for file in directory:
            df = pd.read_csv(file, sep='\t', header=0, encoding='utf-8')
            if "classyfire.kingdom" in df.columns and not is_isomeric(df['smiles.std'].iloc[0]):
                column = df.filter(regex=f'{location}', axis=1)
                column_string = column.select_dtypes(include=['object'])
                for col in column_string.columns:
                    query = column[col].str.lower().str.contains(pattern.lower(), na=False)
                    if not df[query].empty:
                        df_merge = df[query].merge(alt.drop(columns=[col for col in df[query].columns[1:]] + ["0"]),
                                                   left_on="id", right_on="id",  how="left")
                        results.append(df_merge)
                        break
        if column is not None and column.size == 0:
            print(f"{location} not found")
        elif results:
            df_data = pd.concat(results, axis=0, ignore_index=True)
            df_data["alternative_parents"] = (df_data.iloc[:, 14:].astype(str)
                                              .apply(lambda x: ", ".join(x.drop_duplicates()), axis=1))
            df_data = (df_data.drop(columns=df_data.columns[14:287]).replace("NA (NA)", np.nan)
                       .set_index(df_data["id"].str[0:4].astype(int)))
            column_data = Gradient_data.gradient_data(training)
            df = pd.merge(df_data, column_data, left_index=True, right_index=True, how="inner")
            return df
        else:
            print(f'No matches found with {pattern}')
    except Exception as e:
        print(f"Error:{e}")
