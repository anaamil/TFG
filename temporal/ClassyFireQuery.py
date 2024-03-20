import re
import pandas as pd
import sys
from glob import glob
import os
import numpy as np
import Gradient_data


def is_isomeric(smiles):
    """Return isomeric molecules

    Keywords arguments:
    smiles -- smiles molecule information
    """
    return any(c in smiles for c in ['\\', '/', '@'])


def access_data(pattern, location=".*"):
    """Access to specific RepoRT data and return a complete dataframe

    Keyword arguments:
    pattern -- specific molecule type
    location -- specific column of the dataframe (default * -- every column)
    """
    try:
        directory = glob("../processed_data/*/*.tsv")
        results = []
        alt = pd.read_csv("../../ReopRT_classified.tsv", sep='\t', header=0, encoding='utf-8')
        for file in directory:
            df = pd.read_csv(file, sep='\t', header=0, encoding='utf-8')
            if "classyfire.kingdom" in df.columns and not is_isomeric(df['smiles.std'].iloc[0]):
                column = df.filter(regex=f'{location}', axis=1)
                column_string = column.select_dtypes(include=['object'])
                for col in column_string.columns:
                    query = column[col].str.lower().str.contains(pattern.lower(), na=False)
                    if not df[query].empty:
                        df_merge = df[query].merge(alt.drop(columns=[col for col in df[query].columns[1:]] + ["0"]), left_on="id", right_on="id",  how="left")
                        results.append(df_merge)
                        break
        if column.size == 0:
            print(f"{location} not found")
        elif results:
            df_data = pd.concat(results, axis=0, ignore_index=True)
            df_t_data = df_data.T
            for col in df_t_data.columns:
                df_t_data[col].iloc[df_t_data[col].duplicated()] = np.nan
                val_null = df_t_data[col].iloc[14:][df_t_data[col].iloc[14:].isna()]
                val_notnull = df_t_data[col].iloc[14:].dropna()
                df_t_data[col].iloc[14:] = pd.concat([val_notnull, val_null], ignore_index=True)
            df_data = df_t_data.T.replace("NA (NA)", np.nan).set_index("id")
            df_data.to_csv("../../classyfire_data.tsv", sep="\t", index=True)
            column_data = Gradient_data.gradient_data()
            df_data["index"] = df_data.index.str[0:4].astype(int)
            df = pd.merge(df_data, column_data, left_on="index", right_index=True, how="left")
            df.drop(columns=["index"], inplace=True)
            df.to_csv("../../final_data.tsv", sep='\t', index=True)
        else:
            print(f'No matches found with {pattern}')
    except Exception as e:
        print(f"Error:{e}")


access_data("alcohols")
