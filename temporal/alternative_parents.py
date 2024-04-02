import pandas as pd
import re
from glob import glob


def alternative_parents():
    """
    Obtains alternative parents data of the molecules in RepoRTs processed data

    This function searches alternative parents data from "all_classified.tsv" that contains molecules from RepoRT
    processed data. It reads the data and matches the InChIKey values in the processed data with the InChIKey
    values in the 'all_classified.tsv' file. It creates a DataFrame with the matched records and saves it as
    'RepoRT_classified.tsv'.

    Returns:
        DataFrame: DataFrame containing the matched records.
    """
    try:
        directory = glob("data/*/*.tsv")
        results = []
        df_list = []
        for files in directory:
            if re.search(r"_rtdata_canonical_success.tsv", files):
                df_rt = pd.read_csv(files, sep='\t', header=0, encoding='utf-8')
                results.append(df_rt)
        if results:
            df_concat = pd.concat(results, axis=0, ignore_index=True)
            file = open("../../all_classified.tsv", 'r')
            for i, line in enumerate(file):
                lines = line.strip("\n").split("\t")
                df_query = df_concat[df_concat["inchikey.std"].str.contains(lines[0])]
                if not df_query.empty:
                    df_query.loc[0:, "inchikey.std"] = lines[0]
                    df_at = (pd.DataFrame(lines)).transpose()
                    df_list.append(pd.merge(df_query, df_at, right_on=0, left_on="inchikey.std"))
            df = pd.concat(df_list, ignore_index=True)
            # df.to_csv("../RepoRT_classified.tsv", sep="\t", index=False)
            return df
    except Exception as e:
        print(f"Error: {e}")
