import pandas as pd
import re
from glob import glob


def alternative_parents():
    """Concatenate data between RepoRT repository and alternative parents data"""
    try:
        directory = glob("../processed_data/*/*.tsv")
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
            df.to_csv("alternative_parents.tsv", sep="\t", index=False)
    except Exception as e:
        print(f"Error: {e}")


alternative_parents()
