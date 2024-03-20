import re
import pandas as pd
import sys
from glob import glob
import os
import numpy as np


def delete_eluent(gra_data, elu_data):
    """Restructure dataframe with chromatographic columns data

    Keywords arguments:
    gra_data --
    elu_data --
    """
    try:
        gra_data = gra_data.set_index("file")
        result = []
        for pos in range(gra_data.shape[0]):
            row = gra_data.iloc[pos, :]
            if len(row.iloc[1:5][row != 0]) >= 2:
                sort_row = row.iloc[1:5].sort_values(ascending=False)
                drop_columns = sort_row[2:].index
            else:
                sort_row = row.iloc[1:5].sort_values(ascending=False)
                drop_columns = sort_row[2:].index
            gra_drop = row.drop(drop_columns)
            df = pd.concat([elu_data.loc[gra_data.index[0]], gra_drop])
            elu_drop = [i for i in df.index if drop_columns[0][0] in i or drop_columns[1][0] in i]
            elu = df.drop(elu_drop)
            for col in elu.index:
                if f'{gra_drop.index[1][0]}' in col:
                    elu = elu.rename(index={col: f'eluent.1{col[8:]} {pos}'})
                elif f'{gra_drop.index[2][0]}' in col:
                    elu = elu.rename(index={col: f'eluent.2{col[8:]} {pos}'})
            elu = elu.rename(index={f't [min]': f't {pos}', f'flow rate [ml/min]': f'flow_rate {pos}'})
            result.append(elu)
        return result
    except Exception as e:
        print(f"Error delete_eluents: {e}")


def gradient_data():
    """Access to eluents gradient data in each chromatographic column"""
    try:
        directory = glob("../processed_data/*/*.tsv")
        gradient = []
        time_dic = []
        dictionary = {}
        column_data, eluent_data = metadata()
        for file in directory:
            if re.search(r"_gradient.tsv", file):
                gra = pd.read_csv(file, sep='\t', header=0, encoding='utf-8')
                file_name = int(os.path.basename(file)[0:4])
                if gra["t [min]"].isnull().values.any() or gra["t [min]"].values.size == 0:
                    gradient.append(f'experiment nº {file_name}')
                else:
                    dictionary[file_name] = gra["t [min]"].values.max(), gra.values.shape[0]
                    gra["file"] = file_name
                    result = delete_eluent(gra, eluent_data)
                    df_g = pd.DataFrame(pd.concat(result)).transpose()
                    df_merge = pd.merge(column_data, df_g, left_index=True, right_index=True, how="right")
                    col_flowrate = [col for col in df_merge.columns if re.match(r"flow_rate *", col)]

                    for col in col_flowrate:
                        flowrate_null = df_merge[col].isnull()
                        df_merge.loc[flowrate_null, col] = df_merge.loc[flowrate_null, "column.flowrate"]
                    time_dic.append(df_merge)
        df = pd.concat(time_dic, axis=0)
        df_drop = df.iloc[:, 0:8].isnull().sum(axis=1) > 5
        gradient.extend([f'experiment nº {i}' for i in df[df_drop].index.tolist()])
        df = df[~df_drop]
        df.to_csv("../../data.tsv", sep="\t", index=True)
        df_gradient = pd.Series(gradient)
        df_gradient.name = "nº experiments"
        df_gradient.to_csv("../../excluded_files.tsv", index=False)
        df_t = pd.DataFrame(data=dictionary, index=["t_max", "num"])
        df_transpose = df_t.transpose()
        df_transpose.to_csv("../../gradient.tsv", sep="\t", index=True)
        return df
    except Exception as e:
        print(e)


def metadata():
    """Access to chromatographic column data"""
    try:
        directory = glob("../processed_data/*/*.tsv")
        metadata_list = []
        for file in directory:
            if re.search(r"_metadata.tsv", file):
                met = pd.read_csv(file, sep='\t', header=0, encoding='utf-8')
                metadata_list.append(met)
        df_met = pd.concat(metadata_list, ignore_index=True)
        df_filtered = df_met.set_index("id")
        position = [pos for pos, col in enumerate(df_filtered.columns) if "unit" in col]
        for pos in position:
            if df_filtered.iloc[:, pos].notna().any() and df_filtered.iloc[:, pos].str.contains("mM").any():
                if "nh4ac" in df_filtered.columns[pos - 1]:
                    df_filtered.iloc[:, pos - 1] *= 0.007
                elif "nh4form" in df_filtered.columns[pos - 1]:
                    df_filtered.iloc[:, pos - 1] *= 0.005
                elif "nh4carb" in df_filtered.columns[pos - 1]:
                    df_filtered.iloc[:, pos - 1] *= 0.006
                elif "nh4bicarb" in df_filtered.columns[pos - 1]:
                    df_filtered.iloc[:, pos - 1] *= 0.005
                elif "nh4form" in df_filtered.columns[pos - 1]:
                    df_filtered.iloc[:, pos - 1] *= 0.004
                elif "nh4oh" in df_filtered.columns[pos - 1]:
                    df_filtered.iloc[:, pos - 1] *= 0.004
            elif df_filtered.iloc[:, pos].notna().any() and df_filtered.iloc[:, pos].str.contains("µM").any():
                if "phosphor" in df_filtered.columns[pos - 1]:
                    df_filtered.iloc[:, pos - 1] *= 5.21/(10**6)
                elif "medronic" in df_filtered.columns[pos - 1]:
                    df_filtered.iloc[:, pos - 1] *= 8.38/(10**6)
        for column in df_filtered.columns[2:8]:
            lines_null = df_filtered[df_filtered[column].isnull()]
            same_lines = df_filtered[df_filtered['column.name'].isin(lines_null["column.name"])]
            mean = same_lines.groupby('column.name')[column].mean()
            for index, mean in mean.items():
                df_filtered.loc[(df_filtered[column].isnull()) & (df_filtered['column.name'] == index), column] = mean
        columns_unit = [col for col in df_filtered.columns if '.unit' in col or "gradient." in col]
        column_data = df_filtered.iloc[:, 0:8]
        column_data["missing_values"] = column_data.isnull().sum(axis=1)
        eluent_data = df_filtered.iloc[:, 8:].drop(columns=columns_unit)
        df_met.to_csv("../../metadata.tsv", sep="\t", index=True)
        df_filtered.to_csv("../../metadata_filtered.tsv", sep="\t", index=True)
        return column_data, eluent_data
    except Exception as e:
        print(f"Error metadata:{e}")


gradient_data()
