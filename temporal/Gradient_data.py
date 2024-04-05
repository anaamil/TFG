import re
import pandas as pd
from glob import glob
import os
import numpy as np


def delete_eluent(gra_data, elu_data):
    """
    Processes gradient and eluent data.

    This function retains the data for the two most concentrated eluents and removes the remaining ones.

    Args:
        gra_data (DataFrame): A DataFrame containing gradient data
        elu_data (DataFrame): A DataFrame containing eluent data

    Returns:
        list: A list containing processed gradient and eluent data from each experiment
    """
    try:
        gra_data = gra_data.set_index("file")
        result = []
        for pos in range(gra_data.shape[0]):
            row = gra_data.iloc[pos, :]
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


def gradient_data(training):
    """
    Access to data related to gradient used in chromatography

    This function reads gradient data from TSV files in the '../data/*/' directory,
    concatenates them into a single DataFrame, and merges them with chromatographic column metadata.
    It obtains the maximum and minimum gradients, time intervals, and files to exclude if training is enabled.

    Args:
        training(bool): Indicates whether to perform training data processing

    Returns:
        DataFrame: A DataFrame containing processed gradient data merged with chromatographic column metadata
    """
    try:
        directory = glob("data/*/*.tsv")
        excluded_files = []
        list_gra = []
        drop_file = []
        gradient_time = {}
        flowrate_null = {}
        column_data, eluent_data = metadata()
        for file in directory:
            if re.search(r"_gradient.tsv", file):
                gra = pd.read_csv(file, sep='\t', header=0, encoding='utf-8')
                file_name = int(os.path.basename(file)[0:4])
                if gra["t [min]"].isnull().values.any() or gra["t [min]"].values.size == 0:
                    excluded_files.append(f'experiment nº {file_name}')
                    drop_file.append(file_name)
                else:
                    gradient_time[file_name] = gra["t [min]"].values.max(), gra.values.shape[0]
                    gra["file"] = file_name
                    result = delete_eluent(gra, eluent_data)
                    df_g = pd.DataFrame(pd.concat(result)).transpose()
                    col_flowrate = df_g.filter(regex="flow_rate *", axis=1)
                    flowrate_null[df_g.index[0]] = df_g[col_flowrate.columns].isnull().columns
                    list_gra.append(df_g)
        df_g = pd.concat(list_gra)
        df = pd.merge(column_data, df_g, left_index=True, right_index=True, how="left")
        col_drop = df.iloc[:, 0:8].isnull().sum(axis=1) > 5
        drop_file.extend(df[col_drop].index.tolist())
        excluded_files.extend([f'experiment nº {i}' for i in df[col_drop].index.tolist()])
        excluded_files = pd.Series(excluded_files).drop_duplicates()
        excluded_files.name = "nº experiments"
        if training is True:
            df = training_data(df, drop_file, flowrate_null)
        df_gra_time = pd.DataFrame(data=gradient_time, index=["t_max", "num"])
        df_gra_time = df_gra_time.transpose()
        # excluded_files.to_csv("../../excluded_files.tsv", index=False)
        # df_gra_time.to_csv("../../excluded_files.tsv", sep="\t", index=True)
        return df
    except Exception as e:
        print(e)


def training_data(df, drop_file, flowrate_null):
    """
    Processes training data.

    This function fills missing values based on related columns means.
    It calculates dead time (t0) value with "column.id", "column.length" and "column.flowrate"
    in those columns where t0 is missing. Finally, it drops specified rows from the DataFrame.

    Args:
        df (DataFrame): DataFrame containing the data.
        drop_file (list): List of index to drop from the DataFrame.
        flowrate_null(dictionary): Dictionary containing index(key) and flow_rate columns(values)
        from each experiment

    Returns:
        DataFrame: Processed DataFrame after filling missing values and updating "column.t0".
    """
    try:
        for column in df.columns[2:8]:
            lines_null = df[df[column].isnull()]
            for column_name in lines_null["column.name"]:
                if pd.notnull(column_name):
                    same_lines = df[df['column.name'] == column_name]
                    mean = same_lines[column].mean()
                    if pd.isnull(mean):
                        same_pattern = df[df['column.name'].fillna('').str.contains(column_name[0:15])]
                        mean = same_pattern[column].mean()
                        if pd.isnull(mean):
                            mean = df[column].mean()
                    df.loc[(df[column].isnull()) & (df['column.name'] == column_name), column] = mean
        for key, values in flowrate_null.items():
            df.loc[key, values] = df.loc[key, "column.flowrate"]
        t0_lines = df[df["column.t0"] == 0]
        new_t0 = 0.66*np.pi*((t0_lines["column.id"]/2)**2)*t0_lines["column.length"]/(t0_lines["column.flowrate"]*10**3)
        df.loc[new_t0.index, "column.t0"] = new_t0
        df = df.drop(index=[i for i in pd.Series(drop_file)])
        return df
    except Exception as e:
        print(f"Error training:{e}")


def metadata():
    """
    Access to chromatographic column data

    This function reads chromatographic column metadata from TSV files in the '../data/*/' directory,
    concatenates them into a single DataFrame, and processes the data to ensure that all eluents are in
    the same units (%) and to generate a new column with the number of missing values.

    Returns:
        tuple: A tuple containing two DataFrames:
            - `column_data`: DataFrame containing metadata related to chromatographic columns,
              including column inner diameter, name, length, temperature, etc., and a column indicating
              the number of missing values.
            - `eluent_data`: DataFrame containing metadata related to the eluent used in chromatography.
              This DataFrame excludes unit-related columns and columns related to gradient data
    """
    try:
        directory = glob("data/*/*.tsv")
        metadata_list = []
        for file in directory:
            if re.search(r"_metadata.tsv", file):
                met = pd.read_csv(file, sep='\t', header=0, encoding='utf-8')
                metadata_list.append(met)
        df_metadata = pd.concat(metadata_list, ignore_index=True)
        df_metadata = df_metadata.set_index("id")
        position = [pos for pos, col in enumerate(df_metadata.columns) if "unit" in col]
        for pos in position:
            if df_metadata.iloc[:, pos].notna().any() and df_metadata.iloc[:, pos].str.contains("mM").any():
                if "nh4ac" in df_metadata.columns[pos - 1]:
                    df_metadata.iloc[:, pos - 1] *= 0.007
                elif "nh4form" in df_metadata.columns[pos - 1]:
                    df_metadata.iloc[:, pos - 1] *= 0.005
                elif "nh4carb" in df_metadata.columns[pos - 1]:
                    df_metadata.iloc[:, pos - 1] *= 0.006
                elif "nh4bicarb" in df_metadata.columns[pos - 1]:
                    df_metadata.iloc[:, pos - 1] *= 0.005
                elif "nh4form" in df_metadata.columns[pos - 1]:
                    df_metadata.iloc[:, pos - 1] *= 0.004
                elif "nh4oh" in df_metadata.columns[pos - 1]:
                    df_metadata.iloc[:, pos - 1] *= 0.004
            elif df_metadata.iloc[:, pos].notna().any() and df_metadata.iloc[:, pos].str.contains("µM").any():
                if "phosphor" in df_metadata.columns[pos - 1]:
                    df_metadata.iloc[:, pos - 1] *= 5.21 / (10 ** 6)
                elif "medronic" in df_metadata.columns[pos - 1]:
                    df_metadata.iloc[:, pos - 1] *= 8.38 / (10 ** 6)
        columns_unit = [col for col in df_metadata.columns if '.unit' in col or "gradient." in col]
        column_data = df_metadata.iloc[:, 0:8]
        column_data["missing_values"] = column_data.isnull().sum(axis=1)
        eluent_data = df_metadata.iloc[:, 8:].drop(columns=columns_unit)
        return column_data, eluent_data
    except Exception as e:
        print(f"Error metadata:{e}")
