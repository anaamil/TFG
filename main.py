from temporal.ClassyFireQuery import access_data
from temporal.Gradient_data import gradient_data
import pandas as pd
from formula_validation.Formula import Formula

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # final_data_nt = access_data(training=False)
    # training_data = access_data(training=True)
    # final_data_nt.to_csv("../final_data_nt.tsv", sep='\t', index=False)
    # training_data.to_csv("../final_data.tsv", sep='\t', index=False)
    final_df = pd.read_csv("../final_data.tsv", sep="\t", header=0, encoding='utf-8')
    for pos, inchi in enumerate(final_df["inchi.std"]):
        try:
            formula = Formula.formula_from_inchi(inchi, None)
        except Exception as e:
            smiles = final_df.loc[pos, "smiles.std"]
            formula = Formula.formula_from_smiles(smiles, None)
        final_df.loc[pos, "new_formula"] = str(formula)
    formula_column = final_df.pop("new_formula")
    final_df.insert(3, "new_formula", formula_column)
# Se# e PyCharm help at https://www.jetbrains.com/help/pycharm/
