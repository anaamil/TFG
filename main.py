from temporal.ClassyFireQuery import access_data
from temporal.Gradient_data import gradient_data

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    final_data_nt = access_data(training=False)
    training_data = access_data(training=True)
    # final_data_nt.to_csv("../final_data_nt.tsv", sep='\t', index=False)
    # training_data.to_csv("../final_data.tsv", sep='\t', index=False)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
