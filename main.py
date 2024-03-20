from temporal.ClassyFireQuery import access_data


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    result_of_query_df = access_data("aminas")
    print(result_of_query_df.head())

# See PyCharm help at https://www.jetbrains.com/help/pycharm/