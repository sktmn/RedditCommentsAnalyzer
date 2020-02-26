import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

'''
Takes input as data-frame and plots horizontal bar-plot with top-n and bottom-n rows 
'''


def plot_barh(input_df: pd.DataFrame, x_axis_col: str, y_axis_col: str,
              plt_title: str, x_axis_name: str = '', y_axis_name: str = '',
              number: int = 2):
    input_df_top10 = input_df.head(number)
    input_df_bottom10 = input_df.tail(number)
    input_df_plt = pd.concat([input_df_top10, input_df_bottom10])
    plt_x_index = range(2 * number)
    plt_x_data = np.asarray(input_df_plt[x_axis_col])
    plt_y_data = np.asarray(input_df_plt[y_axis_col])
    plt.barh(plt_x_index, plt_y_data)
    if len(x_axis_name) < 1:
        plt.xlabel(y_axis_col, fontsize=5)
    else:
        plt.xlabel(y_axis_name, fontsize=5)

    if len(y_axis_name) < 1:
        plt.ylabel(x_axis_col, fontsize=5)
    else:
        plt.ylabel(x_axis_name, fontsize=5)

    plt.yticks(plt_x_index, plt_x_data, fontsize=5, rotation=0)
    plt.title(plt_title)
    plt.show()

# ups_by_user_df = pd.read_csv('ups_by_user.csv/*.csv')
#
# print(len(ups_by_user_df))
#
# print(ups_by_user_df.describe())
#
# plot_barh(ups_by_user_df, Schema.Author, 'totalUps', 'some-title')
