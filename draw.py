# -*- coding: utf-8 -*-
import pandas as pd

def draw_similarity(minute_bar_length):
    res = pd.read_csv("res.csv", encoding='utf-8')
    ax = res.plot(x='minute', figsize=(20, 13),
                  legend=False, title=u'Minute Bar Prediction')
    res.plot(y=['target_line'], ax=ax, linewidth=5, style='.b')
    res.plot(y=['fitting'], ax=ax, linewidth=4, style='-y')
    ax.vlines(x=minute_bar_length, ymin=-0.02, ymax=0.02,
              linestyles='dashed')
    ax.set_axis_bgcolor('white')
    ax.grid(color='gray', alpha=0.2, axis='y')

    # plot area
    avg_line = res['fitting']
    avg_line = list(avg_line)[minute_bar_length:]
    for line in columns:
        predict_line = res[line]
        predict_line = list(predict_line)[minute_bar_length:]
        ax.fill_between(range(minute_bar_length, 241), avg_line,
                        predict_line, alpha=0.1, color='r')
    return res, ax

if __name__ == '__main__':
    res, ax = draw_similarity(90)