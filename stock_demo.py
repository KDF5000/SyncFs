# -*- coding: utf-8 -*-
# Spark Application - execute with spark-submit

# imports
from pyspark import SparkConf, SparkContext
import pandas as pd
import json

# Module Constants
APP_NAME = "My Spark Application"

minute_bar_length = None
rdd_mkt_data = None
target_line_mkt_data = None
minute_bar_length_share = None
target_line_share = None


def cal_similarity(line):
    """
    计算相似度
    """
    # 使用 sklearn，pandas 来简化计算流程
    import pandas as pd
    import sklearn.preprocessing
    scaler = sklearn.preprocessing.MinMaxScaler()
    # 通过广播变量获取预测的目标线和准备用来预测的分钟线长度
    minute_length = minute_bar_length_share.value
    target_line = target_line_share.value

    # 参数 line 的格式是： (line_id, line_data)
    line_id, line_data = line

    # 获取 pandas dataframe 格式的某日分钟线行情
    ticker, tradeDate = line_id[-25:-5].split('-')
    line_data = pd.DataFrame.from_dict(json.loads(line_data))
    line_data.sort(columns=['barTime'], ascending=True, inplace=True)

    # 每天有 240 条分钟线的 bar，我们用 前 minute_length 来计算相似度
    line1 = list(target_line.ratio)[: minute_length]
    line2 = list(line_data.ratio)[: minute_length]

    tmp = pd.DataFrame()
    tmp['first'], tmp['second'] = line1, line2
    tmp['diff'] = tmp['first'] - tmp['second']
    diff_square = sum(tmp['diff'] ** 2)
    # 返回格式：(分钟线id，该分钟线和目标线前 minute_length 个长度的相似度)
    return (line_id[-25:-5], round(diff_square, 5))


# UDF，从 rdd_mkt_data 里获取指定的多日分钟线数据
def get_similary_line(similarity_data):
    # 获取原始相似的分钟线数据
    rdd_lines = rdd_mkt_data.filter(
        lambda x: x[0][-25:-5] in [i[0] for i in similarity_data]
    ).collect()
    # 把原始分钟线数据转成 pandas dataframe 格式
    similar_line = {
        x[0][-25:-5]: pd.DataFrame.from_dict(json.loads(x[1]))
        for x in rdd_lines
    }
    similar_line = {
        x: similar_line[x].sort(columns=['barTime'], ascending=True)
        for x in similar_line
    }

    return similar_line


def draw_similarity(similar_line, target_line,
                    minute_bar_length, similarity_data):
    res = pd.DataFrame()
    columns = []
    for i in similarity_data:
        line_id = i[0]
        line_data = similar_line[line_id]
        res[line_id] = line_data.ratio
        if 'minute' not in res:
            res['minute'] = line_data.barTime
        columns.append(line_id)
    res['fitting'] = res[columns].sum(axis=1) / len(columns)
    res['target_line'] = target_line_mkt_data.ratio

    # plot
    ax = res.plot(x='minute', y=columns, figsize=(20, 13),
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


# Main Functionality
def main(sc):
    url = "/user/hadoop/data"
    rdd_mkt_data = sc.wholeTextFiles(url, minPartitions=80) \
                     .setName("index_minute_bar") \
                     .cache()
    # 指定要预测的线id<这里预测2016.03.17的分钟线
    target_line = "000001.ZICN-20160317"
    # 指定用于计算县四度的分钟线长度
    minute_bar_length = 90  # 9：30 - 11：00
    minute_bar_length_share = sc.broadcast(minute_bar_length)
    line_data = rdd_mkt_data.filter(lambda x: target_line in x[0]).collect()
    # line_data [(filename, data),(file,name),...]
    target_line_mkt_data = pd.DataFrame.from_dict(json.loads(line_data[0][1]))
    target_line_mkt_data.sort(
        columns=['barTime'],
        ascending=True, inplace=True
    )
    target_line_share = sc.broadcast(target_line_mkt_data)
    # spark 相似度计算代码
    rdd_similarity = rdd_mkt_data.map(cal_similarity)\
                                 .setName('rdd_similarity') \
                                 .cache()
    # 获取相似度最高的30日分钟线
    similarity_data = rdd_similarity.takeOrdered(30, key=lambda x: x[1])
    similar_line = get_similary_line(similarity_data)
    res, ax = draw_similarity(similar_line, target_line,
                              minute_bar_length, similarity_data)


if __name__ == '__main__':
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("spark://172.16.48.108:7077")
    sc = SparkContext(conf=conf)

    # Execute Main
    main(sc)
