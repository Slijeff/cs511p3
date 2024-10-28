"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-10 21:45:33
 	 FilePath: /codes/pandas_q2.py
 	 Description:
"""
import numpy as np
import pandas as pd
import ray
import typing
import util.judge_df_equal
import tempfile


ray.init()


@ray.remote
def process(data: pd.DataFrame, timediff):
    data['l_shipdate'] = pd.to_datetime(data['l_shipdate'])
    data = data[data['l_shipdate'] <= pd.to_datetime(
        '1998-12-01') - pd.DateOffset(days=timediff)]

    def getValueAt(df, x, name): return df.loc[x.index, name]
    result_df = data.groupby(['l_returnflag', 'l_linestatus']).agg(
        sum_qty=('l_quantity', 'sum'),
        sum_base_price=('l_extendedprice', 'sum'),
        sum_disc_price=('l_extendedprice', lambda x: (
            x * (1 - getValueAt(data, x, 'l_discount'))).sum()),
        sum_charge=('l_extendedprice', lambda x: (
            x * (1 - getValueAt(data, x, 'l_discount')) * (1 + getValueAt(data, x, 'l_tax'))).sum()),
        sum_disc=('l_discount', 'sum'),
        count_order=('l_orderkey', 'count')
    ).reset_index()
    return result_df


def ray_q2(timediff: int, lineitem: pd.DataFrame) -> pd.DataFrame:
    # print size before and after
    lineitem = lineitem.drop(columns=[
        'l_comment',
        'l_shipmode',
        'l_shipinstruct',
        'l_receiptdate',
        'l_suppkey',
        'l_partkey',
        'l_commitdate'
    ])

    chunks = np.array_split(lineitem, 4)
    tasks = [process.remote(chunk, timediff) for chunk in chunks]
    results_1 = ray.get(tasks)
    results_2 = pd.concat(results_1)
    results_3 = results_2.groupby(['l_returnflag', 'l_linestatus']).agg(
        sum_qty=('sum_qty', 'sum'),
        sum_base_price=('sum_base_price', 'sum'),
        sum_disc_price=('sum_disc_price', 'sum'),
        sum_charge=('sum_charge', 'sum'),
        avg_qty=('sum_qty', 'sum'),
        avg_price=('sum_base_price', 'sum'),
        avg_disc=('sum_disc', 'sum'),
        count_order=('count_order', 'sum')
    ).reset_index()
    results_3['avg_qty'] /= results_3['count_order']
    results_3['avg_price'] /= results_3['count_order']
    results_3['avg_disc'] /= results_3['count_order']
    results_4 = results_3.sort_values(by=['l_returnflag', 'l_linestatus'])
    return results_4
    # end of your codes


if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    data = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    data.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                    'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                    'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    # run the test
    result = ray_q2(90, data)
    # result.to_csv("correct_results/pandas_q2.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f', index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q2.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(
                f"*******************failed, your incorrect result is {result}**************")
