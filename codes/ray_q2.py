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


@ray.remote
def process(data: pd.DataFrame):

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
    # print("lineitem memory before: ", lineitem.memory_usage(
    #     deep=True).sum() / 1024 / 1024, "MB")
    lineitem.drop(columns=[
        'l_comment',
        'l_shipmode',
        'l_shipinstruct',
        'l_receiptdate',
        'l_suppkey',
        'l_partkey',
    ], inplace=True)
    # print("lineitem memory after: ", lineitem.memory_usage(
    #     deep=True).sum() / 1024 / 1024, "MB")

    lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
    lineitem = lineitem[lineitem['l_shipdate'] <= pd.to_datetime(
        '1998-12-01') - pd.DateOffset(days=timediff)]
    chunks = np.array_split(lineitem, 16)
    tasks = [process.remote(chunk) for chunk in chunks]
    results = ray.get(tasks)
    results = pd.concat(results)
    results = results.groupby(['l_returnflag', 'l_linestatus']).agg(
        sum_qty=('sum_qty', 'sum'),
        sum_base_price=('sum_base_price', 'sum'),
        sum_disc_price=('sum_disc_price', 'sum'),
        sum_charge=('sum_charge', 'sum'),
        avg_qty=('sum_qty', 'sum'),
        avg_price=('sum_base_price', 'sum'),
        avg_disc=('sum_disc', 'sum'),
        count_order=('count_order', 'sum')
    ).reset_index()
    results['avg_qty'] /= results['count_order']
    results['avg_price'] /= results['count_order']
    results['avg_disc'] /= results['count_order']
    results = results.sort_values(by=['l_returnflag', 'l_linestatus'])
    return results
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
