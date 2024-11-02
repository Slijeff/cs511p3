"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-11 10:03:13
 	 FilePath: /codes/pandas_q3.py
 	 Description:
"""
import tempfile

import numpy as np
import pandas as pd
import ray
import typing

import util.judge_df_equal

# ray.init(ignore_reinit_error=True)


@ray.remote
def process_3(customer, orders, lineitem):

    filtered_df = pd.merge(
        pd.merge(
            customer,
            orders,
            left_on='c_custkey',
            right_on='o_custkey'
        ),
        lineitem,
        left_on='o_orderkey',
        right_on='l_orderkey'
    )
    def getValueAt(df, x, name): return df.loc[x.index, name]
    result_df = filtered_df.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']).agg(
        revenue=('l_extendedprice', lambda x: (
            x * (1 - getValueAt(filtered_df, x, 'l_discount'))).sum())
    ).reset_index()
    return result_df


def ray_q3(segment: str, customer: pd.DataFrame, orders: pd.DataFrame, lineitem: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame()

    customer = customer[customer['c_mktsegment'] == segment]
    orders['o_orderdate'] = pd.to_datetime(orders['o_orderdate'])
    lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
    lineitem = lineitem[lineitem['l_shipdate'] > pd.to_datetime('1995-03-15')]
    orders = orders[orders['o_orderdate'] < pd.to_datetime('1995-03-15')]

    # keep only necessary columns
    customer = customer[['c_custkey']]
    orders = orders[['o_orderkey', 'o_custkey',
                     'o_orderdate', 'o_shippriority']]
    lineitem = lineitem[['l_orderkey', 'l_extendedprice', 'l_discount']]

    orders = np.array_split(orders, 4)
    lineitem = np.array_split(lineitem, 4)
    join_indices = [(i, j) for i in range(len(orders))
                    for j in range(len(lineitem))]
    tasks = [process_3.remote(customer, orders[i], lineitem[j])
             for i, j in join_indices]
    results = pd.concat(ray.get(tasks))
    results = results.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']).agg(
        revenue=('revenue', 'sum')
    ).reset_index().sort_values(by=['revenue', 'o_orderdate'], ascending=[False, True]).head(10)
    return results
    # end of your codes


if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    orders = pd.read_csv("tables/orders.csv", header=None, delimiter="|")
    customer = pd.read_csv("tables/customer.csv", header=None, delimiter="|")

    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    customer.columns = ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
                        'c_comment']
    orders.columns = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                      'o_clerk', 'o_shippriority', 'o_comment']

    # run the test
    result = ray_q3('BUILDING', customer, orders, lineitem)
    # result.to_csv("correct_results/pandas_q3.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f', index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q3.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(
                f"*******************failed, your incorrect result is {result}**************")
