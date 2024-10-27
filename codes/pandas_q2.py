"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-10 21:45:33
 	 FilePath: /codes/pandas_q2.py
 	 Description: 
"""
import pandas as pd
import ray
import typing
import util.judge_df_equal
import tempfile


def pandas_q2(timediff: int, lineitem: pd.DataFrame) -> pd.DataFrame:
    lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
    filtered_df = lineitem[lineitem['l_shipdate'] <= pd.to_datetime(
        '1998-12-01') - pd.DateOffset(days=timediff)]

    def getValueAt(df, x, name): return df.loc[x.index, name]
    result_df = filtered_df.groupby(['l_returnflag', 'l_linestatus']).agg(
        sum_qty=('l_quantity', 'sum'),
        sum_base_price=('l_extendedprice', 'sum'),
        sum_disc_price=('l_extendedprice', lambda x: (
            x * (1 - getValueAt(filtered_df, x, 'l_discount'))).sum()),
        sum_charge=('l_extendedprice', lambda x: (
            x * (1 - getValueAt(filtered_df, x, 'l_discount')) * (1 + getValueAt(filtered_df, x, 'l_tax'))).sum()),
        avg_qty=('l_quantity', 'mean'),
        avg_price=('l_extendedprice', 'mean'),
        avg_disc=('l_discount', 'mean'),
        count_order=('l_orderkey', 'count')
    ).sort_values(by=['l_returnflag', 'l_linestatus']).reset_index()
    return result_df
    # end of your codes


if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()

    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    # run the test
    result = pandas_q2(90, lineitem)
    # result.to_csv("correct_results/pandas_q2.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f', index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/pandas_q2.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(
                f"*******************failed, your incorrect result is {result}**************")
