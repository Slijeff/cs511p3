"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:47
 	 LastEditTime: 2023-09-11 10:37:24
 	 FilePath: /codes/pandas_q1.py
 	 Description:
"""
import pandas as pd
import ray
import typing
import numpy as np

# ray.init(ignore_reinit_error=True)


@ray.remote
def process_1(data, start_date):
    # Convert shipdate to datetime format within the data
    data['l_shipdate'] = pd.to_datetime(data['l_shipdate'])

    # filter the data based on the conditions
    filtered_data = data[
        (data['l_shipdate'] >= start_date) &
        (data['l_shipdate'] < start_date + pd.DateOffset(years=1)) &
        (data['l_discount'] >= 0.05) &
        (data['l_discount'] <= 0.070001) &
        (data['l_quantity'] < 24)
    ]

    # calculate and return the revenue for this data
    return (filtered_data['l_extendedprice'] * filtered_data['l_discount']).sum()


def ray_q1(time: str, lineitem: pd.DataFrame) -> float:
    # return pd.DataFrame()
    start_date = pd.to_datetime(time, format='%Y-%m-%d')
    chunks = np.array_split(lineitem, 8)
    tasks = [process_1.remote(chunk, start_date) for chunk in chunks]
    result = ray.get(tasks)
    revenue = sum(result)
    return revenue
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
    result = ray_q1("1994-01-01", lineitem)
    try:
        assert abs(result - 123141078.2283) < 0.01
        print("*******************pass**********************")
    except Exception as e:
        logger.error("Exception Occurred:" + str(e))
        print(
            f"*******************failed, your incorrect result is {result}**************")
