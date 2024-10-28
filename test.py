import tempfile

import pandas as pd
import argparse
import logging
import util.judge_df_equal
import util.ray
from codes.pandas_q1 import pandas_q1
from codes.pandas_q2 import pandas_q2
from codes.pandas_q3 import pandas_q3
from codes.pandas_q4 import pandas_q4
from codes.ray_q1 import ray_q1
from codes.ray_q2 import ray_q2
from codes.ray_q3 import ray_q3
from codes.ray_q4 import ray_q4
logger = logging.getLogger()

# *****************************************define the parameters****************************
parser = argparse.ArgumentParser(description='grader for cs511 Pandas/Ray.')

parser.add_argument('--p1', type=str, default="1994-01-01",
                    help='parameter for pandas_q1')
parser.add_argument('--p2', type=int, default=90,
                    help='parameter for pandas_q2')
parser.add_argument('--p3', type=str, default='BUILDING',
                    help='parameter for pandas_q3')
parser.add_argument('--p4', type=str, default="1993-7-01",
                    help='parameter for pandas_q4')

parser.add_argument('--r1', type=str, default="1994-01-01",
                    help='parameter for ray_q1')
parser.add_argument('--r2', type=int, default=90, help='parameter for ray_q2')
parser.add_argument('--r3', type=str, default='BUILDING',
                    help='parameter for ray_q3')
parser.add_argument('--r4', type=str, default="1993-7-01",
                    help='parameter for ray_q4')

parser.add_argument('--pq1a', type=float, default=123141078.2283,
                    help='correct answer for pandas q1')
parser.add_argument('--rq1a', type=float,
                    default=123141078.2283, help='correct answer for ray q1')
# **************************************************************************************************


args = parser.parse_args()

# ***********************************read the data*******************************
lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
orders = pd.read_csv("tables/orders.csv", header=None, delimiter="|")
customer = pd.read_csv("tables/customer.csv", header=None, delimiter="|")
partsupp = pd.read_csv("tables/partsupp.csv", header=None, delimiter="|")
supplier = pd.read_csv("tables/supplier.csv", header=None, delimiter="|")
part = pd.read_csv("tables/part.csv", header=None, delimiter="|")
region = pd.read_csv("tables/region.csv", header=None, delimiter="|")
nation = pd.read_csv("tables/nation.csv", header=None, delimiter="|")
part.columns = ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice',
                'p_comment']
nation.columns = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                    'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                    'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
region.columns = ['r_regionkey', 'r_name', 'r_comment']
supplier.columns = ['s_suppkey', 's_name', 's_address',
                    's_nationkey', 's_phone', 's_acctbal', 's_comment']
partsupp.columns = ['ps_partkey', 'ps_suppkey',
                    'ps_availqty', 'ps_supplycost', 'ps_comment']
customer.columns = ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
                    'c_comment']
orders.columns = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                  'o_clerk', 'o_shippriority', 'o_comment']

# *******************test********************************************
score = 0
ray_counter = 0

# *************************p1*************************
try:
    print("**************begin grade pandas q1**************************")
    result = pandas_q1(args.p1, lineitem)
    assert abs(result - args.pq1a) < 0.01
    print("*******************pass pandas q1**********************")
    score += 10
except Exception as e:
    logger.error("Exception Occurred:" + str(e))
    print("*******************failed pandas q1**********************")
    pass

# **************************p2**************************
try:
    print("**************begin grade pandas q2**************************")
    result = pandas_q2(args.p2, lineitem)
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f', index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("./correct_results" + "/pandas_q2.csv")
    assert util.judge_df_equal.judge_df_equal(result, correct_result)
    print("*******************pass pandas q2**********************")
    score += 10
except Exception as e:
    logger.error("Exception Occurred:" + str(e))
    print("*******************failed pandas q2**********************")
    pass

# **************************p3**************************
try:
    print("**************begin grade pandas q3**************************")
    result = pandas_q3(args.p3, customer, orders, lineitem)
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f', index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("./correct_results" + "/pandas_q3.csv")
    assert util.judge_df_equal.judge_df_equal(result, correct_result)
    print("*******************pass pandas q3**********************")
    score += 10
except Exception as e:
    logger.error("Exception Occurred:" + str(e))
    print("*******************failed pandas q3**********************")
    pass

# **************************p4**************************
try:
    print("**************begin grade pandas q4**************************")
    result = pandas_q4(args.p4, orders, lineitem)
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f', index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("./correct_results" + "/pandas_q4.csv")
    assert util.judge_df_equal.judge_df_equal(result, correct_result)
    print("*******************pass pandas q4**********************")
    score += 10
except Exception as e:
    logger.error("Exception Occurred:" + str(e))
    print("*******************failed pandas q4**********************")
    pass

# *************************r1*************************
try:
    print("**************begin grade ray q1**************************")
    result = ray_q1(args.r1, lineitem)
    assert abs(result - args.rq1a) < 0.01
    ray_counter += 1
    assert util.ray.success(ray_counter)
    print("*******************pass ray q1**********************")
    score += 15
except Exception as e:
    logger.error("Exception Occurred:" + str(e))
    print("*******************failed ray q1**********************")
    pass

# **************************r2**************************
try:
    print("**************begin grade ray q2**************************")
    result = ray_q2(args.r2, lineitem)
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f', index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("./correct_results" + "/ray_q2.csv")
    assert util.judge_df_equal.judge_df_equal(
        result, correct_result), "result error"
    ray_counter += 1
    assert util.ray.success(ray_counter), "ray error"
    print("*******************pass ray q2**********************")
    score += 15
except Exception as e:
    logger.error("Exception Occurred at Q2:" + str(e))
    print("*******************failed ray q2**********************")
    pass

# **************************r3**************************
try:
    print("**************begin grade ray q3*************************")
    result = ray_q3(args.r3, customer, orders, lineitem)
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f', index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("./correct_results" + "/ray_q3.csv")
    assert util.judge_df_equal.judge_df_equal(result, correct_result)
    ray_counter += 1
    assert util.ray.success(ray_counter)
    print("*******************pass ray q3**********************")
    score += 15
except Exception as e:
    logger.error("Exception Occurred:" + str(e))
    print("*******************failed ray q3**********************")
    pass

# **************************r4**************************
try:
    print("**************begin grade ray q4*************************")
    result = ray_q4(args.r4, orders, lineitem)
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f', index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("./correct_results" + "/ray_q4.csv")
    assert util.judge_df_equal.judge_df_equal(result, correct_result)
    ray_counter += 1
    assert util.ray.success(ray_counter)
    print("*******************pass ray q4**********************")
    score += 15
except Exception as e:
    logger.error("Exception Occurred:" + str(e))
    print("*******************failed ray q4**********************")


# ******************print the total score***********************************
print(f"total score is: {score}")
