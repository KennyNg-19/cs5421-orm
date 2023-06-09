
from sqlalchemy_queries import *
from django_queries import *
import json
from pprint import pprint
import time
import datetime
from concurrent.futures import ThreadPoolExecutor
import os
import subprocess


# convert to mysql
from sqlalchemy.dialects import mysql


def concurrent_queries(num_queries, func, num_thread):
    start = datetime.datetime.now()
    # print(f'start:{start}')
    # num_thread = 10
    # with ThreadPoolExecutor(max_workers=num_thread) as executor:
    with ThreadPoolExecutor(num_thread) as executor:
        futures = [executor.submit(func) for i in range(num_queries)]
        results = [future.result for future in futures]
    # print('result')
    # print(results)
    end = datetime.datetime.now()
    # print(f'end:{end}')
    time_cost = str(end - start)
    print(f'time cost:{end - start}')
    return time_cost

# 测量timming的注解
def timing_function(some_function):
    """
    Outputs the time a function takes
    to execute.
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        some_function(*args, **kwargs)
        end_time = time.time()
        print(f"for function {some_function.__name__}, execution time: {end_time - start_time:.10f} seconds")
    return wrapper

# 对函数执行，采样 Time-based memory usage + 获取max memory usage
def get_func_max_memory(sql_func, viz_plot=False):
    
    filename = sql_func.__name__ + "_memory"
    cmd = ["mprof", "run", "-o", filename + ".dat", "python", "-c", f"import {sql_func.__module__}; \
                {sql_func.__module__}.{sql_func.__name__}()"]
    subprocess.run(cmd) 
    # memory = subprocess.run(cmd, capture_output=True, text=True)
    # memory = subprocess.check_output(cmd)

    # 获取memory最后值，或者 取max峰值
    dat_file = filename + ".dat"
    num_lines = 5

    with open(dat_file, "r") as f:
        # 使用切片操作获取最后几行
        last_lines = f.readlines()[-num_lines:]
        '''
        .dat format like this:
        MEM 37.640625 1680389460.7517
        MEM 47.265625 1680389460.8554
        MEM 58.031250 1680389460.9559
        MEM 59.214844 1680389461.0603
        '''
        memory_samples = [round(float(line.split(" ")[1]),4) for line in last_lines]
    # print(memory_samples)
    
    
    # 使用 subprocess 模块启动新的进程执行 mprof plot 命令, 绘图
    if viz_plot:
        
        plot_cmd = ["mprof", "plot", "-o", f"{filename}.png", "-t", f"time-based {filename}", dat_file]
        subprocess.run(plot_cmd)
    
    # 清除当前文件夹下的 生成memory图的.dat
    subprocess.run(["mprof", "clean"])
        
    return max(memory_samples)

# 生成对应的sql, generation
def sqlalch_gen_sql(sql_func, db_backend=mysql):
    
    query = sql_func()
    return str(query.statement.compile(dialect=db_backend.dialect()))


def sqlalch_insert_benchmark():
    pass

def sqlalch_query_benchmark_time(func_list,num_queries,num_thread):

    metric_result = []

    for i, func in enumerate(func_list):
        print(f'begin func {func.__name__}')
        time_cost = concurrent_queries(num_queries, func, num_thread)
        result = {'func': func.__name__, 'num_queries': num_queries, 'num_thread': num_thread, 'time_cost': time_cost}
        metric_result.append(result)

    print("All results:")
    pprint(metric_result)

    path = './orm_metric_time.txt'
    with open(path, 'w', encoding='utf-8') as f:
        for line in metric_result:
            json.dump(line, f)
            f.write('\n')

def sqlalch_query_benchmark_memory(func_list, viz_plot):
    metric_result = []
    for i, func in enumerate(func_list):
        print(f'begin func {func.__name__}')
        max_memory = get_func_max_memory(func, viz_plot=viz_plot)
        result = {'func': func.__name__, 'num_queries': 1, 'num_thread': 1, 'max_memory': max_memory}
        metric_result.append(result)

    path = './orm_metric_memory.txt'
    with open(path, 'w', encoding='utf-8') as f:
        for line in metric_result:
            json.dump(line, f)
            f.write('\n')

def sqlalch_query_benchmark():
    # test concurrent
    func_list = [
        # sqlalchmey
        sql1,
        sql2,
        sql4,
        sql5,
        sql6,
        sql8,
        # sql8_subquery,
        sql9,
        sql10,
        sql11,
        # sql12,
        # sql13,
        # sql15,
        # sql16,

        # django
        sql_function_1,
        sql_function_2,
        sql_function_4,
        sql_function_5,
        sql_function_6,
        sql_function_8,
        sql_function_9,
        sql_function_10,
        sql_function_11,
    ]

    num_queries = 1
    num_thread = 1
    sqlalch_query_benchmark_time(func_list,num_queries,num_thread)
    sqlalch_query_benchmark_memory(func_list, viz_plot=False)


def sqlalch_sql_generation():
    func_list = [
        {'name': '1', 'func': sql1},
        {'name': '2', 'func': sql2},
        {'name': '4', 'func': sql4},
        {'name': '5', 'func': sql5},
        {'name': '8', 'func': sql8},
        {'name': '9', 'func': sql9},
        {'name': '10', 'func': sql10},
        {'name': '11', 'func': sql11},
        {'name': '12', 'func': sql12},
        {'name': '13', 'func': sql13},
        {'name': '15', 'func': sql15},
        {'name': '16', 'func': sql16},
    ]
    path = './orm_generate_sql.txt'
    with open(path, 'w', encoding='utf-8') as f:
        for func in func_list:
            # json.dump(func['name'], fp)
            # f.write('\n')
            json.dump("sql"+func['name'] + ": " + sqlalch_gen_sql(func['func']), f)
            f.write('\n\n')
    
        

if __name__ == '__main__':
    sqlalch_query_benchmark()
    # sqlalch_sql_generation()
