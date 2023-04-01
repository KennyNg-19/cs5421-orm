
from models_queries import *
import json
from pprint import pprint
from memory_profiler import profile
import time
import datetime
from concurrent.futures import ThreadPoolExecutor

def multi_thread(number):
    start = datetime.datetime.now()
    print(f'start:{start}')
    for _ in range(number):
        thread = threading.Thread(target=sql12)
        thread.start()
        thread.join()
    print('threads end')
    end = datetime.datetime.now()
    print(f'end:{end}')
    print(f'time cost:{end - start}')

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


def sqlalch_insert_benchmark_time():
    pass

def sqlalch_query_benchmark_time():
    # test concurrent
    func_list = [
        {'name': '11', 'func':sql11},
        {'name': '12', 'func':sql12},
        {'name': '13', 'func':sql13},
        {'name': '15', 'func':sql15},
        {'name': '16', 'func':sql16},
    ]
    num_queries = 100
    num_thread = 4
    metric_result = []
    
    
    for i, check in enumerate(func_list):
        print(f'begin func {check["name"]}')
        time_cost = concurrent_queries(num_queries,check['func'],num_thread)
        result = {'func':check['name'],'num_queries':num_queries,'num_thread':num_thread,'time_cost':time_cost}
        metric_result.append(result)
    
    print("All results:")
    pprint(metric_result)

    path = './orm_metric.txt'
    with open(path,'w',encoding='utf-8') as f:
        for line in metric_result:
            json.dump(line,f)
            f.write('\n')


def sqlalch_benchmark_memory():
    # use Python memory profiler
    pass

if __name__ == '__main__':
    sqlalch_query_benchmark_time()