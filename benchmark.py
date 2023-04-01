
from models_queries import *
import json

def multi_thread(number):
    start = datetime.datetime.now()
    print(f'start:{start}')
    for i in range(number):
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

def metric_time():
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
    for i,check in enumerate(func_list):
        print(f'begin func {check["name"]}')
        time_cost = concurrent_queries(num_queries,check['func'],num_thread)
        result = {'func':check['name'],'num_queries':num_queries,'num_thread':num_thread,'time_cost':time_cost}
        metric_result.append(result)
    print(metric_result)

    path = './orm_metric.txt'
    with open(path,'w',encoding='utf-8') as f:
        for line in metric_result:
            json.dump(line,f)
            f.write('\n')
    

if __name__ == '__main__':
    metric_time()