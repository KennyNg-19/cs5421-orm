
from models_queries import *

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

def concurrent_queries(num_queries):
    start = datetime.datetime.now()
    print(f'start:{start}')
    num_thread = 4
    # with ThreadPoolExecutor(max_workers=num_thread) as executor:
    with ThreadPoolExecutor(num_thread) as executor:
        futures = [executor.submit(sql12) for i in range(num_queries)]
        # results = [future.result for future in futures]
    # print('result')
    # print(results)
    end = datetime.datetime.now()
    print(f'end:{end}')
    print(f'time cost:{end - start}')
    
    

if __name__ == '__main__':
    multi_thread(100)
    concurrent_queries(100)