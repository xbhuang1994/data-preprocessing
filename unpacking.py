# %% [markdown]
# Load 7zip Files
# 

# %%
import os
import py7zr
import re

def unpacking(file_path):
    if file_path.endswith('7z'):
        print(file_path)                
        with py7zr.SevenZipFile(file_path, 'r') as archive:
            allfiles = archive.getnames()
            filter_pattern = re.compile(r'\d*/[11-12]\S*/逐笔')
            selective_files = [f for f in allfiles if filter_pattern.match(f)]
            archive.extract(path = "data",targets=selective_files)
            print(len(selective_files))

# %%
from multiprocessing import Pool,cpu_count
import time
def main():
    print("CPU内核数:{}".format(cpu_count()))
    print('当前母进程: {}'.format(os.getpid()))
    start = time.time()
    p = Pool(20)
    go = os.walk("D:\\BaiduNetdiskDownload\\股票数据\\07")
    for root, dirs, files in go:
        for file_name in files:
            file_path = os.path.join(root, file_name) 
            p.apply_async(unpacking, args=(file_path,))
    print('等待所有子进程完成。')
    p.close()
    p.join()
    end = time.time()
    print("总共用时{}秒".format((end - start)))
if __name__ == '__main__':
    main()



