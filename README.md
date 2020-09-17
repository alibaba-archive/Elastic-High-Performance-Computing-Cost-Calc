# E-HPC报表费用分摊计算工具
### 功能特性
1. 通过集群作业详单CSV文件和费用中心费用计费项明细账单分析集群作业任务消费明细

2. 按作业id(JobId)汇总生成各作业的消费详情及核时统计，并输出csv统计文件

3. 按用户(user)汇总生成各用户的消费详情，作业数以及核时统计，并输出csv统计文件

4. 按队列(queue)汇总生成各队列的消费详情，作业数，使用用户数以及核时统计，并输出csv统计文件

5. 统计生成各作业在相应队列的消费详情，以及各队列作业的总消费和实例的实际消费，并输出csv统计文件

6. 通过分析源数据分析获得不同维度(作业id，用户，队列)下，各维度的消费详情，包括包年包月，抢占式实例以及按时按量消费，并生成可视化图

7. 分析使用的抢占式实例价格变化趋势

### 准备工作

* 运行环境: Linux / MacOS + Python3.2 以上
* 依赖包: 

    pandas: `pip3 install pandas`

    numpy: `pip3 install numpy`
    
    matplotlib: `pip3 install matplotlib`

* E-HPC控制台报表页面，导出作业详单CSV文件

* 阿里云费用中心页面，导出费用账单消费项明细。页面选项参考附图 “导出计费项账单明细.jpg”

* 配置参数文件config.ini。参数示例文件及含义如下

### 参数文件
    summaryType = 4
    #汇总类型，按哪个Key进行结果汇总，默认全部汇总输出
    # 0 : 不汇总输出; 1 : 按Job汇总; 2 : 按User汇总; 3 : 按Queue汇总; 4 : 全部汇总输出
    
    starttime = 2020-06-01 01:00:00
    endtime = 2020-06-01 02:00:00
    #时间区间，筛选时间区间[starttime, endtime]中的任务进行统计
    #如果都为空, 则不进行筛选

    analy = yes 
    #是否生成统计分析详情
    #yes : 生成
    #no : 不生成

    spot = yes  
    #是否生成抢占式实例价格趋势; yes : 生成; no : 不生成

    ehpc_file = ../arg/jobInfo/jobInfo_1593014400.csv
    #ehpc_file = ../arg/jobInfo/jobInfo_1593014400.csv ../arg/jobInfo_1593018000.csv
    #ehpc作业详单文件或目录路径，支持多文件输入
    
    consume_detail_file = ../arg/res_consume_detail_4.csv ../arg/a.zip ../arg/res_consume_detail/
    #费用账单计费项明细CSV/ZIP文件名称或目录路径，支持多文件和多目录输入
    
    output_path = ./
    #输出文件路径, 默认为运行文件当前路径
    
    job_file =
    #按Job汇总输出文件名称, 默认名称为job-[timestamp].csv
  
    user_file =
    #按User汇总输出文件名称, 默认名称为user-[timestamp].csv

    queue_file =
    #按队列汇总输出文件名称, 默认名称为queue-[timestamp].csv
    
    #各维度统计消费详情可视化文件名称，默认名称为detail-[timestamp].svg
    detail_file =
    
    #抢占式实例价格趋势可视化文件名称，默认名称为spot-[timestamp].svg
    spot_file =
 
### 运行

```
Usage: ./calc.py ./config.ini
#./calc.py python程序
#./config.ini 参数文件路径
```

### 程序输出
- 按作业id(jobId)统计文件(job-[timestamp].csv)

- 按用户(user)统计文件(user-[timestamp].csv)

- 按队列(queue)统计文件(queue-[timestamp].csv)

- 任务队列统计文件(job-queue-[timestamp].csv)

- 各维度统计消费详情(detail-[timestamp].svg)

- 抢占式实例价格变化趋势(spot-[timestamp].svg)

## Contributing

## Authors

* qianzheng.llc <qianzheng.llc@alibaba-inc.com>
* ted.ft <ted.ft@alibaba-inc.com>
* jonathan.ljn <jonathan.ljn@alibaba-inc.com>

## License