# data_format
### 1. 文件结构

![data_format文件结构.png](http://tva1.sinaimg.cn/large/bf776e91ly1h2bhsmi1pyj20qs0jmdk1.jpg)

- data：配置相关的文件

  - backup：备份目录，每次代码执行过后会将相应数据表数据导出成SQL文件，存储至对应板块目录下，文件按照日期命名。

  - chart：点表对照文件，用于转换某些字段信息

  - conf：主要的配置文件，包括：点表文件目录（`chart_file`）、索引名（`chart_index`）、待解析字段名称（`chart_column`）、数据库名称（`db_name`）、数据表名称（table_name）、数据文件格式（`file_type`）、解析频率（`fre`）、不变项（`id_var`）、可变项（`var_name`）、备份文件目录（`backup`）、sheet表格范围（`sheet_range`）、是否需要转换中英文（`need_convert`）

  - data：存放待解析存储的数据表格文件（如`cuona-2021-04-01.csv`）

- *.py：主程序代码

  - main.py：交互式主程序，直接执行即可
  - data_calc.py：主要用户按照公式计算最终值进行存储
  - tools.py：其他文件：辅助函数

    
  
  
  

### 2. 运行方式

~~~
1. 安装依赖库
pip install -r requirements.txt
注意，默认的参数是linux系统的，在windows平台需要将其中的pandas和numpy改为如下设置
pandas~=1.4.2
numpy~=1.23.4

2.如果不存在/data_format/data/data/kamba目录, 请手动创建该文件夹，这个路径是程序对kamba原始宽表数据.csv文件的读取目录

3.将需要更新的数据文件(一般是.csv格式)放入/data_format/data/data/kamba目录,执行成功后该目录下文件.csv会删除，所以请在别的位置保留好数据备份。

4. 执行主函数
加上history参数，更新历史数据(csv导入)：
python main.py history
更新实时数据使用
python main.py
~~~



### 3. 主函数流程

![data_format流程.png](http://tva1.sinaimg.cn/large/bf776e91ly1h2bif0042ej20wp0gcwgp.jpg)

​    

### 4. 数据库结构

- data_center_original：原始值数据库，包含所有采样后的表格原始数据
- data_center_statistical：公式计算值数据（长表格式）
- data_center_user：用户数据库，保存用户账号信息
- data_center_statistical_wide：宽表格式备份数据库
- data_report: 岗巴地区报表数据库


### 5. 函数详情

#### 5.1 概述

`main.py`文件内函数(`DataFormat`)主要负责将表格数据解析并存储，`data_calc.py`文件内函数(`DataCalc`)负责公式值计算以及备份操作，
`tools.py`文件主要放一些公用工具函数比如日志打印函数，时间列的检查函数等。`settings.py`主要是一些配置项，例如数据参数定义，数据库配置定义等。
`data_report.py`文件主要用以上传存储一些特殊的报表数据（如岗巴的供热分析），所交互的数据库为`data_report`, 目前仅岗巴地区启用。

#### 5.2 主要涉及函数



**DataFormat**

| 函数名称      | 说明                                                         |
| ------------- | ------------------------------------------------------------ |
| get_data      | 获取表格数据，以dataframe格式返回                            |
| parse_data    | 天津数据需要先将每日数据整合后再合并在一起，其他版块内容在这里提取每个文件的具体数据值 |
| insert_to_sql | 将上述内容存储至数据库，针对天津板块会额外对缺失字段进行填充 |



**DataCalc**

| 函数名称             | 说明                                                   |
| -------------------- | ------------------------------------------------------ |
| get_data_range       | 获取数据日期范围，根据此项来进行实时数据的筛选         |
| data_collation       | 调用所有公式进行计算并将结果值整合到字典结构中进行返回 |
| update_history_data  | 更新历史数据，最最初数据进行计算并存储                 |
| update_realtime_data | 更新实时数据，根据数据库最新内容进行追加               |
