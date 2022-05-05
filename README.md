# data_format
### 1. 文件结构

![data_format.png](http://tva1.sinaimg.cn/large/bf776e91ly1h1xlsuegjkj20i40cwdhs.jpg)

- data：配置相关的文件

  - backup：备份目录，每次代码执行过后会将相应数据表数据导出成SQL文件，存储至对应板块目录下，文件按照日期命名。

  - chart：点表对照文件，用于转换某些字段信息

  - conf：主要的配置文件，包括：点表文件目录（`chart_file`）、索引名（`chart_index`）、待解析字段名称（`chart_column`）、数据库名称（`db_name`）、数据表名称（table_name）、数据文件格式（`file_type`）、解析频率（`fre`）、不变项（`id_var`）、可变项（`var_name`）、备份文件目录（`backup`）、sheet表格范围（`sheet_range`）、是否需要转换中英文（`need_convert`）

  - data：存放待解析存储的数据表格文件（如`cuona-2021-04-01.csv`）

- *.py：主程序代码

  - main.py：交互式主程序，直接执行即可
  - cona.py、kamba.py、tianjin.py：各版块代码，后期设置自动化可用到
  - tools.py、其他文件：辅助函数

    
  
  
  

### 2. 运行方式

~~~
1. 安装依赖库
pip install -r requirements.txt

2. 执行主函数
python main.py
~~~



### 3. 主函数流程

![data_format流程.png](http://tva1.sinaimg.cn/large/bf776e91ly1h1xmai3itfj20n20gb75w.jpg)

​    

​     
