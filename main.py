import os
import glob
import shutil
import logging
import platform
import traceback
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import DOUBLE, DATETIME, VARCHAR
from configparser import ConfigParser
from datetime import datetime, timedelta
from tools import get_point_mapping, get_file_data, get_all_columns, DataMissing
from data_calc import update_realtime_data, backup_statistics_data


class DataFormat:

    def __init__(self, config_file):
        cfg = ConfigParser()
        cfg.read(config_file, encoding='utf-8')
        base_log_path = "./data/log/" if platform.system() == "Windows" else "/home/data_format/data/log/"
        block = "tianjin" if "tianjin" in config_file else "kamba" if "kamba" in config_file else "cona"
        logging.basicConfig(level=logging.DEBUG,  # 控制台打印的日志级别
                            filename=base_log_path + block + "/logs.txt",
                            filemode='a+',
                            format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')

        # 说明文件
        self.chart_file = cfg.get('data', 'chart_file').strip() if platform.system() == "Windows" else \
            os.path.join("/home/data_format", cfg.get('data', 'chart_file').strip())

        self.chart_index = cfg.get('data', 'chart_index').strip()  # 说明文件中文说明
        self.chart_column = cfg.get('data', 'chart_column')  # 英文说明

        # 数据文件目录
        self.data_path = cfg.get('data', 'data_path').strip() if platform.system() == "Windows" else \
            os.path.join("/home/data_format", cfg.get('data', 'data_path').strip())
        self.fre = cfg.get('data', 'fre').strip()  # 样本采集频率
        self.id_var = cfg.get('data', 'id_var').strip()  # 不变列
        self.var_name = cfg.get('data', 'var_name').strip()  # 可变列
        self.file_type = cfg.get('data', 'file_type').strip()  # 数据文件格式
        self.db_name = cfg.get('data', 'db_name').strip()   # 数据库名称
        self.table_name = cfg.get('data', 'table_name').strip()  # 数据表名称

        # 备份目录
        self.original_backup = cfg.get('data', 'original_backup').strip() if platform.system() == "Windows" else \
            os.path.join("/home/data_format", cfg.get('data', 'original_backup').strip())

        self.statistics_backup = cfg.get('data', 'statistics_backup').strip() if platform.system() == "Windows" else \
            os.path.join("/home/data_format", cfg.get('data', 'statistics_backup').strip())

        self.sheet_range = cfg.get('data', 'sheet_range').strip()   # sheet表区间
        self.need_convert = int(cfg.get('data', 'need_convert').strip())   # 是否需要转换列名称
        self.conf_file = config_file    # 配置文件
        self.conn_conf = {
            "host": "localhost",
            "user": "root",
            "password": "299521",
        } if platform.system() == "Windows" else {
            "host": "121.199.48.82",
            "user": "root",
            "password": "cdqr2008",
        }
        logging.info("*" * 100)
        logging.info(
            "===============     Start {} 数据解析开始 {}     ===============\n".format(
                "岗巴" if "kamba" in self.table_name else "错那" if "cona" in self.table_name else "天津",
                datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            )
        )

    def get_conn(self):
        """返回数据库连接
        """

        return create_engine(
            'mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format(
                self.conn_conf["user"],
                self.conn_conf["password"],
                "localhost",
                self.db_name
            )
        )

    def chart_to_data(self):
        """
        根据参数对照表返回字典数据
        :return: 返回字典数据，key和value分别对应 中文索引index 和 英文说明column
        """
        start, end = [int(data) for data in self.sheet_range.split('-')]
        if start != end:
            dfs = pd.DataFrame()
            for sheet in range(start, end):
                point = pd.read_excel(self.chart_file, sheet_name=sheet)
                dfs = dfs.append(point)
                dfs = dfs.set_index(self.chart_index)
                return dfs[self.chart_column].to_dict()
        else:
            point = pd.read_excel(self.chart_file, sheet_name=start)
            point = point.set_index(self.chart_index)
            return point[self.chart_column].to_dict()

    def parse_data(self, file):
        """
        返回调整采集频率，转换后的dataframe数据
        :param file: 待处理的文件
        :return: dataframe数据
        """
        if self.need_convert:
            print("文件：{}， 需要转换".format(file))
            df = pd.read_csv(file, header=0, encoding='gbk', index_col=0, parse_dates=True)  # 读取文件
        else:
            df = pd.read_csv(file, header=0, encoding='utf-8', index_col=0, parse_dates=True)  # 读取文件,不用转换

        if self.data_check(df):
            df = df.resample(self.fre).last()
            df.index.name = self.id_var

            if self.need_convert:
                dic_data = self.chart_to_data()
                df = df.rename(columns=dic_data).reset_index()
            else:
                df = df.reset_index()
            df = df.melt(id_vars=self.id_var, var_name=self.var_name)
            if df["value"].dtype == "object":
                df["value"].replace("\s*\[u\.\]\s*", "", regex=True, inplace=True)
                df["value"] = df["value"].astype("float64")
            return df
        else:
            raise DataMissing("数据遗漏, 当前文件：{}".format(file))

    def get_data(self):
        """获取数据内容，错那、岗巴会获取到单个文件内容整理成一个dataframe，天津数据会将该日所有机组的数据文件整合成一个dataframe，缺失项设置为NAN

        """
        if "tianjin" in self.conf_file:
            if not os.listdir(self.data_path):
                logging.info('没有文件')
                print("没有文件")
                return None
            else:
                for dir_path in os.listdir(self.data_path):
                    df = pd.DataFrame()
                    files = glob.glob(self.data_path + "/" + dir_path + "/*." + self.file_type)
                    file_index = None

                    for file in files:
                        point_mapping, res = get_point_mapping(file), []

                        file_data, columns = get_file_data(file, point_mapping)

                        if isinstance(file_data, pd.DataFrame):
                            file_df = file_data
                        else:

                            file_df = pd.DataFrame(file_data, columns=[point_mapping.get(item) for item in columns])
                            file_df["date"] = pd.DatetimeIndex(
                                pd.to_datetime(file_df["date"] + " " + file_df["time"])
                            )
                            new_columns = ["date"] + list(file_df.columns[2:])
                            file_df = file_df.loc[:, new_columns]

                        if not file_index:
                            file_index = len(file_df.index)
                        else:
                            if file_index != len(file_df.index):
                                print("异常，文件：{}".format(file), "公共：{}, now：{}".format(file_index, len(file_df.index)))
                                raise DataMissing(
                                    "异常，文件：{},公共索引长度:{},当前文件索引长度：{}".format(
                                        file, file_index, len(file_df.index)
                                    )
                                )

                        if "date" not in df.columns:

                            df = pd.concat([df, file_df], axis=1)
                        else:
                            df = pd.concat([df, file_df.loc[:, file_df.columns[1:]]], axis=1)
                    all_columns = get_all_columns(self.chart_file)

                    for column in all_columns:
                        if column not in df.columns:
                            df[column] = np.nan
                    df.drop_duplicates(inplace=True)

                    df = df.set_index("date")
                    df = df.resample(self.fre).last()
                    df.index.name = self.id_var

                    if self.data_check(df):
                        df = df.reset_index()
                        df = df.melt(id_vars=self.id_var, var_name=self.var_name)
                        logging.info("数据全部获取完成{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                        return df.sort_values(by=self.id_var)
                    else:
                        raise DataMissing("数据检查异常")

        else:
            if not glob.glob(self.data_path + '/*.' + self.file_type):
                logging.info('没有文件')
                print("没有文件")
                return None

            else:
                dfs = pd.DataFrame()
                logging.info("开始获取数据文件{}".format(glob.glob(self.data_path + '/*.' + self.file_type)))
                for file in glob.glob(self.data_path + '/*.' + self.file_type):
                    dfs = dfs.append(
                        self.parse_data(file)
                    )

                dfs.drop_duplicates(inplace=True)  # 去重
                logging.info("数据全部获取完成{}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                return dfs.sort_values(by=self.id_var)

    def insert_to_sql(self, items, conn):
        """
        上传至数据库
        :param conn: 数据库连接
        :param items: 数据
        :return:
        """
        # 存入数据库，追加
        if items is not None:
            items.to_sql(
                name=self.table_name,
                con=conn,
                if_exists='append',
                index=False,
                dtype={
                    "Timestamp": DATETIME,
                    "pointname": VARCHAR(length=30),
                    "value": DOUBLE
                }
            )
            logging.info("完成所有数据上传")
            return True

        return False

    def clear_backup(self):
        """清除备份文件，默认保留日期为60天内

        """
        today = datetime.today()
        expired_date = today - timedelta(days=60)
        res = []
        for backup_type in [self.original_backup, self.statistics_backup]:
            for file in os.listdir(backup_type):
                dates_component = file.split("_")[1]
                dates = dates_component[:8]

                date = datetime.strptime(dates, "%Y%m%d")
                if date < expired_date:
                    os.remove(os.path.join(backup_type, file))
                    res.append(file)
            if res:
                logging.info("已清除备份文件：", ", ".join(res))

    def original_table_backup(self):
        """数据备份，每次数据存储执行完成后会进行备份，将数据导出成sql文件

        """
        now, num = datetime.today().strftime("%Y%m%d"), 1
        name = os.path.join(self.original_backup, "{}_{}.sql".format(self.table_name, now))

        while os.path.exists(name):
            num += 1
            name = os.path.join(self.original_backup, "{}_{}({}).sql".format(self.table_name, now, num))

        backup_sql = "mysqldump -u{} -p{} {} {} > {}".format(
            self.conn_conf["user"],
            self.conn_conf["password"],
            self.db_name,
            self.table_name,
            name
        )
        os.system(backup_sql)
        print("数据备份已完成 表名：{}，文件名：{}, 时间：{}".format(
            self.table_name, name, datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        ))
        logging.info("数据备份已完成 表名：{}，文件名：{}, 时间：{}".format(
            self.table_name, name, datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        ))

    @staticmethod
    def data_check(df):
        """检查每日数据是否有某个小时段遗漏
        :param df: 数据
        :return: True、False
        """
        dates = []
        for item in df.index:
            if item.hour not in dates:
                dates.append(item.hour)
        if len(dates) == 24:
            return True
        return False

    def file_clear(self):
        """删除数据文件
        """
        data_path = self.data_path
        shutil.rmtree(data_path)
        os.makedirs(self.data_path)

    def run(self):
        engine = self.get_conn()

        try:
            print("*" * 100)
            success = input(
                "===============     Start {} 数据解析开始,回车以继续，任意键结束     ===============\n".format(
                    "岗巴" if "kamba" in self.table_name else "错那" if "cona" in self.table_name else "天津"
                )
            )

            if not success.strip():
                items = self.get_data()
                if self.insert_to_sql(items, engine):
                    self.original_table_backup()  # 备份

                    update_realtime_data(self.table_name)   # 公式计算

                    backup_statistics_data(self.table_name, self.statistics_backup)  # 计算值备份

                    self.clear_backup()  # 清除备份
                    self.file_clear()   # 清除数据文件
            else:
                logging.info(
                    "===============     End {} 操作已取消 {}     ===============\n".format(
                        "岗巴" if "kamba" in self.table_name else "错那" if "cona" in self.table_name else "天津",
                        datetime.today().strftime("%Y-%m-%d %H:%M:%S")
                    )
                )
                print("===============     操作已取消     ===============")

        except Exception as e:
            logging.info("数据解析异常，错误原因：{}, 当前时间：{}".format(e, datetime.today().strftime("%Y-%m-%d %H:%M:%S")))
            traceback.print_exc()
            # print("数据解析异常，错误原因：{}, 当前时间：{}".format(e, datetime.today().strftime("%Y-%m-%d %H:%M:%S")))
        finally:
            print(
                "===============     End {} 数据解析完成 {}     ===============\n".format(
                    "岗巴" if "kamba" in self.table_name else "错那" if "cona" in self.table_name else "天津",
                    datetime.today().strftime("%Y-%m-%d %H:%M:%S")
                )
            )
            logging.info(
                "===============     End {} 数据解析完成 {}     ===============\n".format(
                    "岗巴" if "kamba" in self.table_name else "错那" if "cona" in self.table_name else "天津",
                    datetime.today().strftime("%Y-%m-%d %H:%M:%S")
                )
            )
            logging.info("*" * 100)
            print("*" * 100)
            engine.dispose()


if __name__ == '__main__':
    print("*"*113)
    print("*" * 46 + "     请确认处理项目     " + "*" * 46 + "\n")
    print("*" * 46 + "     1. 岗巴数据     " + "*" * 48 + "\n")
    print("*" * 46 + "     2. 错那数据     " + "*" * 48 + "\n")
    print("*" * 46 + "     3. 天津数据     " + "*" * 48 + "\n")
    choose = input("*" * 46 + "     请确认处理项目，序号输入后回车确认     " + "*" * 31 + "\n")

    base_path = "./data/conf" if platform.system() == "Windows" else "/home/data_format/data/conf"

    conf = {
        "1": os.path.join(base_path, "conf_kamba.ini"),
        "2": os.path.join(base_path, "conf_cona.ini"),
        "3": os.path.join(base_path, "conf_tianjin.ini"),
    }

    DataFormat(conf[choose.strip()]).run()









