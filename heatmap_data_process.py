import json
from collections import Counter

import jsonlines
from collections import Counter
import geopandas as gpd

import csv


def data_consistency(input_file, output_file):
    # 读取JSON文件
    with open(input_file, "r", encoding="utf-8") as file:
        data = [json.loads(line) for line in file]
    
    # 根据fid进行分组统计type出现的次数
    counter_dict = {}
    for item in data:
        fid = item["id"]
        type_value = item["type"]
        if fid not in counter_dict:
            counter_dict[fid] = Counter()
        counter_dict[fid][type_value] += 1
    
    # 处理数据，选择每个fid对应出现次数最多的type，并计算受影响的行数
    affected_rows = 0
    for item in data:
        fid = item["id"]
        type_value = item["type"]
        max_type = counter_dict[fid].most_common(1)[0][0]
        if type_value != max_type:
            item["type"] = max_type
            affected_rows += 1
    
    # 将更新后的数据写入JSON文件
    with open(output_file, "w", encoding="utf-8") as file:
        for item in data:
            json.dump(item, file, ensure_ascii=False)
            file.write("\n")
    
    # 返回受影响的行数
    return affected_rows

# 调用函数
# input_file = r"vis_data\data\part-00001-f54e552a-6c3d-4dc3-bb38-550e2f491b47-c000.json"
# output_file = "1.json"
# affected_rows = data_consistency(input_file, output_file)
#print("受影响的行数：", affected_rows)


def clean_json_data(input_file, output_file):
    # 读取JSON文件
    with open(input_file, "r", encoding="utf-8") as file:
        data = [json.loads(line) for line in file]

    # 过滤type值为-1、0、2、3、7、8、9、10、11、12的JSON对象
    filtered_data = [item for item in data if item.get("type", -1) not in [-1, 0, 2, 3, 7, 8, 9, 10, 11, 12]]

    # 计算删除的行数
    deleted_count = len(data) - len(filtered_data)

    # 将过滤后的数据写入JSON文件
    with open(output_file, "w", encoding="utf-8") as file:
        for item in filtered_data:
            json.dump(item, file, ensure_ascii=False)
            file.write("\n")

    # 返回删除的行数
    return deleted_count

# 调用函数
#input_file = "1.json"
#output_file = "2.json"
#deleted_count = clean_json_data(input_file, output_file)
#print("删除的行数：", deleted_count)

import json

def filter_json_file(input_file, output_file):
    # 读取JSON文件
    with open(input_file, "r") as file:
        data = [json.loads(line) for line in file]

    # 过滤掉seq值为-1的JSON对象
    filtered_data = [item for item in data if item.get("seq", -1) != -1]

    # 计算删除的行数
    deleted_count = len(data) - len(filtered_data)

    # 将更新后的JSON对象写入新文件
    with open(output_file, "w") as file:
        for item in filtered_data:
            json.dump(item, file)
            file.write("\n")

    # 返回删除的行数
    return deleted_count

# 调用函数并获取删除的行数
# input_file = "2.json"
# output_file = "3.json"
# deleted_count = filter_json_file(input_file, output_file)
# print("已将更新后的JSON对象写入到新文件:", output_file)
# print("删除的行数：", deleted_count)


def sort_objects_by_timestamp(input_file, output_file):
    # 读取输入文件
    with open(input_file, 'r') as file:
        data = [json.loads(line) for line in file]

    # 按照"time_meas"属性值递增排序对象
    sorted_data = sorted(data, key=lambda x: x['time_meas'])

    # 写入排序后的对象到输出文件
    with open(output_file, 'w') as file:
        for obj in sorted_data:
            file.write(json.dumps(obj) + '\n')

# 调用函数将对象按时间戳排序后写入新文件
#sort_objects_by_timestamp("3.json", "4.json")


def json_to_csv(json_file, csv_file):
    """json转csv"""
    with open(json_file, 'r') as f:
        data = []
        for line in f:
            data.append(json.loads(line))

    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(data[0].keys())  # 写入CSV文件的表头

        for item in data:
            writer.writerow(item.values())  # 写入数据行

# json_file = '4.json'
# csv_file = '4.csv'
# json_to_csv(json_file, csv_file)

import pandas as pd

def remove_column(csv_file, column_name):
    # 读取CSV文件
    df = pd.read_csv(csv_file)

    # 删除指定列
    df.drop(column_name, axis=1, inplace=True)

    # 保存修改后的结果回CSV文件
    df.to_csv(csv_file, index=False)

# csv_file = '4.csv'
# column_name = 'z'
# remove_column(csv_file, column_name)


def split_position_column(csv_file):
    # 读取CSV文件
    df = pd.read_csv(csv_file)

    # 拆分 "position" 列为 "x", "y", "z" 列
    df[['x', 'y', 'z']] = df['position'].str.split(',', expand=True)

    # 删除原始的 "position" 列
    df.drop('position', axis=1, inplace=True)

    # 保存修改后的结果回CSV文件
    df.to_csv(csv_file, index=False)

# csv_file = '4.csv'
# split_position_column(csv_file)

import pandas as pd
import re

def convert_x_column_to_float(file_path):
    # 读取CSV文件
    df = pd.read_csv(file_path)
    # 使用正则表达式提取数字
    df["y"] = df["y"].apply(lambda y: float(re.findall(r'-?\d+\.?\d*', str(y))[0]))
    # 将转化后的结果写入原来的CSV文件中
    df.to_csv(file_path, index=False)
    # 返回转化后的DataFrame
    return df

# 调用函数并打印转化后的结果
# file_path = "4.csv"
# converted_df = convert_x_column_to_float(file_path)



import pandas as pd
import pytz

def convert_time_to_hour(file_path):
    # 读取CSV文件
    df = pd.read_csv(file_path)

    # 将"ms_no"列的时间戳保留为10位数字
    df["ms_no"] = df["ms_no"].astype(str).str[:10]

    # 将"ms_no"列的时间戳转换为日期时间格式
    df["ms_no"] = pd.to_datetime(df["ms_no"], unit='s')

    # 设置时区为北京时间
    timezone = pytz.timezone("Asia/Shanghai")
    df["ms_no"] = df["ms_no"].dt.tz_localize(pytz.utc).dt.tz_convert(timezone)

    # 提取小时的数字并赋值给新列"hour"
    df["hour"] = df["ms_no"].dt.hour

    # 将转换后的结果写入原来的CSV文件中
    df.to_csv(file_path, index=False)

    # 返回转换后的DataFrame
    return df

# 调用函数并打印转换后的结果
# file_path = "4.csv"
# converted_df = convert_time_to_hour(file_path)
# print(converted_df)

def filter_data_by_hour(input_file, output_file, hour_value):
    # 读取原始CSV文件
    df = pd.read_csv(input_file)

    # 选择"hour"列值为指定小时的行
    filtered_df = df[df["hour"] == hour_value]

    # 将筛选后的结果写入新的CSV文件
    filtered_df.to_csv(output_file, index=False)

    # 返回筛选后的DataFrame
    return filtered_df

# 调用函数并打印筛选后的结果
input_file = "4.csv"
output_file = "part1_16hour_data.csv"
hour_value = 16
filtered_data = filter_data_by_hour(input_file, output_file, hour_value)
print(filtered_data)