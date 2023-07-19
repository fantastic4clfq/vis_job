#数据预处理，按照那个杨彬学长的文档做了2.1.1的内容，每次处理文件的时候可能需要改一改函数里面的文件路径


import jsonlines
from collections import Counter
import json
import geopandas as gpd

def data_clean():
    """数据清洗，去除type=-1的json对象"""
    count = 0
    # 打开原始 JSON Lines 文件和一个新文件用于写入更新后的数据
    with jsonlines.open('high_value_vis\part-00001-905505be-27fc-4ec6-9fb3-3c3a9eee30c4-c000.json', 'r') as reader, jsonlines.open('high_value_vis/updated_part_00001.json', 'w') as writer:
        # 遍历每一行数据
        for obj in reader:
            if obj['type'] == -1:
                # 跳过要删除的特定对象
                count+=1
                continue

            # 写入更新后的 JSON 对象到新文件
            writer.write(obj)
    print(count," rows affected.")
    #统计得到00000数据被删除了39073个数据，part_00001数据被删除了39064个

def data_consistency():
    """数据一致性处理，找出fid相同但是type不同的数据，然后把他们处理为对应ID最多的type"""
    with jsonlines.open('high_value_vis/updated_part_00001.json','r') as reader:
        data = list(reader)
        type_id = {}
        count = 0
        for row in data:
            row_id = row['id']
            row_type = row['type']
            if row_id in type_id:
                type_id[row_id].append(row_type)
            else:
                type_id[row_id] = [row_type]
        # 遍历 JSON Lines 文件，将具有相同 ID 的条目的属性值更新为出现频率最多的属性值

        for obj in data:
            obj_id = obj['id']
            type_list = type_id[obj_id]
            most_common_attribute = Counter(type_list).most_common(1)[0][0]
            obj['type'] = most_common_attribute
        
    with jsonlines.open('high_value_vis/updated_data_1.json', 'w') as writer:
        writer.write_all(data)

def find_different():
    """找出文件中是否存在id相同， 但是type不同的数据"""
    with jsonlines.open('high_value_vis/updated_data_1.json','r') as reader:
        type_id = {}
        count = 0
        for row in reader:
            row_id = row['id']
            row_type = row['type']
            if row_id in type_id:
                if type_id[row_id] != row_type:
                    count+=1
            else:
                type_id[row_id] = row_type
        print(count)
    
def data_intersects():
    """用于判断有多少条机动车道"""
    # 读取 GeoJSON 文件
    data = gpd.read_file('high_value_vis/road10map/laneroad10.geojson')

    # 提取 LineString 几何对象及其 ID
    linestrings = data[(data['turn_type'] == 0) | (data['turn_type']==1)]
    linestrings = linestrings[linestrings['lane_no']!=0]

    # 存储重叠的 LineString 对象的 ID
    overlapping_ids = []

    # 判断 LineString 之间是否有重叠
    for i in range(len(linestrings)):
        for j in range(i + 1, len(linestrings)):
            if linestrings['geometry'].iloc[i].intersects(linestrings['geometry'].iloc[j]):
                overlapping_ids.append((linestrings['fid'].iloc[i], linestrings['fid'].iloc[j]))

    # 打印存在重叠的 LineString 对象的 ID
    if len(overlapping_ids) > 0:
        print("LineString 之间存在重叠：")
        for ids in overlapping_ids:
            print("LineString ID:", ids[0], "和", "LineString ID:", ids[1])
    else:
        print("LineString 之间没有重叠")
    road_set = list()
    count = 0
    flag = False
    for obj in overlapping_ids:
        if len(road_set)==0:
            road_set.append([obj[0],obj[1]])
        else:
            for yuansu in road_set:
                if (obj[0] in yuansu) or (obj[1] in yuansu):
                    if obj[1] not in yuansu:
                        yuansu.append(obj[1])
                    if obj[0] not in yuansu:
                        yuansu.append(obj[0])
                    flag = True
                    break
            if not flag:
                road_set.append([obj[0],obj[1]])
            flag = False
    print(len(road_set))
    for road in road_set:
        count +=len(road)
        print(road)
    #road_set中的每一个列表表示一条道路，结果为25条道路，车道fid一致，如果把文中提到的车道汇合情况算作一条则一致
    print(count)
