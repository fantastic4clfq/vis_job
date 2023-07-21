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
#更新完成了2.1.2的内容 
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
    #7.21更新，结果还是有细微的差距，后来实际操作发现有些车道就是一个fid，不是多个重叠组成的道路，分类好的道路在car_data_manage的注释里面
    print(count)
    
def car_data_manage():
    """
    这个函数是处理车辆数据，只保留了还在运动的车辆，同时去除了道路中的行人和其他非机动车交通参与者
    下面是已经统计分割好的机动车道
    [1442, 1926, 1930]
    [1443, 1927, 1931]
    [1444, 1928, 1932]
    [1445, 1929, 1933]
    [1446, 1921]
    [1447, 1922]
    [1448, 1923]
    [1449, 1924]
    [1450, 1925]
    [1898, 1899, 80569, 1903, 1907]
    [1898, 1902, 1906]
    [1900, 1904, 80568, 1908]
    [1901, 1905, 80567, 1909]
    [1910, 1914, 1918, 80571]
    [1910, 1913, 1917, 80570]
    [1911, 1915, 1919, 80572]
    [1912, 1916, 1920, 80573]
    [1934, 1937, 80692, 1940, 1943, 1947, 1951]
    [1935, 1938, 80693, 1941, 1944, 1948, 1952]
    [1936, 1939, 80694, 1942, 1945, 1949, 1953]
    [1936, 1954, 1946, 1950]
    [1955, 1958, 1961, 1964, 1968, 80691]
    [1956, 1959, 1962, 1965, 1969, 80690]
    [1957, 1960, 1963, 1966, 1970, 80689]
    [1957, 1967, 80688, 1971]
    [1975, 5185]
    [1976, 5186]
    [1977, 5187]
    [1978, 5188]
    [1974]
    [1973]
    [1972]
    """
    with jsonlines.open('high_value_vis/updated_part_00000.json','r') as reader:
        with jsonlines.open("high_value_vis/cesi_part_00000.json", mode='w') as writer:
            for json_obj in reader:
                center_point = json_obj['position']
                center_point = json.loads(center_point)
                shape_data = json_obj['shape']
                shape_data = json.loads(shape_data)
                rec_coords = []
                x1 = center_point['x']-0.5*shape_data['x']
                x4 = x1
                x2 = center_point['x']+0.5*shape_data['x']
                x3 = x2
                y1 = center_point['y']+0.5*shape_data['y']
                y2 = y1
                y4 = center_point['y']-0.5*shape_data['y']
                y3 = y4
                rec_coords.append((x1,y1))
                rec_coords.append((x2,y2))
                rec_coords.append((x3,y3))
                rec_coords.append((x4,y4))
                is_moving = json_obj['is_moving']
                type = json_obj['type']
                if is_moving and (type in [1, 3, 4, 5, 6, 10]):
                    new_json_obj = {
                        "id":json_obj['id'],
                        "type":type,
                        "time_meas":json_obj['time_meas'],
                        "coords": rec_coords,
                        "on_road":0
                    }
                    writer.write(new_json_obj)

def timemeas_change():
    """将十六位时间戳转换成十位时间戳，并按照时间戳大小排序"""
    with jsonlines.open("high_value_vis/cesi_part_00000.json",'r') as reader:
        data = [line for line in reader]
        # 提取每个 JSON 对象中名为 "timemeas" 的时间戳，并在每个对象中增加一个 "_timestamp" 键来保存时间戳的值
    for obj in data:
        timemeas_timestamp = obj.get("time_meas", None)
        if timemeas_timestamp:
            time_meas = int(timemeas_timestamp/1000)
            obj["_timestamp"] = time_meas
            obj['time_meas'] = time_meas
        # 使用 "timemeas" 时间戳的值对 JSON 对象进行排序
    sorted_data = sorted(data, key=lambda x: x.get("_timestamp", 0))
    # 移除中间增加的 "_timestamp" 键
    for obj in sorted_data:
        obj.pop("_timestamp", None)
    
    # 可选：将排序后的 JSON 对象写回 JSON Lines 文件
    with open('high_value_vis/cesi_part_00000.json', 'w', encoding='utf-8') as file:
        for obj in sorted_data:
            json.dump(obj, file)
            file.write('\n')
#statistics()
#timemeas_change()
def total_flow():
    """判断各个车在什么道路上"""
    #给车道编号
    A,B,C,D,E,F,G,H = [],[],[],[],[],[],[],[]
    A.append([1912, 1916, 1920, 80573])
    A.append([1911, 1915, 1919, 80572])
    A.append([1910, 1914, 1918, 80571])
    A.append([1913, 1917, 80570])
    B.append([1898, 1902, 1906])
    B.append([1899, 80569, 1903, 1907])
    B.append([1900, 1904, 80568, 1908])
    B.append([1901, 1905, 80567, 1909])
    C.append([1957, 1960, 1963, 1966, 1970, 80689])
    C.append([1967, 80688, 1971])
    C.append([1955, 1958, 1961, 1964, 1968, 80691])
    C.append([1956, 1959, 1962, 1965, 1969, 80690])
    D.append([1934, 1937, 80692, 1940, 1943, 1947, 1951])
    D.append([1935, 1938, 80693, 1941, 1944, 1948, 1952])
    D.append([1936, 1939, 80694, 1942, 1945, 1949, 1953])
    D.append([1954, 1946, 1950])
    E.append([1450, 1925])
    E.append([1449, 1924])
    E.append([1448, 1923])
    E.append([1447, 1922])
    E.append([1446, 1921])
    F.append([1445, 1929, 1933])
    F.append([1444, 1928, 1932])
    F.append([1443, 1927, 1931])
    F.append([1442, 1926, 1930])
    G.append([1974])
    G.append([1973])
    G.append([1972])
    H.append([1975, 5185])
    H.append([1976, 5186])
    H.append([1977, 5187])
    H.append([1978, 5188])
    roads = [A,B,C,D,E,F,G,H]
    with jsonlines.open('high_value_vis/cesi_part_00000.json','r') as reader, jsonlines.open('high_value_vis/cesi1_part_00000.json', mode='w') as writer:
        gdf = gpd.read_file('high_value_vis/road10map/laneroad10.geojson')
        #统计数据量初始化
        road_count={}
        count_id = 1
        road_geo = {}
        linestrings = []
        for road in roads:
            for path in road:
                for path_id in path:
                    #提前准备好32条路线的地理数据
                    geo = gdf[gdf['fid']== path_id]
                    target_geometry = geo.iloc[0].geometry
                    linestrings.append(target_geometry)
                merged_linstrings = linestrings[0]
                for linestring in linestrings[1:]:
                    merged_linstrings = merged_linstrings.union(linestring)
                road_count[count_id] = 0
                road_geo[count_id] = merged_linstrings
                count_id += 1

        for car_obj in reader:
            
            rectangle_coords = car_obj['coords']
            on_road = car_obj['on_road']
            # 将长方形的四个顶点坐标转换为 Shapely 的 Polygon 对象
            rectangle = Polygon(rectangle_coords)
            flag = False
            count_id = 1
            for road in roads:
                for path in road:
                    if on_road != count_id:
                        target_geometry = road_geo[count_id]
                        # 判断指定 id 的 LineString 对象是否与长方形有重叠
                        overlap = target_geometry.intersects(rectangle)
                        if overlap:
                            #如果重叠，那么就直接终止所有的循环，开始到下一个car_obj
                            flag = True
                            car_obj['on_road'] = count_id
                            writer.write(car_obj)
                            print(count_id)
                            break
                    count_id += 1
                    if flag:
                        break
                if flag:
                    break
    import os
    os.replace('high_value_vis/cesi1_part_00000.json', 'high_value_vis/cesi_part_00000.json')

def total_flow_statistics():
    """统计每一条道路在不同时段中的流量，最后输出结果是一个dataframe，列代表车道编号从1~32，
    行代表时间从0~23，但是不是0点到23点，还没有处理成真实世界的时间"""
    with jsonlines.open('high_value_vis/cesi_part_00000.json','r') as reader:
        time_stage = {}
        road_count = {}
        road_fid = {}
        for i in range(1,33):
            road_count[i] = 0
        for i in range(0,24):
            time_stage[i] = {}
            for j in range(1,33):
               time_stage[i][j] = 0
        for car_obj in reader:
            on_road = car_obj['on_road']
            fid = car_obj['id']
            timemeas = car_obj['time_meas']
            clock = int((timemeas-1681315197699)/3600000)
            if fid not in road_fid:
                road_fid[fid] = on_road
                time_stage[clock][on_road] += 1
            else:
                if road_fid[fid] == on_road:
                    continue
                else:
                    road_fid[fid] = on_road
                    time_stage[clock][on_road] += 1
        import pandas as pd
        df = pd.DataFrame.from_dict(time_stage, orient='index')
        print(df)
