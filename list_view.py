"""本文件主要是完成数据的处理，最后将数据存储到数据库中"""
import jsonlines
from collections import Counter
import json
import os
import geopandas as gpd
from shapely.geometry import LineString, Point, Polygon
import multiprocessing
from alive_progress import alive_bar



class HighValue():
    def __init__(self) -> None:
        pass

    def data_manage(self, inx):
        """
        用于处理车辆数据，在执行这一步的时候可以先考虑将所有的数据都合并到一个文件中
        首先需要将数据清洗、进行一致性处理、还要数据按照时间顺序排列好
        然后最需要的还是判断车辆现在在哪一条路上
        """
        # self.data_clean(inx)的内容
        """数据清洗，去除type=-1的json对象"""
        count = 0
        # 打开原始 JSON Lines 文件和一个新文件用于写入更新后的数据
        with jsonlines.open('part-0000{0}.json'.format(inx), 'r') as reader, jsonlines.open('updated_part_temp{}.json'.format(inx), 'w') as writer:
            #删除掉id为-1的对象
            data = [obj for obj in reader if obj['type'] != -1]
            print("数据清理完成，开始一致性处理")
            #开始进行数据一致性处理
            #首先判断是否存在一个id对应多种type的情况
            type_id = {}
            count = 0
            for row in data:
                row_id = row['id']
                row_type = row['type']
                #type_id是一个字典的嵌套类型{id:{type:次数}}
                if row_id in type_id:
                    if row_type not in type_id[row_id]:
                        type_id[row_id][row_type] = 1
                    else:
                        type_id[row_id][row_type] += 1
                else:
                    #如果没有就初始化字典
                    type_id[row_id] = {row_type : 1}
            #只保留type_id中列表长度大于1的部分
            type_id = {key:value for key, value in type_id.items() if len(value) > 1}
            print("开始写入文件")
            #开始遍历type_id中的文件，将对应的对象转变为出现最多的type
            count = 0
            for id, type_num_dic in type_id.items():
                most_type = max(type_num_dic.values())
                for obj in data:
                    if obj['id'] == id:
                        obj['type'] = most_type
                        count +=1
            print("一致性处理完成，共有{}行执行了一致性处理".format(count))
            writer.write_all(data)


    def data_clean(self, inx):
        """数据清洗，去除type=-1的json对象"""
        count = 0
        # 打开原始 JSON Lines 文件和一个新文件用于写入更新后的数据
        with jsonlines.open('part-0000{0}.json'.format(inx), 'r') as reader, jsonlines.open('updated_part_temp{}.json'.format(inx), 'w') as writer:
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

    def data_consistency(self, inx):
        """数据一致性处理，找出fid相同但是type不同的数据，然后把他们处理为对应ID最多的type"""
        with jsonlines.open('updated_part_temp{}.json'.format(inx),'r') as reader:
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
            
        with jsonlines.open('updated_data_{}.json'.format(inx), 'w') as writer:
            writer.write_all(data)
        os.replace('updated_part_temp{}.json'.format(inx), 'updated_data_{}.json'.format(inx))

    def lane_classify(self):
        """判断有几条道路，路径有重叠的被解释为一条车道，处在同一路段的车道都会被分类为同一条道路"""
        # 读取 GeoJSON 文件
        data = gpd.read_file('road2-12-9road\laneroad_with9road.geojson')
        
        #提取 LineString 几何对象及其 ID
        #首先只考虑机动车道
        linestrings = data[data['road_sec_id'] != 0]
        # 存储重叠的 LineString 对象的 ID
        overlapping_ids = []
        equal_sec_ids = []
        #车道id存储
        lane_set = list()
        road_set = list()
        linestrings['flag'] = 1
        # 判断 LineString 之间是否有重叠
        for i in range(len(linestrings)):
            for j in range(i + 1, len(linestrings)):
                if linestrings['geometry'].iloc[i].intersects(linestrings['geometry'].iloc[j]):
                    overlapping_ids.append((linestrings['fid'].iloc[i], linestrings['fid'].iloc[j]))
                    equal_sec_ids.append((linestrings['fid'].iloc[i], linestrings['fid'].iloc[j]))
                    linestrings.iloc[i, linestrings.columns.get_loc('flag')] = 0
                    linestrings.iloc[j, linestrings.columns.get_loc('flag')] = 0
                elif linestrings['road_sec_id'].iloc[i] == linestrings['road_sec_id'].iloc[j]:
                    equal_sec_ids.append((linestrings['fid'].iloc[i], linestrings['fid'].iloc[j]))
                    linestrings.iloc[i, linestrings.columns.get_loc('flag')] = 0
                    linestrings.iloc[j, linestrings.columns.get_loc('flag')] = 0

        flag = False
        for i in range(len(linestrings)):
            if linestrings['flag'].iloc[i]:
                lane_set.append([linestrings['fid'].iloc[i]])
        #开始判断总共有多少条机动车道
        for obj in overlapping_ids:
            union_set = set(obj)
            if len(lane_set)==0:
                lane_set.append([obj[0],obj[1]])
            else:
                for yuansu in lane_set:
                    set1 = set(yuansu)
                    if union_set&set1:
                        # if obj[1] not in yuansu:
                        #     yuansu.append(obj[1])
                        # if obj[0] not in yuansu:
                        #     yuansu.append(obj[0])
                        #如果在里面就加入到union_set中
                        lane_set.remove(yuansu)
                        union_set = union_set.union(set1)
                        flag = True
                        #需要再遍历一次，可能还有一组
                if flag:
                    union_set = list(union_set)
                    #将合并的道路加入到集合中
                    lane_set.append(union_set)
                else:
                    lane_set.append([obj[0],obj[1]])
                flag = False
        for lane in lane_set:
            print(lane)
        print("共有",len(lane_set),"条车道")

        flag = False
        for i in range(len(linestrings)):
            if linestrings['flag'].iloc[i]:
                road_set.append([linestrings['fid'].iloc[i]])
        #开始判断总共有多少条机动车道
        for obj in equal_sec_ids:
            union_set = set(obj)
            if len(road_set)==0:
                road_set.append([obj[0],obj[1]])
            else:
                for yuansu in road_set:
                    set1 = set(yuansu)
                    if union_set&set1:
                        # if obj[1] not in yuansu:
                        #     yuansu.append(obj[1])
                        # if obj[0] not in yuansu:
                        #     yuansu.append(obj[0])
                        #如果在里面就加入到union_set中
                        road_set.remove(yuansu)
                        set1 = set(yuansu)
                        union_set = union_set.union(set1)
                        flag = True
                        #需要再遍历一次，可能还有一组
                if flag:
                    union_set = list(union_set)
                    #将合并的道路加入到集合中
                    road_set.append(union_set)
                else:
                    road_set.append([obj[0],obj[1]])
                flag = False
        for lane in road_set:
            print(lane)
        print("共有",len(road_set),"条道路")
        #清除变量
        del overlapping_ids, equal_sec_ids, data
        #按照同样的办法来确定非机动车道
        # 读取 GeoJSON 文件
        data = gpd.read_file('road2-12-9road\laneroad_with9road.geojson')
        #提取 LineString 几何对象及其 ID
        #首先只考虑机动车道
        linestrings = data[(data['category'] == 2) & (data['turn_type'] == 0)]
        # 存储重叠的 LineString 对象的 ID
        non_lane_ids = []
        #车道id存储
        non_lane_set = list()
        linestrings['flag'] = 1
        # 判断 LineString 之间是否有重叠
        for i in range(len(linestrings)):
            for j in range(i + 1, len(linestrings)):
                if linestrings['geometry'].iloc[i].intersects(linestrings['geometry'].iloc[j]):
                    non_lane_ids.append((linestrings['fid'].iloc[i], linestrings['fid'].iloc[j]))
                    linestrings.iloc[i, linestrings.columns.get_loc('flag')] = 0
                    linestrings.iloc[j, linestrings.columns.get_loc('flag')] = 0

        flag = False
        for i in range(len(linestrings)):
            if linestrings['flag'].iloc[i]:
                non_lane_set.append([linestrings['fid'].iloc[i]])
        #开始判断总共有多少条人行道
        for obj in non_lane_ids:
            union_set = set(obj)
            if len(non_lane_set)==0:
                non_lane_set.append([obj[0],obj[1]])
            else:
                for yuansu in non_lane_set:
                    set1 = set(yuansu)
                    if union_set&set1:
                       # if obj[1] not in yuansu:
                        #     yuansu.append(obj[1])
                        # if obj[0] not in yuansu:
                        #     yuansu.append(obj[0])
                        #如果在里面就加入到union_set中
                        non_lane_set.remove(yuansu)
                        set1 = set(yuansu)
                        union_set = union_set.union(set1)
                        flag = True
                        #需要再遍历一次，可能还有一组
                if flag:
                    union_set = list(union_set)
                    #将合并的道路加入到集合中
                    non_lane_set.append(union_set)
                else:
                    non_lane_set.append([obj[0],obj[1]])
                flag = False
        for lane in non_lane_set:
            print(lane)
        print("共有",len(non_lane_set),"条非机动道")
        print("完成道路分类，即将开始判断道路区域")
        return lane_set, road_set, non_lane_set
    
    def get_lane_area(self, lane_set, non_lane_set):
        """获得车道区域对象，用于判断车辆在哪一条道路上"""
        lane_gdf = gpd.read_file('road2-12-9road/laneroad_with9road.geojson')
        boundary_gdf = gpd.read_file('road2-12-9road/boundaryroad_with9road.geojson')
        left_boundary = {}
        right_boundary = {}
        lane_area = {}
        #利用count来对车道标号，这样就可以确定是哪一条车道
        count = 1
        #确定好每一条道路的左右边界
        print("开始处理机动车道")
        for lane in lane_set:
            left_linestrings = []
            right_linestrings = []
            for lane_id in lane:
                if lane_id == 3106:
                    geo = lane_gdf[lane_gdf['fid']==lane_id].iloc[0]
                    #确定一条车道上的边界，每一段都加到列表里
                    left_id = geo['left_boundary_id']
                    left_geo = boundary_gdf[boundary_gdf['fid']==left_id].iloc[0].geometry
                    left_geo = [(x,y) for x,y,z in left_geo.coords]
                    left_linestrings.extend(left_geo)
                    right_linestrings.extend([(343,-273), (360,-248)])
                else:
                    geo = lane_gdf[lane_gdf['fid']==lane_id].iloc[0]
                    #确定一条车道上的边界，每一段都加到列表里
                    left_id = geo['left_boundary_id']
                    right_id = geo['right_boundary_id']
                    
                    if left_id==0 or right_id == 0:
                        #如果边界不存在，就直接pass掉,主要道路合并时是不存在边界的
                        pass
                    else:
                        # print(boundary_gdf[boundary_gdf['fid']==right_id])
                        left_geo = boundary_gdf[boundary_gdf['fid']==left_id].iloc[0].geometry
                        right_geo = boundary_gdf[boundary_gdf['fid']==right_id].iloc[0].geometry
                        left_geo = [(x,y) for x,y,z in left_geo.coords]
                        right_geo = [(x,y) for x,y,z in right_geo.coords]
                        left_linestrings.extend(left_geo)
                        right_linestrings.extend(right_geo)
                        # print(type(left_geo))
                        # print(left_geo)
            # 将对应道路的边界线加入到字典中
            left_boundary[count] = left_linestrings
            right_boundary[count] = right_linestrings
            left_linestrings = sorted(left_linestrings, key=lambda point:point[0])
            right_linestrings = sorted(right_linestrings, key=lambda point:point[0])
            #将一条车道的边界线连接起来，也就是merged_left_linestrings和merged_right_linestrings
            merged_coords = right_linestrings+left_linestrings[::-1]
            #利用shapely处理出左右两条边界线之间的区域用于判断车辆是否在这条车道
            area = Polygon(merged_coords)
            #将区域对象存储到lane_area中
            lane_area[count] = area
            count += 1

        #还有非机动车道,从这个count开始就是非机动车道
        parting_count = count
        print(parting_count,",开始处理非机动车道")

        for lane in non_lane_set:
            left_linestrings = []
            right_linestrings = []
            for lane_id in lane:
                geo = lane_gdf[lane_gdf['fid']==lane_id].iloc[0]
                #确定一条车道上的边界，每一段都加到列表里
                left_id = geo['left_boundary_id']
                right_id = geo['right_boundary_id']
                if left_id==0 or right_id == 0:
                    #如果边界不存在，就直接pass掉,主要道路合并时是不存在边界的
                    pass
                else:
                    # print(boundary_gdf[boundary_gdf['fid']==right_id])
                    left_geo = boundary_gdf[boundary_gdf['fid']==left_id].iloc[0].geometry
                    right_geo = boundary_gdf[boundary_gdf['fid']==right_id].iloc[0].geometry
                    left_geo = [(x,y) for x,y,z in left_geo.coords]
                    right_geo = [(x,y) for x,y,z in right_geo.coords]
                    left_linestrings.extend(left_geo)
                    right_linestrings.extend(right_geo)
            # 将对应道路的边界线加入到字典中
            left_boundary[count] = left_linestrings
            right_boundary[count] = right_linestrings
            left_linestrings = sorted(left_linestrings, key=lambda point:point[0])
            right_linestrings = sorted(right_linestrings, key=lambda point:point[0])
            #将一条车道的边界线连接起来，也就是merged_left_linestrings和merged_right_linestrings
            merged_coords = right_linestrings+left_linestrings[::-1]
            #利用shapely处理出左右两条边界线之间的区域用于判断车辆是否在这条车道
            area = Polygon(merged_coords)
            #将区域对象存储到lane_area中
            lane_area[count] = area
            count += 1
        print("车道区域判断完成，即将进行下一阶段")
        # data = {'id':[], 'geometry':[]}
        # for key, value in lane_area.items():
        #     data['id'].append(key)
        #     data['geometry'].append(value)
        # data_pd = gpd.GeoDataFrame(data)
        # output_geojson_filename = 'output.geojson'  # 你可以指定保存的文件名
        # data_pd.to_file(output_geojson_filename, driver='GeoJSON')
        return lane_area, parting_count
    
    def judge_lane(self, inx, lane_area, parting_count):
        """开始判断各种交通参与者在哪条道路"""
        #打开车辆位置数据
        with jsonlines.open('updated_part_temp{}.json'.format(inx),'r') as reader, jsonlines.open('final_part_{}.json'.format(inx), mode='w') as writer:
            print("开始判断交通参与者在哪条车道")
            #除去各种地理设施，如红绿灯、路障等
            data = [obj for obj in reader if (obj['type'] in [1,2,3,4,6,7,10])]
            # id_lane_dic = {}

            print("开始判断")
            count = 0
            with alive_bar(len(data)) as bar:
                for obj in data:
                    position = json.loads(obj['position'])
                    x,y = position['x'], position['y']
                    postion_point = Point(x,y)
                    count +=1
                    # if obj['id'] in id_lane_dic:
                    #     #如果在字典里面，直接判断是否还是之前那条车道
                    #     if lane_area[id_lane_dic[obj['id']]].contains(postion_point):
                    #         key = id_lane_dic[obj['id']]
                    #         obj['on_road'] = key
                    #         flag = 1
                    #         id_lane_dic[obj['id']] = key
                    #         print(key)
                    #         #判断是否在机动车道上面
                    #         if key >= parting_count:
                    #             obj['motor_lane'] = 0
                    #         else:
                    #             obj['motor_lane'] = 1
                    #         continue
                    #print(x," ",y)
                    #print("点加载完成",count)
                    flag = 0
                    for key, area in lane_area.items():
                        if area.contains(postion_point):
                            obj['on_road'] = key
                            flag = 1
                            # id_lane_dic[obj['id']] = key
                            # print(key)
                            #判断是否在机动车道上面
                            if key >= parting_count:
                                obj['motor_lane'] = 0
                            else:
                                obj['motor_lane'] = 1
                            break
                    if not flag:
                        # print('没有找到')
                        #说明车辆应该在某个路口处
                        obj['on_road'] = 0
                        obj['motor_lane'] = 1
                    bar()
            print("处理完成，开始写入数据")
            writer.write_all(data)
        # print("快要完成了")
        # os.replace('updated_part_temp{}.json'.format(inx), 'final_part_{}.json'.format(inx))

    def split(self, lane_set):
        """
        有些道路会出现分流和合并的现象，这时候使用原函数会将分开的车道仍然解释为同一条车道
        利用这个函数将道路从正确的位置分开是有必要的
        """
        lane_set.remove([129, 130, 163, 101, 134, 135, 106, 111, 116, 120, 124, 125])
        lane_set.append([124,129,134])
        lane_set.append([])
        lane_set.append([130, 163, 101, 135, 106, 111, 116, 120,125])
        lane_set.remove([128, 133, 166, 104, 105, 138, 109, 110, 114, 115, 119, 123])
        lane_set.append([105,110,115])
        lane_set.append([128, 133, 166, 104, 138, 109, 114, 119, 123])
        lane_set.remove([4938, 1691, 1692, 203])
        lane_set.append([1692])
        lane_set.append([1691])
        lane_set.append([4938,203])
        lane_set.remove([208, 213, 214, 219, 207])
        lane_set.append([208, 214, 219])
        lane_set.append([207, 213])
        lane_set.remove([210, 211, 216, 217, 205])
        lane_set.append([205,211,217])
        lane_set.append([210,216])
        lane_set.remove([1688,1689,200])
        lane_set.append([1688,200])
        # lane_set.append([1689,201])
        lane_set.remove([1910, 80570, 1913, 1914, 80571, 1917, 1918])
        lane_set.append([80570,1917,1913])
        lane_set.append([1910, 80571, 1914, 1918])
        lane_set.remove([1898, 1899, 1902, 1903, 1906, 1907, 80569])
        lane_set.append([1898,1902,1906])
        lane_set.append([1899, 1903, 1907, 80569])
        lane_set.remove([5185, 5186, 1975])
        lane_set.append([5185,1975])
        lane_set.append([5186,1976])
        lane_set.remove([1957, 1960, 1963, 1966, 1967, 80688, 80689, 1970, 1971])
        lane_set.append([80688, 1971, 1967])
        lane_set.append([1957, 1960, 1963, 1966, 80689, 1970])
        lane_set.remove([65, 1935, 1936, 1938, 1939, 1941, 1942, 1944, 1945, 1946, 1948, 1949, 1950, 1952, 1953, 1954, 80693, 80694])
        lane_set.append([65, 80693, 1935, 1938, 1941, 1944, 1948, 1952])
        lane_set.append([1953,1949, 1945, 1942, 1939, 1936, 80694])
        lane_set.append([1946, 1950, 1954])
        lane_set.remove([224, 229, 230, 235, 223])
        lane_set.append([223, 229])
        lane_set.append([224, 230, 235])
        lane_set.remove([226, 227, 232, 233, 221])
        lane_set.append([226, 232])
        lane_set.append([221, 227, 233])
        lane_set.remove([1002, 1003, 1005, 1006, 1008, 1009, 1011, 1014, 1015, 1018, 1019])
        lane_set.append([1003, 1006, 1009])
        lane_set.append([1015, 1019])
        lane_set.append([1002,  1005, 1008, 1011, 1014, 1018])
        lane_set.remove([1001, 1004, 1007, 1010, 1012, 1013, 1016, 1017])
        lane_set.append([1013,1017])
        lane_set.append([1001, 1004, 1007, 1010, 1012, 1016])
        lane_set.remove([1472, 1473, 1474, 1464, 1467, 1468, 1469])
        lane_set.append([1472,1467])
        lane_set.append([1473, 1468])
        lane_set.append([ 1474, 1469, 1464])
        lane_set.remove([1451, 1454, 1455, 1456, 1459, 1460, 1461])
        lane_set.append([1451, 1456, 1461])
        lane_set.append([1455, 1460])
        lane_set.append([1454, 1459])
        lane_set.remove([242, 243, 248, 249, 237])
        lane_set.append([242, 248])
        lane_set.append([237, 243, 249])
        lane_set.remove([240, 245, 246, 251, 239])
        lane_set.append([239, 245])
        lane_set.append([240, 246, 251])
        lane_set.remove([1553, 1554, 1548, 1549])
        lane_set.append([1554, 1549])
        lane_set.append([1553, 1548])
        lane_set.remove([256, 261, 262, 267, 255])
        lane_set.append([255,261])
        lane_set.append([256, 262, 267])
        lane_set.remove([258, 259, 264, 265, 253])
        lane_set.append([259, 265, 253])
        lane_set.append([258, 264])
        rest_lane = [155,156,157,139,140, 1972,1973,1974, 1020,1021,70370,1485,1490,1492,1491,1493]
        for lane in rest_lane:
            lane_set.append([lane])
        # lane_gdf = gpd.read_file('road2-12-9road/laneroad_with9road.geojson')
        # lane_list = list(lane_gdf['fid'])
        # for lane in lane_set:
        #     for lane_id in lane:
        #         if lane_id not in lane_list:
        #             print(lane)
        # print(len(lane_set),"changdu")
        return lane_set

    def timemeas_change(self, inx):
        """将十六位时间戳转换成十位时间戳，并按照时间戳大小排序"""
        print("开始进行时间排序")
        with jsonlines.open("updated_data_{}.json".format(inx),'r') as reader:
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
        with open('updated_data_{}.json'.format(inx), 'w', encoding='utf-8') as file:
            for obj in sorted_data:
                json.dump(obj, file)
                file.write('\n')
        print("结束时间排序")

    def overspeed(self):
        """
        判断车辆是否超速，数据中有车辆的瞬时速度，同时还需要打开道路数据文件确定道路的速度限制，就可以判断，
        文件中按照时间顺序排序，那么只要超过这个速度就将id加入到列表中，直到速度回复正常才从列表中弹出
        """
        #从文件中发现每条道路的限速几乎都是一样的60千米每小时
        limit_speed = 60
        over_speed_cars = []
        for i in range(0,10):
            with jsonlines.open("final_part_{}.json".format(i),"r") as reader:
                # {"id": 197348364, "seq": 280637347, "is_moving": 0, "position": "{\"x\":-44.87064,\"y\":-143.21825,\"z\":12.736328}",
                # "shape": "{\"x\":4.9819317,\"y\":1.9096746,\"z\":1.5957031}", "orientation": 1.1773413, "velocity": 0.0, "type": 1,
                # "heading": 1.1773413, "time_meas": 1681341914599746, "ms_no": 16813419145, "on_road": 0, "motor_lane": 1, "road_id": 0, "road_sec": 0}
                data = [obj for obj in reader if obj['type'] in [1,4,6] and obj['velocity']>limit_speed]
                over_speed_cars.extend(data)
            for obj in over_speed_cars:
                print(obj)

    def reverseVehicle(self, inx):
        """
        判断车辆是否逆行
        """
        with jsonlines.open("updated_data_{}.json".format(inx), "r") as reader:
            #筛选出车辆
            data = [obj for obj in reader if obj.type in [1,3,4,6]]
            #计算车辆朝向和道路的夹角
            for obj in data:
                pass
    
    def accelerate(self, inx):
        """判断车辆是否急加速还是急减速"""
        with jsonlines.open("updated_data_{}.json".format(inx), "r") as reader:
            #筛选出正在运动的车辆
            data = [obj for obj in reader if obj['type'] in [1,4,6] and obj['is_moving'] == 1]
            #然后计算车辆在0.5秒内的加速度
            length = len(data)
            print("开始记录车辆加速度")
            over_acc = []
            for i in range(0,length):
                v0 = data[i]['velocity']
                v1 = 0
                id = data[i]['id']
                start_time = data[i]['time_meas']
                print(start_time)
                for j in range(i+1,length):
                    if data[j]['id'] == id:
                        v1 = data[j]['velocity']
                        end_time = data[j]['time_meas']
                        print(end_time)
                        break
                duration =(end_time-start_time)/1000
                print(duration)
                acceleration = abs(v1-v0)/(duration)
                #如果加速度大于2.78则加入到列表中
                if acceleration >= 2.78 and duration>2:
                    over_acc.append(id)
                break

    def judge_intersection(self, lane_set:list):
        """防止没有把道路合并"""
        for i in range(0,len(lane_set)):
            set1 = set(lane_set[i])
            list1= lane_set[i]
            flag = False
            for j in range(i+1, len(lane_set)):
                set2 = set(lane_set[j])
                list2 = lane_set[j]
                if set1&set2:
                    print("有交集")
                    print(set1)
                    print(set2)
                    lane_set.remove(list2)
                    set1 = set1.union(set2)
                    flag = True
            if flag:
                lane_set.remove(list1)
                print(set1)
                lane_set.append(list(set1))
            return lane_set

    def total_manage(self, inx):
        """将几个函数包装为一个函数"""
        self.data_manage(inx)
        lane_set, road_set, non_lane_set = self.lane_classify()

        lane_set = self.judge_intersection(lane_set)
        print("chedao")
        
        road_set = self.judge_intersection(road_set)
        print("chedao")
        non_lane_set = self.judge_intersection(non_lane_set)
        
        lane_set = self.split(lane_set)
        # get_lane(lane_set)
        lane_area, parting_count = self.get_lane_area(lane_set, non_lane_set)
        self.judge_lane(inx, lane_area, parting_count)


def point_output():
    """输出点的geojson文件，用于判断点的位置"""
    with jsonlines.open("final_part_0.json","r") as reader:
        data = {'id':[], 'geometry':[]}
        count = 0
        for obj in reader:
            if count>1000:
                break
            if obj['on_road']==0:
                position = json.loads(obj['position'])
                x,y = position['x'], position['y']
                postion_point = Point(x,y)
                data['id'].append(count)
                data['geometry'].append(postion_point)
                count+=1
        data_pd = gpd.GeoDataFrame(data)
        output_geojson_filename = 'point.geojson'  # 你可以指定保存的文件名
        data_pd.to_file(output_geojson_filename, driver='GeoJSON')

def find_road_sec(lane_set, road_set):
    """为道路添加对应的路段号和路段"""
    #首先打开地理文件
    gdf = gpd.read_file("road2-12-9road\laneroad_with9road.geojson")
    #根据道路的fid确定它的路段
    dic_fid_sec = {}
    dic_road_lane = {}
    count = 1
    for lane in lane_set:
        for fid in lane:
            road_sec = gdf[gdf['fid']==fid].iloc[0].road_sec_id
            dic_fid_sec[count] = road_sec
            break
        for road_id in range(1, len(road_set)+1):
            lane = set(lane)
            road = set(road_set[road_id-1])

            if lane.intersection(road):
                dic_road_lane[count] = road_id
                break
        count+=1
    print(dic_fid_sec)
    print(dic_road_lane)
    return dic_fid_sec, dic_road_lane

def write_road_sec(inx, dic_fid_sec, dic_road_lane):
    data = [ ]
    with jsonlines.open('final_part_{}.json'.format(inx),'r') as reader:
        for obj in reader:
            
            lane_id = obj['on_road']
            moter_lane_id = obj['motor_lane']
            if lane_id==0 or moter_lane_id ==0:
                obj['road_id'] = 0
                obj['road_sec'] = 0
                data.append(obj)
            else:
                obj.pop('road_sec')
                obj['road_id'] = int(dic_road_lane[lane_id])
                obj['road_sec'] = int(dic_fid_sec[lane_id])
                data.append(obj)
    with jsonlines.open('final_part_1_{}.json'.format(inx),'w') as writer:
        for obj in data:
            writer.write(obj)



if __name__ == "__main__":
    # point_output()
    # #get_lane()
    ####################################################
    #执行进程
    # high_value = HighValue()
    # for i in range(1,2):
    #     print(i,"i")
    #     processes = []
    #     index = i*3+4
    #     for j in range(index, index+3):
    #         print(j,"j")
    #         process = multiprocessing.Process(target=high_value.total_manage, args=(j,))
    #         processes.append(process)

    #     # 启动这五个进程
    #     for process in processes:
    #         process.start()

    #     # 等待所有进程完成
    #     for process in processes:
    #         process.join()

    # print("All processes have finished.")

    # lane_set, road_set, non_lane_set = high_value.lane_classify()
    # lane_set = high_value.judge_intersection(lane_set)
    # lane_set = high_value.split(lane_set)
    # dic_fid_sec,dic_road_lane = find_road_sec(lane_set,road_set)
    # for i in range(1, 10):
    #     write_road_sec(i, dic_fid_sec,dic_road_lane)
    # for i in range(1,10):
    #     os.replace("final_part_1_{}.json".format(i), "final_part_{}.json".format(i))

    ################################################################
 
    high_value = HighValue()
    high_value.overspeed( )


