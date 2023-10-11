"""这个文件主要用于本文件用于判断交通过程中有哪些高价值场景，根据参考，高价值场景主要包括机动车超速行驶、车辆急加/减速、车辆
切入切出、逆行、倒车、非机动车/行人异常行为、拥堵、违章停车、占用非机动车道以及占用公交车道,同时将处理的文件信息全部存储到数据库中 """
import pymysql
import jsonlines
import json
import math
import csv
from alive_progress import alive_bar
import geopandas as gpd
from list_view import HighValue
from shapely.geometry import Polygon, MultiPolygon,Point
# import pandas as pd
# from decimal import Decimal

class RoadToDB():
    def __init__(self) -> None:
        pass
        # cursor = self.conn.cursor()
    def full_data_todb(self):
        """将道路数据总文件，也就是final_part_{}这个名字的文件存入数据库中"""
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ', db='GuoChuang')
        count = 1
        for i in range(0,10):
            value_list = []
            with jsonlines.open("final_part_{}.json".format(i), 'r') as reader:
                # json = {"id": 197348364, "seq": 280637347, "is_moving": 0, "position": "{\"x\":-44.87064,\"y\":-143.21825,\"z\":12.736328}", "shape": "{\"x\":4.9819317,\"y\":1.9096746,\"z\":1.5957031}", "orientation": 1.1773413, "velocity": 0.0, "type": 1, "heading": 1.1773413, "time_meas": 1681341914599746, "ms_no": 16813419145, "on_road": 0, "motor_lane": 1, "road_id": 0, "road_sec": 0}
                for obj in reader:
                    count_value = [count]
                    count_value.extend(obj.values())
                    value = tuple(count_value)
                    value_list.append(value)
                    count+=1
            cursor = conn.cursor()
            query = "INSERT INTO road_data (data_id, id, seq, is_moving, position, shape, orientation, velocity, type, heading, time_meas, ms_no, on_road, motor_lane, road_id, road_sec) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(query, value_list)
            conn.commit()
            cursor.close()
            conn.close()
            print(1)
    
    def over_speed(self):
        """找到对应的超速车辆数据"""
        speed_limit = 60/3.6
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "SELECT id, type, position, velocity, time_meas, road_id FROM guochuang.road_data where velocity>60/3.6 and is_moving = 1 and type in (1,4,6) order by time_meas"
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        overspeed_table = []
        overspeed_id_table = []
        for obj in result:
            #超速的id
            id = obj[0]
            #看是否已经被判断过了
            if id not in overspeed_id_table:
                #没有就加入到表格中
                road_id = obj[-1]
                typecar = obj[1]
                overspeed_id_table.append(id)
                id_list = [obj for obj in result if obj[0] == id]
                start_time = id_list[0][-2]
                end_time = id_list[-1][-2]
                duration = (end_time-start_time)/1000000
                if duration != 0:
                    speed_list = [single[3] for single in id_list]
                    mean_velo = sum(speed_list)/len(speed_list)
                else:
                    mean_velo = obj[3]
                overspeed_table.append((id, typecar, start_time, end_time, 60, mean_velo, road_id))
        cursor = conn.cursor()
        print(overspeed_table[1])
        sql = "INSERT INTO overspeed (id, type, start_time, end_time, speed_limit, mean_velo, road_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        cursor.executemany(sql, overspeed_table)
        # 提交更改
        conn.commit()
        # 关闭游标和连接
        cursor.close()
        conn.close()

    def judge_accelerate(self):
        """判断车辆是否急加速还是急减速"""
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "SELECT id, time_meas/1000000, on_road, velocity, type, road_id FROM road_data WHERE is_moving = 1 AND type IN (1, 4, 6) ORDER BY time_meas"
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        #然后计算车辆的加速度
        length = len(data)
        print("开始记录车辆加速度")
        #用于记录车辆id
        over_acc_id = []
        #用于记录急加速急减速车辆数据
        over_acc = []
        #开始遍历data里的数据
        with alive_bar(len(data)) as bar:
            for obj in data:
                #确定车的id和type，是否已经判断过这辆车
                car_id = obj[0]
                car_type = obj[-2]
                if car_id not in over_acc_id:
                    #如果没有则加入到列表中
                    over_acc_id.append(car_id)
                    #获取相同id下的所有车辆行驶信息
                    same_id_list = [obj for obj in data if obj[0]==car_id]
                    #做一个标注
                    mark_point = 0
                    for i in range(0,len(same_id_list)):
                        flag = False
                        #如果是在Markpoint后面，则直接跳过
                        if i < mark_point:
                            continue
                        #获取开始的时间
                        start_time = same_id_list[i][1]
                        start_velo = same_id_list[i][3]
                        #开始遍历每一种结果，如果2秒内的加速度超过预设值，那么就记录到表格中
                        for j in range(i+3 ,len(same_id_list)): 
                            end_time = same_id_list[j][1]
                            end_velo = same_id_list[j][3]
                            duration = (end_time-start_time)
                            acceleration = abs(start_velo-end_velo)/float(duration)
                            #如果没有超过直接到下一循环
                            if acceleration < 2.78:
                                break
                            #否则的话，则继续循环,直到最后结束
                            #同时每次都更新Markpoint和acceleration
                            mark_point = j
                            flag = True
                        if flag:
                            over_acc.append((car_id, car_type, start_time, end_time, start_velo, end_velo, obj[-1]))
                bar()
            # print(len(data)-count)
        sql1 = "INSERT INTO over_acce (id, type, start_time, end_time, start_velo, end_velo, road_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        cursor = conn.cursor()
        cursor.executemany(sql1,over_acc)

        print(over_acc[0])
        print(len(over_acc))
        with open('overacce.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['id', 'type', 'start_time', 'end_time', 'start_velo', 'end_velo', 'road_id'])
            writer.writerows(over_acc)
        conn.close()

    def insert_accetable(self):
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        tablename = 'GuoChuang.over_acce'
        with open('overacce.csv','r', newline='') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)
            for row in csv_reader:
                insert_sql = "INSERT INTO over_acce (id, type, start_time, end_time, start_velo, end_velo, road_id) VALUES (%s, %s, %s, %s, %s, %s, %s);"
                cursor.execute(insert_sql,tuple(row))
        # 提交更改并关闭连接
        conn.commit()
        conn.close()

    def back_up(self):
        """判断是否倒车,有精确和非精确两版本"""
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        # sql = """select id, type, orientation, heading, abs(orientation-heading) as included_angle, time_meas/1000000, on_road from road_data
        #   where type in (1,4,6) and is_moving = 1
        #   order by time_meas;"""
        # cursor.execute(sql)
        # data = cursor.fetchall()
        # cursor.close()
        # cursor1 = conn.cursor()
        # print("数据检索完成")
        # sql1 = "select distinct id from road_data where type in (1,4,6) and is_moving = 1"
        # cursor1.execute(sql1)
        # id_column = cursor1.fetchall()
        # id_list = [id[0] for id in id_column]
        # print(id_list)
        # pi_value = math.pi
        # back_up_list = []
        # with alive_bar(len(id_list)) as bar:
        #     for id in id_list:
        #         same_id_list = [obj for obj in data if obj[0] == id]
        #         flag = 1
        #         for obj in same_id_list:
        #             if obj[-3]>0.75*pi_value and obj[-3]<1.25*pi_value:
        #                 if flag:
        #                     start_time = obj[-2]
        #                     end_time = obj[-2]
        #                     flag  = 0
        #                 else:
        #                     end_time = obj[-2]
        #             else:
        #                 if not flag:
        #                     flag = 1
        #                     back_up_list.append((id, obj[1], start_time, end_time, obj[3], obj[-1]))
        #         bar()
        sql = """select id, type, orientation, heading, abs(orientation-heading) as included_angle, time_meas/1000000, on_road from road_data
           where type in (1,4,6) and is_moving = 1
           having included_angle>pi()/4*3 and included_angle<pi()/4*5
           order by time_meas;"""
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        cursor1 = conn.cursor()
        print("数据检索完成")
        sql1 = "select distinct id from road_data where type in (1,4,6) and is_moving = 1"
        cursor1.execute(sql1)
        id_column = cursor1.fetchall()
        id_list = [id[0] for id in id_column]
        back_up_list = []
        for id in id_list:
            same_id_list = [obj for obj in data if obj[0] == id]
            if len(same_id_list)!=0:
                start_time = same_id_list[0][-2]
                end_time = same_id_list[-1][-2]
                back_up_list.append((id, same_id_list[0][1], start_time, end_time, same_id_list[0][3], same_id_list[0][-1]))
        with open('back_up.csv','w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['id', 'type', 'start_time', 'end_time', 'heading','road_id'])
            writer.writerows(back_up_list)
        cursor1.close()
        conn.close()

    def insert_backup(self):
        """向数据库中插入数据"""
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        with open('back_up.csv','r', newline='') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)
            for row in csv_reader:
                insert_sql = "INSERT INTO back_up (id, type, start_time, end_time, heading, road_id) VALUES (%s, %s, %s, %s, %s, %s);"
                cursor.execute(insert_sql,tuple(row))
        # 提交更改并关闭连接
        conn.commit()
        conn.close()

    def retrograde(self):
        """
        判断车辆是否逆行
        首先需要了解所在道路的方向
        然后根据车辆的行驶方向，两者的夹角超过135度即可判断车辆是否逆行
        """
        high_value = HighValue()
        print("获取道路数据")
        lane_set, road_set, non_lane_set = high_value.lane_classify()
        road_set = high_value.judge_intersection(road_set)
        gdf = gpd.read_file("road2-12-9road\laneroad_with9road.geojson")
        road_heading_list = {}
        count = 1
        for fid in road_set:
            road_geo = gdf[gdf['fid']==fid[0]].iloc[0].geometry
            start_point = (road_geo.coords[0][0], road_geo.coords[0][1])
            end_point = (road_geo.coords[-1][0], road_geo.coords[-1][1])
            # 获取道路的方向向量
            road_heading = (end_point[0]-start_point[0], end_point[1]-start_point[1])
            road_cos = road_heading[0]/math.sqrt(road_heading[0]**2+road_heading[1]**2)
            road_angle = math.acos(road_cos)
            road_heading_list[count] = road_angle
            count+=1
        print(road_heading_list)
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "select id, type, time_meas/1000000, velocity, road_id, heading from road_data where type in (1,3,4,6) and is_moving = 1 order by time_meas"
        cursor.execute(sql)
        data = cursor.fetchall()
        nixing_list = []
        with alive_bar(len(data)) as bar:
            for obj in data:
                if obj[-2]!= 0:
                    road_id = obj[-2]
                    heading = obj[-1]
                    if abs(abs(float(heading))-road_heading_list[road_id])>math.pi*0.75:
                        nixing_list.append((road_id, obj[1], obj[2], heading, obj[-2]))
                bar()
        print(len(nixing_list))
        # 整合表格里的数据，把相同id的数据放在一起
        id_list = []
        final_list = []
        for obj in nixing_list:
            if obj[0] not in id_list:
                id_list.append(obj[0])
                zhongzhuan = [row for row in nixing_list if row[0] == obj[0]]
                start_time = zhongzhuan[0][2]
                end_time = zhongzhuan[-1][2]
                velocity = sum([row[3] for row in zhongzhuan])/len(zhongzhuan)
                final_list.append((obj[0], obj[1], start_time, end_time, velocity, float(obj[-2]), obj[-1]))
        print(final_list)
        print(len(final_list))
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "insert into retrograde(id, type, start_time, end_time, velocity, heading, road_id) values(%s, %s, %s, %s, %s, %s, %s);"
        cursor.executemany(sql, final_list)
        conn.commit()
        cursor.close()
        conn.close()

    def occupation_non_motrorized(self):
        """统计占用非机动车道的数据"""
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "select id, type, time_meas/1000000, velocity from road_data where type in (1,4,6,7,10) and motor_lane = 0"
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        id_list = []
        final_list = []
        from alive_progress import alive_bar
        with alive_bar(len(data)) as bar:
            for obj in data:
                id = obj[0]
                if id not in id_list:
                    id_list.append(id)
                    zhongzhuan = [row for row in data if row[0]==id]
                    start_time = zhongzhuan[0][2]
                    end_time = zhongzhuan[-1][2]
                    velocity = sum([row[-1] for row in zhongzhuan])
                    final_list.append((id, obj[1], start_time, end_time, velocity))
                bar()
        print(len(final_list))
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "insert into non_motorized(id, type, start_time, end_time, velocity) values(%s,%s,%s,%s,%s)"
        cursor.executemany(sql,final_list)
        conn.commit()
        cursor.close()
        conn.close()
    
    def occupy_bus_lane(self):
        """判断是否占用了公交车道"""
        high_value = HighValue()
        print("获取道路数据")
        lane_set, road_set, non_lane_set = high_value.lane_classify()
        lane_set = high_value.judge_intersection(lane_set)
        print("chedao")
        road_set = high_value.judge_intersection(road_set)
        print("chedao")
        non_lane_set = high_value.judge_intersection(non_lane_set)
        gdf = gpd.read_file("road2-12-9road\laneroad_with9road.geojson")
        lane_set = high_value.split(lane_set)
        bus_road = gdf[gdf['category']== 3].fid
        bus_road = bus_road.tolist()
        print(bus_road)
        #首先判断这些fid到底属于哪一条lane
        bus_dic = {}
        for fid in bus_road:
            flag = 0
            for i in range(1, len(lane_set)+1):
                if fid in lane_set[i-1]:
                    bus_dic[fid] = i
                    flag = 1
                    break
            if not flag:
                bus_dic[fid] = 0
        print(bus_dic)
        from shapely.geometry import Polygon
        # 先按照所有的公交车道绘制一个包围盒，用于判断车辆是否在公交车道上面
        lane_gdf = gpd.read_file('road2-12-9road/laneroad_with9road.geojson')
        boundary_gdf = gpd.read_file('road2-12-9road/boundaryroad_with9road.geojson')
        left_boundary = {}
        right_boundary = {}
        lane_area = {}
        #利用count来对车道标号，这样就可以确定是哪一条车道
        count = 1
        bus_road = [key for key, value in bus_dic.items() if value != 0]
        #确定好每一条道路的左右边界
        for lane in bus_road:
            left_linestrings = []
            right_linestrings = []
            
            geo = lane_gdf[lane_gdf['fid']==lane].iloc[0]
            #确定一条车道上的边界，每一段都加到列表里
            left_id = geo['left_boundary_id']
            right_id = geo['right_boundary_id']
                    
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
        #先从数据库中检索出对应道路的车辆，并判断是否出现占用公交车道的情况
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "select id, type, time_meas, velocity, on_road, position from road_data where type in (1,3,4,6,7) and on_road in (33,35,46,57,59)"
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        print(len(data))
        from shapely.geometry import Point
        # 遍历data中的所有数据， 判断是否在公交车道上
        result_list = []
        for obj in data:
            position = json.loads(obj[-1])
            position = (position['x'], position['y'])
            position_point = Point(position)
            for area in lane_area.values():
                if area.contains(position_point):
                    result_list.append((obj[0], obj[1], obj[2], obj[3], obj[4], 0))
                    break
        print(len(result_list))
        from shapely.geometry import LineString
        # 再继续判断路口中的车辆是否有占用公交车道的情况
        cross_buss_road = [key for key,value in bus_dic.items() if value==0]
        linestring_list = []
        # 把道路数据加载成为linestring
        for fid in cross_buss_road:
            lane = lane_gdf[lane_gdf['fid']==fid].iloc[0].geometry
            lane = [(x,y) for x,y,z in lane.coords]
            lane_line = LineString(lane)
            linestring_list.append(lane_line)
        #从数据库中找到on_road = 0的数据，利用他们的坐标和长宽处理成polygon对象
        #判断是否有交集就可以判断是否有占用公交车道
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "select id, type, time_meas, velocity, on_road, position, shape from road_data where type in (1,3,4,6,7) and on_road = 0"
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        print(len(data))
        # 一样的遍历data

        for obj in data:
            shape = json.loads(obj[-1])
            position = json.loads(obj[-2])
            top_left = (position['x'] - shape['x'] / 2, position['y'] - shape['y'] / 2)
            top_right = (position['x'] + shape['x'] / 2, position['y'] - shape['y'] / 2)
            bottom_left = (position['x'] - shape['x'] / 2, position['y'] + shape['y'] / 2)
            bottom_right = (position['x'] + shape['x'] / 2, position['y'] + shape['y'] / 2)
            car_polygon = Polygon([top_left,top_right,bottom_left, bottom_right])
            for line in linestring_list:
                if car_polygon.intersects(line):
                    result_list.append((obj[0], obj[1], obj[2], obj[3], obj[4], 1))
                    break
        #从结果中判断是什么时候开始
        sorted_result_list = sorted(result_list,key=lambda x:x[2])
        id_list = []
        final_list = []
        from alive_progress import alive_bar
        with alive_bar(len(sorted_result_list)) as bar:
            for obj in sorted_result_list:
                if obj[0] not in id_list:
                    id_list.append(obj[0])
                    zhongzhuan = [row for row in sorted_result_list if row[0] == obj[0]]
                    start_time = zhongzhuan[0][2]
                    end_time = zhongzhuan[-1][2]
                    velocity = sum([row[3] for row in zhongzhuan])/len(zhongzhuan)
                    final_list.append((obj[0],obj[1], start_time,end_time, velocity, obj[4], obj[5]))
                bar()
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "insert into occupy_bus_lane(id, type, start_time, end_time, velocity, on_road, on_cross) values(%s,%s,%s,%s,%s,%s,%s)"
        cursor.executemany(sql,final_list)
        conn.commit()
        cursor.close()
        conn.close()

    # def judge_parking(self):
    #     conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
    #     cursor = conn.cursor()
    #     sql = "select id, type, time_meas/1000000, on_road, is_moving from road_data where type in (1,4,6) order by time_meas"
    #     cursor.execute(sql)
    #     data = cursor.fetchall()
    #     cursor.close()
    #     conn.close()
    #     conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
    #     cursor = conn.cursor()
    #     sql = "select distinct id from road_data where type in (1,4,6) and is_moving = 0 ;"
    #     cursor.execute(sql)
    #     id_list = cursor.fetchall()
    #     cursor.close()
    #     conn.close()
    #     id_list = [id[0] for id in id_list]
    #     print(id_list[0:10])
    #     final_list = []
    #     data_list = list(data)

    #     with alive_bar(len(id_list)) as bar:
    #         for obj in id_list:
    #             zhongzhuan = [row for row in data_list if row[0]==obj]
    #             data_list = [row for row in data_list if row[0]!=obj]
    #             # 确定状态
    #             flag = 0
    #             for i in range(len(zhongzhuan)):
    #                 # 当前面没有停止的，现在开始停车
    #                 if zhongzhuan[i][-1] == 0 and flag == 0:
    #                     start_time = zhongzhuan[i][2]
    #                     flag = 1
    #                 elif zhongzhuan[i][-1] == 1 and flag == 1:
    #                     end_time = zhongzhuan[i][2]
    #                     flag = 0
    #                     duration = end_time-start_time
    #                     if float(duration)> 300:
    #                         final_list.append((obj,zhongzhuan[0][1], start_time,end_time,duration,zhongzhuan[0][-2]))
    #             bar()

    #     with open('parking.csv', mode='w', newline='') as file:
    #         writer = csv.writer(file)
    #         writer.writerow(('id', 'type', 'start_time', 'end_time', 'duration', 'on_road'))
    #         for row in final_list:
    #             writer.writerow(row)
    #     conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
    #     cursor = conn.cursor()
    #     sql = "insert into parking(id, type, start_time, end_time, duration, on_road) values(%s,%s,%s,%s,%s,%s)"
    #     cursor.executemany(sql,final_list)
    #     conn.commit()
    #     cursor.close()
    #     conn.close()
    
    def judge_xingren_abnormal(self):
        """判断是否存在行人的异常行为"""
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "select id, type, time_meas/1000000, on_road, heading, position from road_data where type in (2,3) and is_moving = 0 and motor_lane = 1 order by time_meas"
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "select distinct id from road_data where type in (2,3) and motor_lane = 1 ;"
        cursor.execute(sql)
        id_list = cursor.fetchall()
        cursor.close()
        conn.close()
        id_list = [id[0] for id in id_list]
        print(id_list[0:10])
        gdf = gpd.read_file("road2-12-9road\crosswalkroad_with9road.geojson")
        polygon_list = []

        # print(gdf)
        for geom in gdf.geometry:
            # print(geom)
            coordinates = list(geom.exterior.coords)
            # print(coordinates)
            coordinates_2d = [(coor[0], coor[1]) for coor in coordinates]
            polygon = Polygon(coordinates_2d)
            polygon_list.append(polygon)
        walking_area = MultiPolygon(polygon_list)
        data_list = []

        for obj in data:
            # print(obj)
            position = json.loads(obj[-1])
            position_point = Point((position['x'],position['y']))
            if walking_area.contains(position_point):
                data_list.append((obj[0],obj[1],obj[2],obj[3],obj[4],1))
            else:
                data_list.append((obj[0],obj[1],obj[2],obj[3],obj[4],0))
        final_list = []

        with alive_bar(len(id_list)) as bar:
            for obj in id_list:
                zhongzhuan = [row for row in data_list if row[0]==obj]
                data_list = [row for row in data_list if row[0]!=obj]
                # 确定状态
                flag = 0
                for i in range(len(zhongzhuan)):
                    # 当前面没有异常行为，现在突然有了
                    if zhongzhuan[i][-1] == 1 and flag == 0:
                        start_time = zhongzhuan[i][2]
                        flag = 1
                    elif zhongzhuan[i][-1] == 0 and flag == 1:
                        end_time = zhongzhuan[i][2]
                        flag = 0
                        final_list.append((obj,zhongzhuan[0][1], start_time,end_time,zhongzhuan[i][4],zhongzhuan[0][2]))
                bar()
        import csv
        with open('parking.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(('id', 'type', 'start_time', 'end_time', 'heading', 'on_road'))
            for row in final_list:
                writer.writerow(row)
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "insert into abnormalbehavior(id, type, start_time, end_time, heading, on_road) values(%s,%s,%s,%s,%s,%s)"
        cursor.executemany(sql,final_list)
        conn.commit()
        cursor.close()
        conn.close()
    
    def judge_parking(self):
        """判断是否存在长时间停车的情况"""
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "select id, type, time_meas/1000000, on_road, is_moving from road_data where type in (1,4,6) order by time_meas"
        cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "select distinct id from road_data where type in (1,4,6) and is_moving = 0 ;"
        cursor.execute(sql)
        id_list = cursor.fetchall()
        cursor.close()
        conn.close()
        print("查询完成")
        id_list = [id[0] for id in id_list]
        print(id_list[0:10])
        final_list = []
        data_list = list(data)
        from alive_progress import alive_bar
        with alive_bar(len(id_list)) as bar:
            for obj in id_list:
                zhongzhuan = [row for row in data_list if row[0]==obj]
                data_list = [row for row in data_list if row[0]!=obj]
                # 确定状态
                flag = 0
                for i in range(len(zhongzhuan)):
                    # 当前面没有停止的，现在开始停车
                    if zhongzhuan[i][-1] == 0 and flag == 0:
                        start_time = zhongzhuan[i][2]
                        flag = 1
                    elif zhongzhuan[i][-1] == 1 and flag == 1:
                        end_time = zhongzhuan[i][2]
                        flag = 0
                        duration = end_time-start_time
                        if float(duration)> 300:
                            final_list.append((obj,zhongzhuan[0][1], start_time,end_time,duration,zhongzhuan[0][-2]))
                bar()
        import csv
        with open('parking.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(('id', 'type', 'start_time', 'end_time', 'duration', 'on_road'))
            for row in final_list:
                writer.writerow(row)
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
        cursor = conn.cursor()
        sql = "insert into parking(id, type, start_time, end_time, duration, on_road) values(%s,%s,%s,%s,%s,%s)"
        cursor.executemany(sql,final_list)
        conn.commit()
        cursor.close()
        conn.close()


if __name__ == "__main__":
    reality_db = RoadToDB()
    # # reality_db.judge_accelerate()
    # #reality_db.back_up()
    # # reality_db.insert_backup()
    # reality_db.retrograde()
    # reality_db.judge_parking()
    # reality_db.judge_xingren_abnormal()

