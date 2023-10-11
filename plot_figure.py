""""本文件主要用于绘制图表，包括了流量总览、各时段的流量预测、交通场景雷达图"""
import pymysql 
import pandas as pd
from prophet import Prophet
import pyecharts.options as opts
from pyecharts.charts import Radar, Timeline, Line, Bar


def total_flow_statistics(data_list:list):
    """统计每一条道路在不同时段中的流量，最后输出结果是一个dataframe，列代表车道编号从1到153，
    行代表时间从7点到16点，每间隔15分钟"""
    # with jsonlines.open('final1_part_00000.json','r') as reader:
    time_stage = {}
    road_count = {}
    road_fid = {}
    for i in range(1,153):
        #初始化每一条道路的车流量
        road_count[i] = 0
    for i in range(28,68):
        # 每个小时为一行
        time_stage[i] = {}
        for j in range(1,153):
            # 每一条道路为一列
            time_stage[i][j] = 0
    for car_obj in data_list:
        on_road = car_obj[-2]
        if on_road<=152 and on_road!=0:
            fid = car_obj[0]
            clock = car_obj[2]
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
    # print(df)
    df.to_csv('lane_traffic_volume.csv', index=False)
    return df

def get_traffic_volume():
    """统计车流量，然后将数据存储到对应的csv文件中"""
    conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
    #统计超速行驶
    cursor = conn.cursor()
    sql = "select id, type, convert((time_meas/1000-1681315200000)/900000,signed) as time, on_road, road_sec from road_data where type in (1,4,5,6,10) and is_moving=1 order by time_meas;"
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    # 这个用于记录独立的id
    conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
    cursor = conn.cursor()
    sql = "select distinct id from road_data where type in (1,4,5,6,10) and is_moving = 1 ;"
    cursor.execute(sql)
    id_list = cursor.fetchall()
    cursor.close()
    cursor = conn.cursor()
    sql = "select distinct road_sec from road_data where type in (1,4,5,6,10) and is_moving = 1"
    cursor.execute(sql)
    road_sec_list = cursor.fetchall()
    cursor.close()
    conn.close()
    id_list = [id[0] for id in id_list]
    road_sec_list = [sec[0] for sec in road_sec_list]
    print(id_list[0:10])
    data_list = list(data)
    total_flow_statistics(data_list)

def get_taishi():
    """获取各个时段内的交通情况"""
    conn = pymysql.connect(host='localhost', port=3306, user='root', password='407491498QzJ.', db='GuoChuang')
    #统计超速行驶
    cursor = conn.cursor()
    sql = "select convert((end_time/1000-1681315200000)/300000,signed) as time, count(*)  from overspeed group by time order by time;"
    cursor.execute(sql)
    data_speed = cursor.fetchall()
    cursor.close()
    #统计急加速和急减速
    cursor = conn.cursor()
    sql = "select convert((end_time*1000-1681315200000)/300000,signed) as time, count(*)  from over_acce group by time order by time;"
    cursor.execute(sql)
    data_acce = cursor.fetchall()
    cursor.close()
    #统计车辆的切入切出
    cursor = conn.cursor()
    sql = "select convert((time*1000-1681315200000)/300000,signed) as atime, count(*)  from change_road group by atime order by atime;"
    cursor.execute(sql)
    data_change = cursor.fetchall()
    cursor.close()
    #统计车辆逆行
    cursor = conn.cursor()
    sql = "select convert((end_time*1000-1681315200000)/300000,signed) as atime, count(*)  from retrograde group by atime order by atime;"
    cursor.execute(sql)
    data_retrograde = cursor.fetchall()
    cursor.close()
    #统计倒车
    cursor = conn.cursor()
    sql = "select convert((end_time*1000-1681315200000)/300000,signed) as atime, count(*)  from back_up group by atime order by atime;"
    cursor.execute(sql)
    data_back = cursor.fetchall()
    cursor.close()
    #统计非机动车和行人的异常行为
    cursor = conn.cursor()
    sql = "select convert((end_time*1000-1681315200000)/300000,signed) as atime, count(*)  from abnormalbehavior group by atime order by atime;"
    cursor.execute(sql)
    data_abnormal = cursor.fetchall()
    cursor.close()
    #统计长时间停车的情况
    cursor = conn.cursor()
    sql = "select convert((end_time*1000-1681315200000)/300000,signed) as atime, count(*)  from parking group by atime order by atime;"
    cursor.execute(sql)
    data_parking = cursor.fetchall()
    cursor.close()
    #统计占用非机动车道的时间
    cursor = conn.cursor()
    sql = "select convert((end_time*1000-1681315200000)/300000,signed) as atime, count(*)  from non_motorized group by atime order by atime;"
    cursor.execute(sql)
    data_motor = cursor.fetchall()
    cursor.close()
    #统计占用公交车道的情况
    cursor = conn.cursor()
    sql = "select convert((end_time/1000-1681315200000)/300000,signed) as atime, count(*)  from occupy_bus_lane group by atime order by atime;"
    cursor.execute(sql)
    data_bus = cursor.fetchall()
    cursor.close()
    conn.close()

    data_back = [obj[1] for obj in data_back]
    data_abnormal = [obj[1] for obj in data_abnormal]
    data_acce = [obj[1] for obj in data_acce]
    data_bus = [obj[1] for obj in data_bus]
    data_change = [obj[1] for obj in data_change]
    data_motor = [obj[1] for obj in data_motor]
    data_parking = [obj[1] for obj in data_parking]
    data_parking.insert(0,0)#在最初始的时刻是没有停车的所以补加了一个
    data_retrograde = [obj[1] for obj in data_retrograde]
    data_speed = [obj[1] for obj in data_speed]
    data = {"倒车":data_back, "非机动车和行人异常":data_abnormal, "急加速或急减速":data_acce, "占用公交车道":data_bus, "车辆变道":data_change,"占用非机动车道":data_motor,
    "长时间停车":data_parking, "车辆逆行":data_retrograde, "车辆超速":data_speed}
    data_df = pd.DataFrame(data)
    data_df.to_csv("changjing.csv",index=False)
    return data_df

def create_radar(data_df, time):
    value = [list(data_df.iloc[time])]
    radar = (
        Radar(init_opts=opts.InitOpts(bg_color='#6c8c94'))
        .add_schema(
            schema=[
                opts.RadarIndicatorItem(name="倒车", max_=55),
                opts.RadarIndicatorItem(name="非机动车和行人异常", max_=50),
                opts.RadarIndicatorItem(name="急加速或急减速", max_=155),
                opts.RadarIndicatorItem(name="占用公交车道", max_=290),
                opts.RadarIndicatorItem(name="车辆变道", max_=5000),
                opts.RadarIndicatorItem(name="占用非机动车道", max_=175),
                opts.RadarIndicatorItem(name="长时间停车", max_=20),
                opts.RadarIndicatorItem(name="车辆逆行", max_=20),
                opts.RadarIndicatorItem(name="车辆超速", max_=70),
            ],
            splitarea_opt=opts.SplitAreaOpts(
                is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
            ),
            textstyle_opts=opts.TextStyleOpts(color="#e8f3ee",font_family='Arial', font_size=15, font_weight='bold'),
        )
        .add(
            series_name="五分钟内发生次数",
            data=value,
            linestyle_opts=opts.LineStyleOpts(color="#dd6b66",width=2),
            color="#aa5b54",
        )
        .set_series_opts(label_opts=opts.LabelOpts(is_show=True,color="#dd6b66"))
        .set_global_opts(
            title_opts=opts.TitleOpts(title="态势雷达图",title_textstyle_opts=opts.TextStyleOpts(color='#333333')),
            legend_opts=opts.LegendOpts(inactive_color='#6c8c94',textstyle_opts=opts.TextStyleOpts(color='#eebaab',font_size=14, font_weight='bold'), border_color="#6c8c94")
        )
        # .render("basic_radar_chart{}.html".format(time))
    )
    return radar



def create_line(dframe,i):
    y = dframe['{}'.format(i)]

    data = {
        'ds': pd.date_range(start='2023-04-13 07:00:00', periods=37,freq='15T'),
        'y' : y
    }
    df = pd.DataFrame(data)
    # print(df)
    df['ds'] = pd.to_datetime(df['ds'])
    # 创建Prophet模型
    model = Prophet()
    model = Prophet(seasonality_mode='additive')  # 将趋势设置为非线性
    model.add_seasonality(name='daily', period=1, fourier_order=5)

    print("开始拟合数据")
    # 拟合数据
    model.fit(df)
    future = model.make_future_dataframe(periods=0, freq='H')
    forecast = model.predict(future)
    df = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

    print("预测结果绘图")
    df['ds'] = df['ds'].dt.strftime('%m-%d %H:%M')

    x= df['ds'].tolist()
    real = y
    y = df['yhat'].tolist()
    yl = df['yhat_lower'].tolist()
    yu = df['yhat_upper'].tolist()
    # print(x)
    # print(len(y))
    # print(real)

    # 使用Pyecharts绘制Line图
    line = (
        Line(init_opts=opts.InitOpts(theme='ThemeType.DARK'))
        .add_xaxis(x)
        .add_yaxis("预测流量", y, is_smooth = True)
        .add_yaxis("实际流量", real, is_smooth = True, 
                linestyle_opts=opts.LineStyleOpts(width=3),
                symbol='triangle',)
        .add_yaxis("预测流量下界", yl, is_smooth = True,areastyle_opts=opts.AreaStyleOpts(color = "blue", opacity=0.3))
        .add_yaxis("预测流量上界", yu, is_smooth = True,areastyle_opts=opts.AreaStyleOpts(opacity=0.5))
        .set_series_opts(
                        label_opts=opts.LabelOpts(is_show=False),
        )
        .set_global_opts(title_opts=opts.TitleOpts(title="Prophet流量预测"),
                        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=45)),
                        )
    )
    return line

def create_bar(df:pd.DataFrame, i):
    bar = (
    Bar()
    .add_xaxis( pd.date_range(start='2023-04-13 07:00:00', periods=37,freq='15T'))
    .add_yaxis("道路流量", list(df.iloc[i]))
    .set_global_opts(
        title_opts=opts.TitleOpts(title="交通流量总览"),
        datazoom_opts=[opts.DataZoomOpts(), opts.DataZoomOpts(type_="inside")],
        toolbox_opts=opts.ToolboxOpts(),
    )
    .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
)
    return bar

def traffic_radar():
    """交通事件"""
    data = pd.read_csv('changjing.csv')
    data_df = data.iloc[:,1:]
    # 生成时间轴的图
    # 定义自定义的 JavaScript 代码，用于处理点击事件
    timeline = Timeline()
    
    for y in range(0,109):
        timeline.add(create_radar(data_df, y), time_point=y)
    timeline.add_schema(is_auto_play=True, play_interval=3000)
    timeline.render("finance_indices_2002.html")

def flow_total_forecast_bar():
    """交通流量总览图"""
    data_df = pd.read_csv('lane_traffic_volume.csv')
    # 生成时间轴的图
    # 定义自定义的 JavaScript 代码，用于处理点击事件
    timeline = Timeline(init_opts=opts.InitOpts(width="800px", height="600px", page_title="Timeline Example"))
    
    for y in range(0,37):
        timeline.add(create_bar(data_df, y), time_point=y)
    timeline.add_schema(is_auto_play=True, play_interval=3000,orient='vertical',height=500,pos_left=20)
    timeline.render("chartweb/bar.html")


    # 渲染图表
    # line.render("chartweb/volumes/prophet_forecast_line{}.html".format(i))
def flow_total_forecast_line():
    """交通流量总览图"""
    data_df = pd.read_csv('lane_traffic_volume.csv')
    # 生成时间轴的图
    # 定义自定义的 JavaScript 代码，用于处理点击事件
    timeline = Timeline()
    
    for y in range(1,153):
        timeline.add(create_line(data_df, y), time_point=y)
    timeline.add_schema(is_auto_play=True, play_interval=3000)
    timeline.render("chartweb/line.html")

def flow_forecast_heatmap():
    """生成当前道路的流量热力图"""
    df = pd.read_csv('lane_traffic_volume.csv')
    heatmap_data = []
    time = [i for i in range(1,38)]
    data_column = [i for i in range(1,153)]
    #准备数据
    for columnname in df:
        flow_data = [[columnname, x, value] for x, value in enumerate(df[columnname])]
        heatmap_data.extend(flow_data)
    #绘制图像
    from pyecharts.charts import HeatMap
    heatmap = (
    HeatMap(init_opts = opts.InitOpts(width="80%",height="800px"))
    .add_xaxis(data_column)
    .add_yaxis(
        series_name="", 
        yaxis_data=time,
        value=heatmap_data,
        label_opts=opts.LabelOpts(is_show=False, position="inside"),
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="交通流量热力图"),
        visualmap_opts=opts.VisualMapOpts(min_=0, max_=200),  # 根据数据范围调整
    )
)

    # 渲染热力图到 HTML 文件中
    heatmap.render("chartweb/flow_heatmap.html")

if __name__ == "__main__":
    traffic_radar()
    flow_forecast_heatmap()
    flow_total_forecast_bar()
    flow_total_forecast_line()
