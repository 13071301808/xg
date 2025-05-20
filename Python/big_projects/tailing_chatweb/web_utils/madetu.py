from pyecharts.charts import Bar, Line
from pyecharts import options as opts


class Madetu:
    # 柱状图
    @staticmethod
    def made_tu_bar(data_pd) -> Bar:
        bar = Bar()
        data_cns = data_pd.columns
        print('获取的列数有：', len(data_cns))
        bar.add_xaxis(data_pd[data_cns[0]].tolist())
        # 如果column_names中的数量超过2个维度
        if len(data_cns) > 2:
            for i in range(1, len(data_cns)):
                # 添加多维度的y轴
                bar.add_yaxis(data_cns[i], data_pd[data_cns[i]].tolist())
                bar.set_series_opts(label_opts=opts.LabelOpts(is_show=False))
        else:
            bar.add_yaxis(data_cns[1], data_pd[data_cns[1]].tolist())
        # 柱状图配置
        bar.set_global_opts(
            xaxis_opts=opts.AxisOpts(
                name=data_cns[0],
                axislabel_opts=opts.LabelOpts(rotate=-45)
            )
        )
        print('图片正在创建中...')
        return bar

    # 折线图
    @staticmethod
    def made_tu_line(data_pd) -> Line:
        line = Line()
        data_cns = data_pd.columns
        print('获取的列数有：', len(data_cns))
        line.add_xaxis(data_pd[data_cns[0]].tolist())
        # 如果column_names中的数量超过2个维度,制作双y轴
        if len(data_cns) > 2:
            for i in range(1, len(data_cns)):
                line.add_yaxis(data_cns[i], data_pd[data_cns[i]].tolist())
                line.set_series_opts(label_opts=opts.LabelOpts(is_show=False))
            line.set_global_opts(
                yaxis_opts=opts.AxisOpts(
                    # 设置为对数刻度
                    type_="log"  # 设置为对数刻度
                )
            )
        else:
            line.add_yaxis(data_cns[1], data_pd[data_cns[1]].tolist())
        # 折线图配置
        line.set_global_opts(
            xaxis_opts=opts.AxisOpts(
                name=data_cns[0],
                axislabel_opts=opts.LabelOpts(rotate=-45)
            )
        )
        print('图片正在创建中...')
        return line
