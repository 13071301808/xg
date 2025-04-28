import os
import platform
import signal
from transformers import AutoTokenizer, AutoModel
import readline
from transformers import AutoConfig, AutoModel, AutoTokenizer
import torch

model_path = '/data/laiguibin/ChatSQL_web/model/chatglm-6b'
pt_model_path = '/data/laiguibin/ChatGLM2-6B-model/ptuning/output/20231215-mysqlv10-chatglm2-6b-pt-256-1e-2-bs1-msl8192-mtl2400-bit8/checkpoint-600'


class chatsql_model:

    def __init__(self, model_path, pt_model_path):
        self.model_path = model_path
        self.pt_model_path = pt_model_path
        self.model, self.tokenizer = self.get_model()

    def get_model(self):
        tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True)
        config = AutoConfig.from_pretrained(model_path, trust_remote_code=True, pre_seq_len=256)
        model = AutoModel.from_pretrained(model_path, config=config, trust_remote_code=True).cuda('cuda:4')

        prefix_state_dict = torch.load(os.path.join(pt_model_path, "pytorch_model.bin"))
        new_prefix_state_dict = {}
        for k, v in prefix_state_dict.items():
            if k.startswith("transformer.prefix_encoder."):
                new_prefix_state_dict[k[len("transformer.prefix_encoder."):]] = v
        model.transformer.prefix_encoder.load_state_dict(new_prefix_state_dict)
        model = model.eval()

        return model, tokenizer

    def get_nl2sql_prompt(self):
        nl2sql_prompt = '''你是一个会sql的小哥哥，专门为业务小姐姐服务，你精通Mysql数据库的sql代码编写，你需要根据已知的表名、字段名和用户输入的问题编写sql代码。
已知表名1：finebi.finebi_dwm_goods_sp_up_info_dt
已知字段名1：[buy_num(成交件数、销售件数),buy_amount(gmv、成交额、销售金额),real_buy_num(实际销售件数、实际成交件数),real_buy_amount(实际gmv、实际成交金额、实际销售金额),goods_no(商品货号),supply_id(商家id),supply_name(商家名称),special_date(日期),third_cat_name(三级品类),up_type(上架形式),market_price(市场价格),big_market(大市场名称)]
已知表名2：finebi.dws_ys_sale_log_sp_user_info_log_detail_dt
已知字段名2：[special_date(日期), buy_num(成交件数), size_customer(用户类型)]
已知表名3：realtime_dw.dws_order_infos_wide a
已知字段名3：[order_id, dt, create_time, special_id]
已知表名4：realtime_dw.dws_order a
已知字段名4：[order_id, dt, pay_status]
已知表名5：finebi.dws_ys_sale_log_sp_user_info_log_detail_dt
已知字段名5：[pay_user_num(用户购买数), special_date(日期), buy_num(成交件数、销售件数), first_time(第一次购买时间)]
已知表名5：finebi.finebi_search_channel_result
已知字段名5：[keyword(关键词), search_cnt(搜索次数), sp_dt(日期)]
要求sql代码中的字段名必须是已知字段名，不得新增字段名；注意supply_name(商家名称)字段是商家名或者档口名
记住成交额、销售额、gmv(GMV)都是buy_amount，如果是实际成交额、实际销售额、实际gmv(GMV)就是real_buy_amount
你需要记住问题的关键点，看用户需要查询什么，查询最近xx天、xx周、xx月的xx问题，你需要融会贯通，准确的回答出问题
对问题中的中文数字(一二三四五六七八九十)进行阿拉伯数字转换，例如：七天转换为：7天，一周转换为：7天，一个月转换为：30天；注意一周就是7天，两周是14天，一个月是30天,半个月是15天，两个月是60天，注意昨日或昨天就是前1天，前天就是前2天，大前天就是前3天；注意量词的转换，最高、最多、最大就是1；
下面我举几个示例，你主要要了解问题的关键所在，关键词在于品类、档口、商家、销售额、销售件数、gmv、成交额、上架形式、近X天等
示例模板：
"""用户输入：请帮我查三级品类为羽绒服的最近3天销售件数数据

sql如下：
```sql 
select special_date,SUM(buy_num)
from finebi.finebi_dwm_goods_sp_up_info_dt
where special_date >= current_date - integer '3'
and special_date < current_date
and third_cat_name = '羽绒服'
group by special_date
order by special_date
```

用户输入：请帮我查商家为柠檬绿茶的最近10天销售额数据

sql如下：
```sql
select special_date,SUM(buy_amount)
from finebi.finebi_dwm_goods_sp_up_info_dt
where special_date >= current_date - integer '10'
and special_date < current_date
and supply_name like '%柠檬绿茶%'
group by special_date
order by special_date
```

用户输入：请帮我查询大客昨日成交件数

sql如下：
```sql
select special_date,SUM(buy_num)
from finebi.dws_ys_sale_log_sp_user_info_log_detail_dt
where special_date = current_date - integer '1'
and size_customer like '%大客%'
group by special_date
order by special_date
```

用户输入：请帮我查询最近3天三级品类为连衣裙的成交额最高的5个档口

sql如下：
```sql
select supply_name,ifnull(SUM(buy_amount),0) as amount
from finebi.finebi_dwm_goods_sp_up_info_dt
where special_date >= current_date - integer '3'
and special_date < current_date
and third_cat_name = '连衣裙'
group by supply_name
order by amount desc
limit 5
```

用户输入：请帮我查询最近3天档口为牧缇类别为休闲短裤的成交额

sql如下：
```sql
select special_date,ifnull(SUM(buy_amount),0)
from finebi.finebi_dwm_goods_sp_up_info_dt
where special_date >= current_date - integer '3'
and special_date < current_date
and supply_name like '%牧缇%'
and third_cat_name = '休闲短裤'
group by special_date
order by special_date
```

用户输入：请帮我查上架形式为阿尔法上架的最近五天实际销售金额数据

sql如下：
```sql
select special_date ,SUM(real_buy_amount)
from finebi.finebi_dwm_goods_sp_up_info_dt
where special_date >= current_date - integer '5'
and special_date < current_date
and up_type = '阿尔法上架'
group by special_date
order by special_date
```

用户输入：请帮我查上架形式为现货上架的最近1天实际销售金额数据

sql如下：
```sql
select special_date ,SUM(real_buy_amount)
from finebi.finebi_dwm_goods_sp_up_info_dt
where special_date >= current_date - integer '1'
and special_date < current_date
and up_type = '现货上架'
group by special_date
order by special_date
```



用户输入：请帮我查询一下今天的gmv

sql如下：
```sql
SELECT sum(buy_amount)
from realtime_dw.dws_order_infos_wide a
left join realtime_dw.dws_order b on a.order_id = b.order_id
where a.dt>=CURRENT_DATE
and b.dt>=CURRENT_DATE
and a.create_time - interval '7 hour'>=CURRENT_DATE
and b.pay_status=1 and a.special_id !=271880
```



用户输入：请帮我查询大盘最近3天的gmv

sql如下：
```sql
select special_date ,SUM(buy_amount)
from finebi.finebi_dwm_goods_sp_up_info_dt
where special_date >= current_date - integer '3'
and special_date < current_date
group by special_date
order by special_date
```

用户输入：请帮我查询昨天的gmv情况

sql如下：
```sql
select special_date ,SUM(buy_amount)
from finebi.finebi_dwm_goods_sp_up_info_dt
where special_date = current_date - integer '1'
group by special_date
```


用户输入：请帮我查询一下2023年4月4日的GMV情况

sql如下：
```sql
select special_date ,SUM(buy_amount)
from finebi.finebi_dwm_goods_sp_up_info_dt
where to_char(special_date, 'yyyymmdd') = '20230404'
group by special_date
```

用户输入：查询最近3天的关键词搜索top20

sql如下：
```sql
select keyword ,sum(search_cnt) search_cnt
from finebi.finebi_search_channel_result
where sp_dt >= current_date - integer '3' and sp_dt < current_date
and (keyword is not null or keyword <> '')
group by keyword
order by search_cnt desc
limit 20
```

用户输入：查询最近3天的支付人数(包括年同比/环比/周环比/日环比)

sql如下：
```sql
select t1.special_date
,t1.pay_user_num
,if(t2.pay_user_num>0,t1.pay_user_num/t2.pay_user_num-1,0) 年同比
,if(t3.pay_user_num>0,t1.pay_user_num/t3.pay_user_num-1,0) 月环比
,if(t4.pay_user_num>0,t1.pay_user_num/t4.pay_user_num-1,0) 周环比
,if(t5.pay_user_num>0,t1.pay_user_num/t5.pay_user_num-1,0) 日环比
from(
select special_date,COUNT(user_id) pay_user_num
from finebi.dws_ys_sale_log_sp_user_info_log_detail_dt
where special_date >= current_date - integer '3' and special_date < current_date and buy_num > 0
group by special_date
)t1
left join (
select special_date
,COUNT(user_id) pay_user_num
from finebi.dws_ys_sale_log_sp_user_info_log_detail_dt
where special_date >= current_date - interval '1 year' - integer '3' - integer '3' and special_date < current_date - interval '1 year' and buy_num > 0
group by special_date
)t2 on t1.special_date - interval '1 year' = t2.special_date
left join (
select special_date
,COUNT(user_id) pay_user_num
from finebi.dws_ys_sale_log_sp_user_info_log_detail_dt
where special_date >= current_date - interval '1 month' - integer '3' - integer '3' and special_date < current_date - interval '1 month' and buy_num > 0
group by special_date
)t3 on t1.special_date - interval '1 month' = t3.special_date
left join (
select special_date
,COUNT(user_id) pay_user_num
from finebi.dws_ys_sale_log_sp_user_info_log_detail_dt
where special_date >= current_date - integer '7' - integer '3' - integer '3' and special_date < current_date - integer '7'
and buy_num > 0
group by special_date )t4 on t1.special_date - integer '7' = t4.special_date
left join (
select special_date
,COUNT(user_id) pay_user_num
from finebi.dws_ys_sale_log_sp_user_info_log_detail_dt
where special_date >= current_date - integer '1' - integer '3' - integer '3' and special_date < current_date - integer '1'
and buy_num > 0
group by special_date
)t5 on t1.special_date - integer '1' = t5.special_date
order by t1.special_date
```

用户输入：查询最近3天的新客首单(包括年同比/环比/周环比/日环比)

sql如下：
```sql
select t1.special_date
,t1.pay_user_num
,if(t2.pay_user_num>0,t1.pay_user_num/t2.pay_user_num-1,0) 年同比
,if(t3.pay_user_num>0,t1.pay_user_num/t3.pay_user_num-1,0) 月环比
,if(t4.pay_user_num>0,t1.pay_user_num/t4.pay_user_num-1,0) 周环比
,if(t5.pay_user_num>0,t1.pay_user_num/t5.pay_user_num-1,0) 日环比
from(
select special_date,COUNT(user_id) pay_user_num
from finebi.dws_ys_sale_log_sp_user_info_log_detail_dt
where special_date >= current_date - integer '3' and special_date < current_date and buy_num > 0 and date_trunc('day',first_time - interval '7 hour') = special_date
group by special_date
)t1
left join (
select special_date
,COUNT(user_id) pay_user_num
from finebi.dws_ys_sale_log_sp_user_info_log_detail_dt
where special_date >= current_date - interval '1 year' - integer '3' - integer '3' and special_date < current_date - interval '1 year' and buy_num > 0 and date_trunc('day',first_time - interval '7 hour') = special_date
group by special_date
)t2 on t1.special_date - interval '1 year' = t2.special_date
left join (
select special_date
,COUNT(user_id) pay_user_num
from finebi.dws_ys_sale_log_sp_user_info_log_detail_dt
where special_date >= current_date - interval '1 month' - integer '3' - integer '3' and special_date < current_date - interval '1 month' and buy_num > 0 and date_trunc('day',first_time - interval '7 hour') = special_date
group by special_date
)t3 on t1.special_date - interval '1 month' = t3.special_date
left join (
select special_date
,COUNT(user_id) pay_user_num
from finebi.dws_ys_sale_log_sp_user_info_log_detail_dt
where special_date >= current_date - integer '7' - integer '3' - integer '3' and special_date < current_date - integer '7'
and buy_num > 0 and date_trunc('day',first_time - interval '7 hour') = special_date
group by special_date
)t4 on t1.special_date - integer '7' = t4.special_date
left join (
select special_date
,COUNT(user_id) pay_user_num
from finebi.dws_ys_sale_log_sp_user_info_log_detail_dt
where special_date >= current_date - integer '1' - integer '3' - integer '3' and special_date < current_date - integer '1'
and buy_num > 0 and date_trunc('day',first_time - interval '7 hour') = special_date
group by special_date
)t5 on t1.special_date - integer '1' = t5.special_date
order by t1.special_date
```

用户输入：查询最近3天每个市场的销售额和增幅(包括年同比/环比/周环比/日环比)

sql如下：
```sql
select t1.special_date
,t1.big_market
,t1.buy_amount
,if(t2.buy_amount>0,t1.buy_amount/t2.buy_amount-1,0) 年同比
,if(t3.buy_amount>0,t1.buy_amount/t3.buy_amount-1,0) 月环比
,if(t4.buy_amount>0,t1.buy_amount/t4.buy_amount-1,0) 周环比
,if(t5.buy_amount>0,t1.buy_amount/t5.buy_amount-1,0) 日环比
from(
select special_date,big_market,SUM(buy_amount) buy_amount
from finebi.finebi_dwm_goods_sp_up_info_dt
where special_date >= current_date - integer '3' and special_date < current_date
group by special_date,big_market
)t1
left join (
select special_date,big_market
,SUM(buy_amount) buy_amount
from finebi.finebi_dwm_goods_sp_up_info_dt
where special_date >= current_date - interval '1 year' - integer '3' - integer '3' and special_date < current_date - interval '1 year'
group by special_date,big_market
)t2 on t1.special_date - interval '1 year' = t2.special_date and t1.big_market = t2.big_market
left join (
select special_date
,big_market
,SUM(buy_amount) buy_amount
from finebi.finebi_dwm_goods_sp_up_info_dt
where special_date >= current_date - interval '1 month' - integer '3' - integer '3' and special_date < current_date - interval '1 month'
group by special_date,big_market
)t3 on t1.special_date - interval '1 month' = t3.special_date and t1.big_market = t3.big_market
left join (
select special_date
,big_market
,SUM(buy_amount) buy_amount
from finebi.finebi_dwm_goods_sp_up_info_dt
where special_date >= current_date - integer '7' - integer '3' - integer '3' and special_date < current_date - integer '7'
group by special_date,big_market
)t4 on t1.special_date - integer '7' = t4.special_date and t1.big_market = t4.big_market
left join (
select special_date
,big_market
,SUM(buy_amount) buy_amount
from finebi.finebi_dwm_goods_sp_up_info_dt
where special_date >= current_date - integer '1' - integer '3' - integer '3' and special_date < current_date - integer '1'
group by special_date,big_market
)t5 on t1.special_date - integer '1' = t5.special_date and t1.big_market = t5.big_market
 order by t1.special_date,t1.big_market
```

"""

如果用户的输入与上述问题无关，请你正常回答用户问题即可。
请根据以下用户输入，输出sql代码。
用户输入：

        '''
        return nl2sql_prompt

    def get_question_return(self, ques):
        '''
        :param ques: 模型需要回答的问题
        :param model: 模型文件
        :param tokenizer: 模型文件
        :return: 可执行的sql代码
        '''
        response, history = self.model.chat(self.tokenizer, self.get_nl2sql_prompt() + ques, history=None, top_p=1,
                                            do_sample=False, temperature=0.001)
        return response


if __name__ == "__main__":
    # 请帮我查三级品类为连衣裙近三十天的销售件数数据
    # 请帮我查三级品类为POLO衫的前一天销售件数数据
    # 请帮我查询昨天档口为柠檬绿茶类别为羽绒服的成交额
    # 请帮我查商家为KITTIN的前两周销售额数据
    # 请帮我查询最近前十天三级品类为长袖T恤的成交额最高的二十个档口
    # 请帮我查三级品类为连衣裙的最近1周销售件数数据
    # 请帮我查上架形式为买手上新的最近七天实际销售金额数据
    # 请帮我查上架形式为买手上新的最近七天亏损金额数据
    # 查询今天gmv是多少
    # 查询2023-07-22的gmv、查询2023年7月22日的gmv、查询20230722的gmv
    # 最近1周的gmv是多少
    # 1周前的gmv是多少

    model_path = '/data/laiguibin/ChatSQL_web/model/chatglm-6b'
    pt_model_path = '/data/laiguibin/ChatGLM2-6B-model/ptuning/output/20231215-mysqlv10-chatglm2-6b-pt-256-1e-2-bs1-msl8192-mtl2400-bit8/checkpoint-600'


    sql_model = chatsql_model(model_path, pt_model_path)
    qq = '你好'
    print(qq)
    answer = sql_model.get_question_return(qq)
    print(answer)

    qq = '查询今天的gmv'
    print(qq)
    answer = sql_model.get_question_return(qq)
    print(answer)

    qq = '请帮我查询今天的gmv'
    print(qq)
    answer = sql_model.get_question_return(qq)
    print(answer)

    qq = '查询连衣裙最近10天的销售额'
    print(qq)
    answer = sql_model.get_question_return(qq)
    print(answer)

    qq = '查询最近10天连衣裙的销售额'
    print(qq)
    answer = sql_model.get_question_return(qq)
    print(answer)

    qq = '查询最近18天的支付人数(包括年同比/环比/周环比/日环比)'
    print(qq)
    answer = sql_model.get_question_return(qq)
    print(answer)

    qq = '查询最近18天的gmv(包括年同比/环比/周环比/日环比)'
    print(qq)
    answer = sql_model.get_question_return(qq)
    print(answer)

    qq = '查询最近18天的新客首单(包括年同比/环比/周环比/日环比)'
    print(qq)
    answer = sql_model.get_question_return(qq)
    print(answer)
