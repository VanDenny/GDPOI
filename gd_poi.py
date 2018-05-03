from multiprocessing.dummy import Pool as ThreadPool
from Clawer_Base.logger import logger
from Clawer_Base.geo_lab import Rectangle, Sample_Generator
from Clawer_Base.email_alerts import Email_alarm
from Clawer_Base.key_changer import Key_Changer
from Clawer_Base.ioput import Res_saver, Type_Input
from Clawer_Base.res_extractor import Res_Extractor
from Clawer_Base.clawer_frame import Clawer
from Clawer_Base.geo_visualization import Geo_Visual
from Clawer_Base.shape_io import Shapefile_Write
import prettytable
import datetime
import math
import sys
sys.setrecursionlimit(1000000)

def clawer_init(rect, category, page = 1):
    key_dict = Key_Changer(u'高德').key_dict
    params = Gdpoi_params(rect, category, key_dict, page)
    a_clawer = Gdpoi_clawer(params)
    return a_clawer

class Gdpoi_params(dict):
    """高德POI参数,其中polygon左上右下"""
    params = {
        'key':"70de561d24ed370ab68d0434d834d106",
        'polygon': '',
        'types': '',
        'page': 1
    }
    def __init__(self, rect, types, key, page=1):
        self.rect = rect
        self.update(Gdpoi_params.params)
        self.update_key(key)
        self.update_polygon(self.rect_to_dict(rect))
        self.update_types(types)
        self.update_page(page)


    def rect_to_dict(self, rect):
        a_dict = {}
        left_up = rect.left_up
        right_down = rect.right_down
        a_dict['polygon'] = '%s,%s|%s,%s'%(left_up.lng, left_up.lat, right_down.lng, right_down.lat)
        return a_dict

    def update_key(self, keys):
        if isinstance(keys,dict) and keys.__contains__('key'):
            super(Gdpoi_params,self).update(keys)
        else:
            raise TypeError("Imput is not a dict, or don't have key 'key'")

    def update_polygon(self, rect):
        if isinstance(rect, dict) and rect.__contains__('polygon'):
            super(Gdpoi_params, self).update(rect)
        else:
            raise TypeError("Imput is not a dict, or don't have key 'polygon'")

    def update_types(self,types):
        if isinstance(types, dict) and types.__contains__('types'):
            super(Gdpoi_params, self).update(types)
        else:
            raise TypeError("Imput is not a dict, or don't have key 'types'")

    def update_page(self, page):
        page_dict = {'page': page}
        super(Gdpoi_params, self).update(page_dict)

class Gdpoi_clawer(Clawer):
    def __init__(self, params):
        super(Gdpoi_clawer, self).__init__(params)
        self.url = 'http://restapi.amap.com/v3/place/polygon?'

    def scheduler(self):
        """根据状态码实现调度"""
        deal_code = ['10000', '10001', '10003', '10004', '10016', '10020','10021','10022', '10023']
        pass_code = ['20800', '20801', '20802', '20803', '20003']
        self.status_dict = {
            "10000": self.status_ok,
            "10001": self.status_change_key,
            "10003": self.status_change_key,
            "10004": self.status_change_user_agent,
            "10010": self.status_change_proxy,
            "10016": self.status_change_user_agent,
            "10020": self.status_change_key,
            "10021": self.status_change_proxy,
            "10022": self.status_change_proxy,
            "10023": self.status_change_key
        }
        status = self.respond['status']
        infocode = self.respond['infocode']
        # print(infocode)
        if infocode in deal_code:
            return self.status_dict[infocode]()
        elif infocode in pass_code:
            logger.info('出现 %s 跳过的网址 %s' % (infocode, self.req_url))
            self.status_pass()
        else:
            print(infocode)
            logger.info(infocode)
            self.status_invalid_request()

    def status_ok(self):
        all_res = []
        pois = self.respond.get('pois')
        if pois:
            for poi in pois:
                if poi:
                    all_res.append(self.parser(poi))
                else:
                    logger.info('没有值的连接是 %s' % self.req_url)
            return all_res
        else:
            logger.info('没有值的连接是 %s' % self.req_url)


    def parser(self, poi):
        res_dict = Res_Extractor().json_flatten(poi)
        return res_dict


    def get_count(self):
        self.requestor()
        count = self.respond.get("count")
        if count:
            return int(count)
        else:
            logger.info('没有count字段的网址是 %s' % self.req_url)
            return 0


class Gd_Sample_Generator(Sample_Generator):
    def __init__(self, region_name, category):
        super(Gd_Sample_Generator, self).__init__(region_name, category)

    def filter_count(self, rects, res_num):
        print('开始生成 %s %s 采样区域'%(self.region_name, self.category))
        loop_count = 0
        while rects:
            loop_count += 1
            print('第 %s 计算'%loop_count)
            rect = rects.pop()
            poi_clawer = clawer_init(rect, self.category)
            poi_num = poi_clawer.get_count()
            if poi_num > res_num:
                rects.extend(rect.divided_into_four())
            else:
                self.count_sati_rects.append(rect)
        print(u"%s %s 生成结果少于 %s 采样点 %s 个" % (self.region_name, self.category, res_num, len(self.count_sati_rects)))

def main(region_name, input_rect):
    res_floder = 'GD_poi_result/%s'% region_name
    type_changer = Type_Input('GD_POI_Type_L.csv', u'大类', res_floder, method='add')
    category_list = [{'types': i} for i in type_changer.type_list]
    base_shapefile = r"guangzhou\广州shapefile"
    def by_category(category):
        pic_title = '广东省_%s_%s'%(region_name, category['types'])
        geo_pic = Geo_Visual(pic_title, base_shapefile, figsize=[20, 15])
        shape_w = Shapefile_Write('ploygon', [('GD_count', 'N')])
        def by_rect(rect):
            lng1 = rect.left_down.lng
            lat1 = rect.left_down.lat
            lng2 = rect.right_up.lng
            lat2 = rect.right_up.lat
            geo_pic.add_patch(lng1, lat1, lng2, lat2)
            poi_clawer = clawer_init(rect, category, page=1)
            poi_num = poi_clawer.get_count()
            shape_w.plot([[lng1, lat1],
                          [lng1, lat2],
                          [lng2, lat2],
                          [lng2, lat1]
                          ], (poi_num,))
            def by_page(page):
                print('开始抓取 %s, 类别 %s, 区域 %s， 第 %s 页，'% (region_name, category['types'], rect, page))
                poi_clawer = clawer_init(rect, category, page)
                return poi_clawer.process()

            page_num = math.ceil(poi_num/20)
            page_list = range(1, page_num+1)
            pool_lv3 = ThreadPool()
            page_res = pool_lv3.map(by_page, page_list)
            pool_lv3.close()
            pool_lv3.join()

            rect_res = []
            for res in page_res:
                if isinstance(res, list):
                    rect_res += res
                else:
                    print(res)
            return rect_res

        category_results = []
        sample_generator = Gd_Sample_Generator(region_name, category)
        sample_generator.filter_radius([input_rect], 4000)
        raidus_right = sample_generator.radius_sati_rects
        print(raidus_right)
        sample_generator.filter_count(raidus_right, 1000)
        filtered_list = sample_generator.count_sati_rects
        print(filtered_list)
        pool_lv2 = ThreadPool()
        rect_result = pool_lv2.map(by_rect, filtered_list)
        pool_lv2.close()
        pool_lv2.join()

        for res in rect_result:
            if isinstance(res, list):
                category_results += res
            elif isinstance(res, None):
                pass

        res_saver = Res_saver(category_results, category['types'], floder_path='GD_poi_result/%s' % region_name, duplicates_key='id')
        res_saver.save_as_file()
        print('区域 %s, 类型 %s, 已保存'% (region_name, category['types']))
        geo_pic.savefig('%s_%s.png' % (region_name, category['types']))
        shape_w.save('sample_shapefile/%s/%s' % (region_name, category['types']))

    pool_lv1 = ThreadPool(1)
    pool_lv1.map(by_category, category_list)
    pool_lv1.close()
    pool_lv1.join()


def param_info(info_dict):
    info_table = prettytable.PrettyTable(['项目', '描述'])
    for key in list(info_dict.keys()):
        info_table.add_row([key, info_dict[key]])
    info_table.align = 'l'
    return str('\n' + str(info_table))

if __name__ == "__main__":
    # rect_dict = {
    #     "白云区" : [Rectangle(113.1461246, 23.13955449, 113.5008903, 23.43149718)],
    #     "从化区" : [Rectangle(113.2738078, 23.37099304, 114.0565605, 23.93695479)],
    #     "番禺区" : [Rectangle(113.2429326, 22.87177748, 113.5533215, 23.08258251)],
    #     "海珠区" : [Rectangle(113.2333014, 23.04533721, 113.4122732, 23.11366537)],
    #     "花都区" : [Rectangle(112.9540515, 23.24907373, 113.4694197, 23.61688869)],
    #     "荔湾区" : [Rectangle(113.1706897, 23.0442161, 113.2693343, 23.15839047)],
    #     "黄埔区" : [Rectangle(113.389631, 23.03409065, 113.6017962, 23.42672447)],
    #     "南沙区" : [Rectangle(113.2911038, 22.56227328, 113.6843494, 22.90920969)],
    #     "天河区" : [Rectangle(113.2922662, 23.09766052, 113.4391771, 23.24457675)],
    #     "越秀区" : [Rectangle(113.2323543, 23.10463126, 113.3178628, 23.17175286)],
    #     "增城区" : [Rectangle(113.5406707, 23.08627615, 113.9949777, 23.62208945)]
    # }
    a_rect = Rectangle().read_from_shp(r'D:\program_lib\GDPOI\guangzhou\广州shapefile')
    square = a_rect.convert_to_outline_square()
    rect_dict = {"广州市":[square]}
    start_time = datetime.datetime.now().strftime('%y-%m-%d %I:%M:%S %p')
    info_dict = {'名称': '高德 POI 抓取工具V1.0',
                 '邮箱': '575548935@qq.com',
                 '起始时间': start_time,
                 '终止时间': '20180401'
                 }
    logger.info(param_info(info_dict))
    for region_name, rect_list in rect_dict.items():
        main(region_name, rect_list[0])
    email_alarm = Email_alarm()
    end_time = datetime.datetime.now().strftime('%y-%m-%d %I:%M:%S %p')
    info_dict = {'名称': '高德 抓取工具V1.0',
                 '邮箱': '575548935@qq.com',
                 '起始时间': start_time,
                 '终止时间': end_time
                 }
    email_alarm.send_mail(param_info(info_dict))




