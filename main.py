import time

from db.base_class import Cpppc, CpppcNew
from db.mongo_pool import MongoPoolCPPPC, MongoPoolP


def main():
    mongo_new = MongoPoolCPPPC()
    mongo = MongoPoolP()
    old_data = mongo.find_all()
    for index, i in enumerate(old_data):
        cur_item = Cpppc(**i)
        temp_dict = cur_item.__dict__
        _所在区域 = temp_dict.pop('所在区域')
        _所属行业 = temp_dict.pop('所属行业')
        _所属行业_list = _所属行业.split('-')
        _所在区域_list = _所在区域.split('-')
        try:
            if len(_所在区域_list) == 1:
                temp_dict['所在省'] = _所在区域_list[0]
            elif len(_所在区域_list) == 2:
                temp_dict['所在省'] = _所在区域_list[0]
                temp_dict['所在市'] = _所在区域_list[1]
            else:
                temp_dict['所在省'] = _所在区域_list[0]
                temp_dict['所在市'] = _所在区域_list[1]
                temp_dict['所在县'] = _所在区域_list[2]
            if _所属行业_list == 1:
                temp_dict['上游行业'] = _所属行业_list[0]
            else:
                temp_dict['上游行业'] = _所属行业_list[0]
                temp_dict['下游行业'] = _所属行业_list[1]
        except Exception as e:
            print(e)
            print(i)
            time.sleep(5)

        mongo_new.insert_one(CpppcNew(**temp_dict))
    mongo.client.close()
    mongo_new.client.close()


if __name__ == '__main__':
    main()
