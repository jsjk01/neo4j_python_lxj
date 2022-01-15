from db.mongo_pool import MongoPoolCPPPC
import py2neo
from py2neo import Node, Graph, Relationship

from important import bolt_uri, user_name, password

g = Graph(bolt_uri, user=user_name, password=password)
g.delete_all()


def main():
    mongo = MongoPoolCPPPC()
    temp_dict = {}

    items = mongo.find_all()
    for index, item in enumerate(items):
        print(len(item.items()))
        for key, value in item.items():
            print(index, key, value)

        temp_command = f'''
            craete ({item['项目名称']}:项目{{name}})
        '''
        print()
        # 项目名称 = Node('项目名', project_name=item['项目名称'])
        # 项目链接 = Node('链接', project_url=item['项目链接'])
        # 项目名称_项目链接 = Relationship(项目名称, '项目链接', 项目链接)
        # g.merge(项目名称, '项目名', 'project_name')
        # g.merge(项目链接, '链接', 'project_url')
        # g.merge(项目名称_项目链接, '项目名', ' 链接')
        # 所处阶段 = Node('所处阶段', stage=item['所处阶段'])
        # g.merge(所处阶段, '所处阶段', 'stage')
        # 项目名称_所处阶段 = Relationship(项目名称, '所处阶段', 所处阶段)
        # g.merge(项目名称_所处阶段, '项目名', '所处阶段')
        # 汇报机制 = Node('汇报机制', reporting_mechanisms=item['汇报机制'])
        # g.merge(汇报机制, '汇报机制', 'reporting_mechanisms')
        # 项目名称_汇报机制 = Relationship(项目名称, '汇报机制', 汇报机制)
        # g.merge(项目名称_汇报机制, '项目名', '汇报机制')
        # 发起时间 = Node('时间', initiation_time=item['发起时间'])
        # g.merge(发起时间, '发起时间', 'initiation_time')
        # 项目名称_发起时间 = Relationship(项目名称, '发起时间', 发起时间)
        # g.merge(项目名称_发起时间, '项目名', '发起时间')
        # 项目联系人 = Node('项目联系人', project_contact_person_name=item['项目联系人'])
        # g.merge(项目联系人, '项目联系人', 'project_contact_person_name')
        # 项目名称_项目联系人 = Relationship(项目名称, '项目联系人', 项目联系人)
        # g.merge(项目名称_项目联系人, '项目名', '项目联系人')
        # 项目总投资 = Node('项目总投资（万元）', total_project_investment=item['项目总投资'].replace('万元', '').replace(',', ''))
        # g.merge(项目总投资, '项目总投资', 'total_project_investment')
        # 项目名称_项目总投资 = Relationship(项目名称, '项目总投资', 项目总投资)
        # g.merge(项目名称_项目总投资, '项目名', '项目总投资')
        # 项目示范级别_批次 = Node('项目示范级别_批次', project_demonstration_level_name=item['项目示范级别_批次'])
        # g.merge(项目示范级别_批次, '项目示范级别_批次', 'project_demonstration_level_name')
        # 项目名称_项目示范级别_批次 = Relationship(项目名称, '项目示范级别_批次', 项目示范级别_批次)
        # g.merge(项目名称_项目示范级别_批次, '项目名', '项目示范级别_批次')
        # 联系电话 = Node('联系电话', tel=item['联系电话'])
        # g.merge(联系电话, '联系电话', 'tel')
        # 项目联系人_联系电话 = Relationship(项目联系人, '联系电话', 联系电话)
        # g.merge(项目联系人_联系电话, '项目联系人', '联系电话')
        # 项目概况 = Node('项目概况', description=item['项目概况'])
        # g.merge(项目概况, '项目概况', 'description')
        # 项目名称_项目概况 = Relationship(项目名称, '项目概况', 项目概况)
        # g.merge(项目名称_项目概况, '项目名', '项目概况')
        # 合作范围 = Node('合作范围', range=item['合作范围'])
        # g.merge(合作范围, '合作范围', 'range')
        # 项目名称_合作范围 = Relationship(项目名称, '合作范围', 合作范围)
        # g.merge(项目名称_合作范围, '项目名', '合作范围')
        # 合作期限 = Node('合作期限(年)', duration_of_cooperation=item['合作期限'].replace('年', ''))
        # g.merge(合作期限, '合作期限', 'duration_of_cooperation')
        # 项目名称_合作期限 = Relationship(项目名称, '合作期限', 合作期限)
        # g.merge(项目名称_合作期限, '项目名', '合作期限')
        # 运作方式 = Node('运作方式', how_it_works=item['运作方式'])
        # g.merge(运作方式, '运作方式', 'how_it_works')
        # 项目名称_运作方式 = Relationship(项目名称, '运作方式', 运作方式)
        # g.merge(项目名称_运作方式, '项目名', '运作方式')
        # 采购方式 = Node('采购方式', procurement_methods=item['采购方式'])
        # g.merge(采购方式, '采购方式', 'procurement_methods')
        # 项目名称_采购方式 = Relationship(项目名称, '采购方式', 采购方式)
        # g.merge(项目名称_采购方式, '项目名', '采购方式')
        # 物有所值评价评估结论 = Node('物有所值评价评估结论', pass_or_not=item['物有所值评价评估结论'])
        # g.merge(物有所值评价评估结论, '物有所值评价评估结论', 'pass_or_not')
        # 项目名称_物有所值评价评估结论 = Relationship(项目名称, '物有所值评价评估结论', 物有所值评价评估结论)
        # g.merge(项目名称_物有所值评价评估结论, '项目名', '物有所值评价评估结论')
        # 所在省 = Node('省', province=item['所在省'])
        # g.merge(所在省, '省', 'province')
        # 项目名称_所在省 = Relationship(项目名称, '所在省', 所在省)
        # g.merge(项目名称_所在省, '项目名', '所在省')
        # if item['所在市'] is not None:
        #     所在市 = Node('市', city=item['所在市'])
        #     g.merge(所在市, '市', 'city')
        #     省市 = Relationship(所在省, '下属市', 所在市)
        #     g.merge(省市, '下属市', 所在市)
        #     if item['所在县'] is not None:
        #         所在县 = Node('县', county=item['所在县'])
        #         g.merge(所在县, '县', 'county')
        #         项目名称_所在县= Relationship(项目名称, '所在县', 所在县)
        #         g.merge(项目名称_所在县, '项目名', '所在县')
        #         市县 = Relationship(所在市, '下属县', 所在县)
        #         g.merge(市县, '下属县', 所在县)
        # 上游行业 = Node('上游行业', upstream_industries=item['上游行业'])
        # g.merge(上游行业, '上游行业', 'upstream_industries')
        # 项目名称_上游行业 = Relationship(项目名称, '上游行业', 上游行业)
        # g.merge(项目名称_上游行业, '项目名', '上游行业')
        # if item['下游行业'] is not None:
        #     下游行业 = Node('下游行业', downstream_industries=item['下游行业'])
        #     g.merge(下游行业, '下游行业', 'downstream_industries')
        #     项目名称_下游行业 = Relationship(项目名称, '下游行业', 下游行业)
        #     g.merge(项目名称_下游行业, '项目名', '下游行业')

        #
    mongo.client.close()


if __name__ == '__main__':
    main()
