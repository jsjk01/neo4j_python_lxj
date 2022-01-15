# @Time    : 2021/5/12 17:50
# @Author  : LXJ
# @FileName: base_class.py
# @Software: PyCharm
from datetime import datetime


class CpppcNew(object):

    def __init__(self, 项目名称, 项目链接, 所处阶段, 汇报机制, 发起时间, 项目联系人, 项目总投资, 项目示范级别_批次, 联系电话, 项目概况, 合作范围, 合作期限, 运作方式,
                 采购方式, 物有所值评价评估结论, 所在省, 上游行业, 下游行业=None, 所在市=None, 所在县=None, ):
        self.项目名称 = 项目名称
        self.项目链接 = 项目链接
        self.所处阶段 = 所处阶段
        self.汇报机制 = 汇报机制
        # self.所属行业 = 所属行业
        self.发起时间 = 发起时间
        self.项目联系人 = 项目联系人
        self.项目总投资 = 项目总投资
        self.项目示范级别_批次 = 项目示范级别_批次
        self.联系电话 = 联系电话
        self.项目概况 = 项目概况
        self.合作范围 = 合作范围
        self.合作期限 = 合作期限
        self.运作方式 = 运作方式
        self.采购方式 = 采购方式
        self.物有所值评价评估结论 = 物有所值评价评估结论
        self.所在省 = 所在省
        self.所在市 = 所在市
        self.所在县 = 所在县
        self.上游行业 = 上游行业
        self.下游行业 = 下游行业

        def __str__(self):
            return str(self.__dict__)


class Cpppc(object):

    def __init__(self, 项目名称, 项目链接, 所在区域, 所处阶段, 汇报机制, 所属行业, 发起时间, 项目联系人, 项目总投资, 项目示范级别_批次, 联系电话, 项目概况, 合作范围, 合作期限, 运作方式,
                 采购方式, 物有所值评价评估结论):
        self.项目名称 = 项目名称
        self.项目链接 = 项目链接
        self.所在区域 = 所在区域
        self.所处阶段 = 所处阶段
        self.汇报机制 = 汇报机制
        self.所属行业 = 所属行业
        self.发起时间 = 发起时间
        self.项目联系人 = 项目联系人
        self.项目总投资 = 项目总投资
        self.项目示范级别_批次 = 项目示范级别_批次
        self.联系电话 = 联系电话
        self.项目概况 = 项目概况
        self.合作范围 = 合作范围
        self.合作期限 = 合作期限
        self.运作方式 = 运作方式
        self.采购方式 = 采购方式
        self.物有所值评价评估结论 = 物有所值评价评估结论

        def __str__(self):
            return str(self.__dict__)
