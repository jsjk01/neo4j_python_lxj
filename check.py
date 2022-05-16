# coding:utf-8

import logging
import sys
from pprint import pprint

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

from db.base_class import CpppcNew, nsfcNewFujianProject
from db.mongo_pool import MongoPoolCPPPC, MongoPoolNewNsfc
from important import bolt_uri, user_name, password


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    @staticmethod
    def enable_log(level, output_stream):
        handler = logging.StreamHandler(output_stream)
        handler.setLevel(level)
        logging.getLogger("neo4j").addHandler(handler)
        logging.getLogger("neo4j").setLevel(level)

    def 创建项目实体(self, 项目名称, 项目链接, 所处阶段, 汇报机制, 发起时间, 项目联系人, 项目总投资, 项目示范级别_批次, 合作范围,
               联系电话, 项目概况, 合作期限, 运作方式, 采购方式, 物有所值评价评估结论, 所在省, 上游行业, 下游行业, 所在市=None, 所在县=None):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._创建并返回项目关系, 项目名称=项目名称, 项目链接=项目链接, 所处阶段=所处阶段, 汇报机制=汇报机制, 发起时间=发起时间, 项目联系人=项目联系人, 项目总投资=项目总投资,
                项目示范级别_批次=项目示范级别_批次, 合作范围=合作范围,
                联系电话=联系电话, 项目概况=项目概况, 合作期限=合作期限, 运作方式=运作方式, 采购方式=采购方式, 物有所值评价评估结论=物有所值评价评估结论, 所在省=所在省, 所在市=所在市, 所在县=所在县,
                上游行业=上游行业, 下游行业=下游行业)
            for row in result:
                print("项目创建成功: {project}".format(
                    project=row['project']))

    @staticmethod
    def _创建并返回项目关系(tx, 项目名称, 项目链接, 所处阶段, 汇报机制, 发起时间, 项目联系人, 项目总投资, 项目示范级别_批次, 合作范围,
                   联系电话, 项目概况, 合作期限, 运作方式, 采购方式, 物有所值评价评估结论, 所在省, 上游行业, 下游行业, 所在市, 所在县):

        if 所在县 is not None:
            project = """
                MERGE (project:项目  {项目名称: $项目名称, 项目链接: $项目链接, 所处阶段: $所处阶段, 发起时间: $发起时间, 项目总投资: $项目总投资, 项目示范级别_批次: $项目示范级别_批次, 合作范围: $合作范围, 项目概况: $项目概况, 合作期限: $合作期限, 运作方式: $运作方式, 采购方式: $采购方式, 物有所值评价评估结论: $物有所值评价评估结论, 所在省: $所在省, 所在市: $所在市, 所在县: $所在县})
                MERGE (province: 省 { 省份: $所在省}) 
                MERGE (city: 市 { 市: $所在市})
                MERGE (county: 县 { 县: $所在县})
                MERGE (p1: 项目联系人 { 姓名: $项目联系人, 电话: $联系电话 })
                MERGE (hangye1: 一级行业 { 行业: $上游行业}) 
                MERGE (hangye2: 二级行业 { 行业: $下游行业})
                MERGE (city)-[:所在省]->(province)
                MERGE (county)-[:所在市]->(city)
                MERGE (project)-[:所在县]->(county)
                MERGE (project)-[:所在市]->(city)
                MERGE (project)-[:所在省]->(province)
                MERGE (project)-[:所属行业]->(hangye2)
                MERGE (hangye2)-[:上游行业]->(hangye1)
                MERGE (p1)-[m:项目联系人]->(project)
                RETURN project
            """
            query = (
                project
            )
            pprint(query)
            result = tx.run(query,
                            项目名称=项目名称,
                            项目链接=项目链接,
                            所处阶段=所处阶段,
                            汇报机制=汇报机制,
                            发起时间=发起时间,
                            项目联系人=项目联系人,
                            项目总投资=项目总投资,
                            项目示范级别_批次=项目示范级别_批次,
                            合作范围=合作范围,
                            联系电话=联系电话,
                            项目概况=项目概况,
                            合作期限=合作期限,
                            运作方式=运作方式,
                            采购方式=采购方式,
                            物有所值评价评估结论=物有所值评价评估结论,
                            所在省=所在省,
                            所在市=所在市.strip(),
                            所在县=所在县.strip(),
                            上游行业=上游行业,
                            下游行业=下游行业.strip())
        elif 所在市 is not None:
            project = """
                MERGE (project:项目  {项目名称: $项目名称, 项目链接: $项目链接, 所处阶段: $所处阶段, 发起时间: $发起时间, 项目总投资: $项目总投资, 项目示范级别_批次: $项目示范级别_批次, 合作范围: $合作范围, 项目概况: $项目概况, 合作期限: $合作期限, 运作方式: $运作方式, 采购方式: $采购方式, 物有所值评价评估结论: $物有所值评价评估结论, 所在省: $所在省, 所在市: $所在市})
                MERGE (province: 省 { 省份: $所在省}) 
                MERGE (city: 市 { 市: $所在市})
                MERGE (p1: 项目联系人 { 姓名: $项目联系人, 电话: $联系电话 })
                MERGE (hangye1: 一级行业 { 行业: $上游行业}) 
                MERGE (hangye2: 二级行业 { 行业: $下游行业})
                MERGE (city)-[:所在省]->(province)
                MERGE (project)-[:所在市]->(city)
                MERGE (project)-[:所在省]->(province)
                MERGE (project)-[:所属行业]->(hangye2)
                MERGE (hangye2)-[:上游行业]->(hangye1)
                MERGE (p1)-[m:项目联系人]->(project)
                RETURN project
                """
            query = (
                project
            )
            pprint(query)
            result = tx.run(query,
                            项目名称=项目名称,
                            项目链接=项目链接,
                            所处阶段=所处阶段,
                            汇报机制=汇报机制,
                            发起时间=发起时间,
                            项目联系人=项目联系人,
                            项目总投资=项目总投资,
                            项目示范级别_批次=项目示范级别_批次,
                            合作范围=合作范围,
                            联系电话=联系电话,
                            项目概况=项目概况,
                            合作期限=合作期限,
                            运作方式=运作方式,
                            采购方式=采购方式,
                            物有所值评价评估结论=物有所值评价评估结论,
                            所在省=所在省,
                            所在市=所在市.strip(),
                            上游行业=上游行业,
                            下游行业=下游行业.strip())
        else:
            project = """
                MERGE (project:项目  {项目名称: $项目名称, 项目链接: $项目链接, 所处阶段: $所处阶段, 发起时间: $发起时间, 项目总投资: $项目总投资, 项目示范级别_批次: $项目示范级别_批次, 合作范围: $合作范围, 项目概况: $项目概况, 合作期限: $合作期限, 运作方式: $运作方式, 采购方式: $采购方式, 物有所值评价评估结论: $物有所值评价评估结论, 所在省: $所在省})
                MERGE (province: 省 { 省份: $所在省}) 
                MERGE (p1: 项目联系人 { 姓名: $项目联系人, 电话: $联系电话 })
                MERGE (hangye1: 一级行业 { 行业: $上游行业}) 
                MERGE (hangye2: 二级行业 { 行业: $下游行业})
                MERGE (project)-[:所在省]->(province)
                MERGE (project)-[:所属行业]->(hangye2)
                MERGE (hangye2)-[:上游行业]->(hangye1)
                MERGE (p1)-[m:项目联系人]->(project)
                RETURN project
                """
            query = (
                project
            )
            pprint(query)
            result = tx.run(query,
                            项目名称=项目名称,
                            项目链接=项目链接,
                            所处阶段=所处阶段,
                            汇报机制=汇报机制,
                            发起时间=发起时间,
                            项目联系人=项目联系人,
                            项目总投资=项目总投资,
                            项目示范级别_批次=项目示范级别_批次,
                            合作范围=合作范围,
                            联系电话=联系电话,
                            项目概况=项目概况,
                            合作期限=合作期限,
                            运作方式=运作方式,
                            采购方式=采购方式,
                            物有所值评价评估结论=物有所值评价评估结论,
                            所在省=所在省,
                            上游行业=上游行业,
                            下游行业=下游行业.strip())
        # query = (
        #     project
        # )
        # pprint(query)
        # result = tx.run(query,
        #                 项目名称=项目名称,
        #                 项目链接=项目链接,
        #                 所处阶段=所处阶段,
        #                 汇报机制=汇报机制,
        #                 发起时间=发起时间,
        #                 项目联系人=项目联系人,
        #                 项目总投资=项目总投资,
        #                 项目示范级别_批次=项目示范级别_批次,
        #                 合作范围=合作范围,
        #                 联系电话=联系电话,
        #                 项目概况=项目概况,
        #                 合作期限=合作期限,
        #                 运作方式=运作方式,
        #                 采购方式=采购方式,
        #                 物有所值评价评估结论=物有所值评价评估结论,
        #                 所在省=所在省,
        #                 所在市=所在市,
        #                 所在县=所在县,
        #                 上游行业=上游行业,
        #                 下游行业=下游行业)
        try:
            return [{
                "project": row["project"]["项目名称"],
            }
                for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_friendship(self, person1_name, person2_name, knows_from):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_and_return_friendship, person1_name, person2_name, knows_from)
            for row in result:
                print("Created friendship between: {p1}, {p2} from {knows_from}".format(
                    p1=row['p1'],
                    p2=row['p2'],
                    knows_from=row["knows_from"]))

    @staticmethod
    def _create_and_return_friendship(tx, person1_name, person2_name, knows_from):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = (
            "MERGE (p1:Person { name: $person1_name }) "
            "MERGE (p2:Person { name: $person2_name }) "
            "MERGE (p1)-[k:KNOWS { from: $knows_from }]->(p2) "
            "RETURN p1, p2, k"
        )
        result = tx.run(query, person1_name=person1_name,
                        person2_name=person2_name, knows_from=knows_from)
        try:
            return [{
                "p1": row["p1"]["name"],
                "p2": row["p2"]["name"],
                "knows_from": row["k"]["from"]
            }
                for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_person(self, person_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_person, person_name)
            for row in result:
                print("Found person: {row}".format(row=row))

    @staticmethod
    def _find_and_return_person(tx, person_name):
        query = (
            "MERGE (p:Person) "
            "WHERE p.name = $person_name "
            "RETURN p.name AS name"
        )
        result = tx.run(query, person_name=person_name)
        return [row["name"] for row in result]


class NSFC:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    @staticmethod
    def enable_log(level, output_stream):
        handler = logging.StreamHandler(output_stream)
        handler.setLevel(level)
        logging.getLogger("neo4j").addHandler(handler)
        logging.getLogger("neo4j").setLevel(level)

    def 创建课题实体(self, 链接, 题名, 批准号, 项目类别, 依托单位, 项目负责人, 资助经费_万元, 批准年度, 关键词, 是否结题, 研究成果类别, 依托单位链接, 项目负责人链接, 资助类别代码,
               申请代码, 备注, 备注2, 备注3, 备注4, 省份):
        if 备注3 is None:
            备注3 = '空'
        if 备注4 is None:
            备注4 = '空'
        with self.driver.session() as session:
            result = session.write_transaction(
                self._创建并返回课题关系, 链接, 题名, 批准号, 项目类别, 依托单位, 项目负责人, 资助经费_万元, 批准年度, 关键词, 是否结题, 研究成果类别, 依托单位链接, 项目负责人链接,
                资助类别代码, 申请代码, 备注, 备注2, 备注3, 备注4, 省份)
            for row in result:
                print("项目创建成功: {project}".format(
                    project=row['project']))

    @staticmethod
    def _创建并返回课题关系(tx, 链接, 题名, 批准号, 项目类别, 依托单位, 项目负责人, 资助经费_万元, 批准年度, 关键词, 是否结题, 研究成果类别, 依托单位链接, 项目负责人链接, 资助类别代码,
                   申请代码, 备注, 备注2, 备注3, 备注4, 省份):
        project = """
            MERGE (project:项目{题名: $题名, 链接: $链接, 批准号: $批准号, 项目类别: $项目类别, 依托单位: $依托单位, 项目负责人: $项目负责人, 资助经费_万元: $资助经费_万元, 批准年度: $批准年度, 关键词: $关键词, 是否结题: $是否结题, 研究成果类别: $研究成果类别, 依托单位链接: $依托单位链接, 项目负责人链接: $项目负责人链接, 资助类别代码: $资助类别代码, 申请代码: $申请代码, 备注: $备注, 备注2: $备注2, 备注3: $备注3, 备注4: $备注4, 省份: $省份})
            MERGE (province: 省 { 省份名: $省份}) 
            MERGE (man: 人 { 名字: $项目负责人, 链接: $项目负责人链接} )
            MERGE (department: 单位 { 名字: $依托单位, 链接: $依托单位链接})
            MERGE (man)-[:负责]->(project)
            MERGE (project)-[:委托负责]->(man)
            MERGE (man)-[:就职]->(department)
            MERGE (department)-[:聘用]->(man)
            MERGE (province)-[:存在课题]->(project)
            MERGE (project)-[:所在省]->(province)
            RETURN project
            """

        query = (
            project
        )
        pprint(query)
        result = tx.run(query,
                        链接=链接,
                        题名=题名,
                        批准号=批准号,
                        项目类别=项目类别,
                        依托单位=依托单位,
                        项目负责人=项目负责人,
                        资助经费_万元=资助经费_万元,
                        批准年度=批准年度,
                        关键词=关键词,
                        是否结题=是否结题,
                        研究成果类别=研究成果类别,
                        依托单位链接=依托单位链接,
                        项目负责人链接=项目负责人链接,
                        资助类别代码=资助类别代码,
                        申请代码=申请代码,
                        备注=备注,
                        备注2=备注2,
                        备注3=备注3,
                        备注4=备注4,
                        省份=省份)

        try:
            return [{
                "project": row["project"]["题名"],
            }
                for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_friendship(self, person1_name, person2_name, knows_from):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_and_return_friendship, person1_name, person2_name, knows_from)
            for row in result:
                print("Created friendship between: {p1}, {p2} from {knows_from}".format(
                    p1=row['p1'],
                    p2=row['p2'],
                    knows_from=row["knows_from"]))

    @staticmethod
    def _create_and_return_friendship(tx, person1_name, person2_name, knows_from):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = (
            "MERGE (p1:Person { name: $person1_name }) "
            "MERGE (p2:Person { name: $person2_name }) "
            "MERGE (p1)-[k:KNOWS { from: $knows_from }]->(p2) "
            "RETURN p1, p2, k"
        )
        result = tx.run(query, person1_name=person1_name,
                        person2_name=person2_name, knows_from=knows_from)
        try:
            return [{
                "p1": row["p1"]["name"],
                "p2": row["p2"]["name"],
                "knows_from": row["k"]["from"]
            }
                for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def find_person(self, person_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_and_return_person, person_name)
            for row in result:
                print("Found person: {row}".format(row=row))

    @staticmethod
    def _find_and_return_person(tx, person_name):
        query = (
            "MERGE (p:Person) "
            "WHERE p.name = $person_name "
            "RETURN p.name AS name"
        )
        result = tx.run(query, person_name=person_name)
        return [row["name"] for row in result]


if __name__ == "__main__":
    NSFC.enable_log(logging.INFO, sys.stdout)
    nsfc = NSFC(bolt_uri, user_name, password)
    mongo = MongoPoolNewNsfc()
    for _ in mongo.find_all():
        print('*' * 10)
        pprint(_)
        cur = nsfcNewFujianProject(**_)
        pprint(cur)
        cur.备注3 = '空' if cur.备注3 is None or len(cur.备注3) < 1 else print(cur.备注3)
        cur.备注4 = '空' if cur.备注4 is None or len(cur.备注4) < 1 else print(cur.备注4)
        nsfc.创建课题实体(
            链接=cur.链接,
            题名=cur.题名,
            批准号=cur.批准号,
            项目类别=cur.项目类别,
            依托单位=cur.依托单位,
            项目负责人=cur.项目负责人,
            资助经费_万元=cur.资助经费_万元,
            批准年度=cur.批准年度,
            关键词=cur.关键词,
            是否结题=cur.是否结题,
            研究成果类别=cur.研究成果类别,
            依托单位链接=cur.依托单位链接,
            项目负责人链接=cur.项目负责人链接,
            资助类别代码=cur.资助类别代码,
            申请代码=cur.申请代码,
            备注=cur.备注,
            备注2=cur.备注2,
            备注3=cur.备注3,
            备注4=cur.备注4,
            省份=cur.省份
        )
        print('*' * 10)
    mongo.client.close()
    # app.create_friendship("Alice", "David", "School")
    # app.find_person("Alice")
    nsfc.close()
