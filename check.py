import logging
import sys
from pprint import pprint

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

from db.base_class import CpppcNew
from db.mongo_pool import MongoPoolCPPPC
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

    def 创建项目实体(self, 项目名称, 项目链接, 所处阶段, 汇报机制, 发起时间, 项目联系人, 项目总投资, 项目示范级别_批次,合作范围,
               联系电话, 项目概况, 合作期限, 运作方式, 采购方式, 物有所值评价评估结论, 所在省, 上游行业, 下游行业, 所在市=None, 所在县=None):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._创建并返回项目关系, 项目名称=项目名称, 项目链接=项目链接, 所处阶段=所处阶段, 汇报机制=汇报机制, 发起时间=发起时间, 项目联系人=项目联系人, 项目总投资=项目总投资, 项目示范级别_批次=项目示范级别_批次,合作范围=合作范围,
                联系电话=联系电话, 项目概况=项目概况, 合作期限=合作期限, 运作方式=运作方式, 采购方式=采购方式, 物有所值评价评估结论=物有所值评价评估结论, 所在省=所在省, 所在市=所在市, 所在县=所在县, 上游行业=上游行业, 下游行业=下游行业)
            for row in result:
                print("项目创建成功: {project}".format(
                    project=row['project']))

    @staticmethod
    def _创建并返回项目关系(tx, 项目名称, 项目链接, 所处阶段, 汇报机制, 发起时间, 项目联系人, 项目总投资, 项目示范级别_批次,合作范围,
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


if __name__ == "__main__":
    App.enable_log(logging.INFO, sys.stdout)
    app = App(bolt_uri, user_name, password)
    mongo = MongoPoolCPPPC()
    for _ in mongo.find_all():
        cur = CpppcNew(**_)
        pprint(cur.__dict__)
        app.创建项目实体(
            项目名称=cur.项目名称,
            项目链接=cur.项目链接,
            所处阶段=cur.所处阶段,
            汇报机制=cur.汇报机制,
            发起时间=cur.发起时间,
            项目联系人=cur.项目联系人,
            项目总投资=cur.项目总投资,
            项目示范级别_批次=cur.项目示范级别_批次,
            合作范围=cur.合作范围,
            联系电话=cur.联系电话,
            项目概况=cur.项目概况,
            合作期限=cur.合作期限,
            运作方式=cur.运作方式,
            采购方式=cur.采购方式,
            物有所值评价评估结论=cur.物有所值评价评估结论,
            所在省=cur.所在省,
            上游行业=cur.上游行业,
            下游行业=cur.下游行业,
            所在市=cur.所在市,
            所在县=cur.所在县
        )
    mongo.client.close()
    # app.create_friendship("Alice", "David", "School")
    # app.find_person("Alice")
    app.close()
