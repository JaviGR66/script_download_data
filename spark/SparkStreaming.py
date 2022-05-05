import time

import findspark
import pyspark
import socket
import sys
from neo4j import GraphDatabase
from bs4 import BeautifulSoup
from pyspark.sql.session import SparkSession

from scraper.Scraper import insert_data, BDDD_Conection

TCP_IP = "localhost"
TCP_PORT = 10007


# TCP_PORT = 35000


class SparkStreaming(object):
    def __init__(self):
        findspark.init()
        self.neo_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
        BDDD_Conection(self.neo_driver)
        sc = pyspark.SparkContext(appName="CALLER_02")
        self._spark = SparkSession(sc)

        self._s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._s.bind((TCP_IP, TCP_PORT))
        self._s.listen(1)

        print("Waiting for TCP connection...")
        self._conn, self._addr = self._s.accept()
        # print("hola")

    def send_raw_data(self, raw_data):
        # print("------------------------------------------")
        # print("Data: " + str(raw_data))
        # print("------------------END DATA------------------------")
        time.sleep(1)
        self.insert_neo(str(raw_data).replace("\n", ""))
        # print("Data: " + str(raw_data))
        # print("Data: " + type(raw_data))
        # exit()
        # try:
        #     print("------------------------------------------")
        #     print("Data: " + raw_data)
        #     print(self._conn.send(raw_data))
        # except:
        #     e = sys.exc_info()[0]
        #     print("Error: %s" % e)

    def insert_neo(self, raw_data):
        soup = BeautifulSoup(raw_data, 'html.parser')
        print("************************************")
        # print("DATA: ---->", raw_data)
        # data = soup.find_all('form')

        print("************************************")
        # print("DATA: ---->", data)
        # b4s_data = self.get_values_b4s(str(data))
        b4s_data = self.get_values_b4s(str(raw_data).replace("\n", ""))
        print("*****************BS4*******************")

        print("B4S: ", b4s_data[1])
        print(" -------- INSERT NEO --------")

        insert_data(self.neo_driver, b4s_data)

        print("*****************END DATA*******************")

    def get_values_b4s(self, bs4_extract):
        soup = BeautifulSoup(bs4_extract, "html.parser")
        spans = soup.find_all('span')
        final_values = {}
        tipo3 = ""
        cont = 0
        cont_cab = 0
        for span in spans:
            if span.has_attr('class') and len(span['class']) > 1 and span.get("title") == 'Resumen Licitación':
                return final_values
            if span.has_attr('class') and span['class'][0] == 'tipo3':
                tipo3 = span.get("title").replace(":", "").strip()
            if span.has_attr('class') and span['class'][0] == 'outputText':
                if tipo3 == 'Enlace a la licitación':
                    print(tipo3)
                if tipo3 != "":
                    final_values[tipo3] = str(span.get("title"))
                    tipo3 = ""
                else:
                    final_values[cont] = str(span.get("title"))
                    tipo3 = ""
                    cont += 1
            if str(span.get("title")).startswith("http"):
                print(spans.get("title"))
        return final_values





