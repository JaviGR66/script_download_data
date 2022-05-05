from bs4 import BeautifulSoup
import requests

import time

# class Scraper(object):
#   def __init__(self, url):
#     self.url = url
#     print("Descargando...")
#     r = requests.get(url)
#     soup = BeautifulSoup(r.text, 'lxml')
#     self._raw_data = soup.select('form.form')
#     print(self._raw_data)
#     return self.raw_data
#
#   def getData(self):
#     pass
#
from neo4j import GraphDatabase


class BDDD_Conection(object):
    neo_connection = None

    def __init__(self, neo_driver):
        print("CONNECT")
        self.neo_connection = neo_driver.session()
        # query_adj = 'match (n) return n'
        # self.neo_connection.run(query_adj)

    def delete_database(self):
        self.neo_connection.run("MATCH (n) DETACH DELETE n")


class insert_data(object):
    neo_connection = None
    list_ccaa = ['Canarias', 'Principado de Asturias', 'Comunidad Valenciana', 'Región de Murcia',
                 'Galicia', 'Comunidad de Madrid', 'Castilla y León', 'Cantabria', 'Ceuta', 'Cataluña',
                 'País Vasco', 'La Rioja', 'Islas Baleares', 'Melilla', 'Castilla-La Mancha', 'Aragón',
                 'Comunidad Foral de Navarra', 'Extremadura', 'Andalucía']

    def __init__(self, neo_driver, datos):
        print("DATOS INSERT NEO ---> ",  datos)
        # if datos['Adjudicatario'] != 'Leandro Fernandez Mata ':
        print("LICITACION ACEPTADA")
        print(datos)
        self.neo_connection = neo_driver.session()
        self.adjudicatario = datos['Adjudicatario']
        self.anio = ""
        if self.adjudicatario == 'Ver detalle de la adjudicación':
            # print(self.adjudicatario)
            self.adjudicatario = 'Restringido/Publico'
        self.extra = datos[1]

        for year in range(1975, 2023):
            if str(year) in datos[1]:
                self.anio = str(year)
        if self.anio == "":
            for year in range(1975, 2023):
                if str(year) in datos["Fecha fin de presentación de oferta"].split("-")[0]:
                    self.anio = str(year)

        # CHANGE FOR 'España - Palencia' LUGAR DE EJECUCION
        self.ccaa = ""
        for com_data in datos[2].split(">"):
            for com_aut in self.list_ccaa:

                if com_data.strip() in com_aut or com_aut in com_data.strip():
                    self.ccaa = com_aut
                    break
        if self.ccaa == '':
            for com_aut in self.list_ccaa:
                for lug_ej in datos["Lugar de Ejecución"].split("-"):
                    if "/" in lug_ej.strip():
                        lug_ej = lug_ej.split("/")[0]
                    if lug_ej.strip() in com_aut:
                        self.ccaa = com_aut
                        break
        self.estado = datos["Estado de la Licitación"]
        self.presupuesto = datos["Presupuesto base de licitación sin impuestos"]
        self.valor_estimado = datos["Valor estimado del contrato"]
        # self.importe_adjudicacion = datos["Importe de Adjudicación"]
        if 'Importe de Adjudicación' in datos:
            self.importe_adjudicacion = datos['Importe de Adjudicación']
        else:
            self.importe_adjudicacion = ""
        if 'Enlace a la licitación' in datos:
            self.url = datos['Enlace a la licitación']
        else:
            self.url = "no link"
        if self.ccaa != "" and self.anio != "" and self.adjudicatario != "":
            try:
                self.create_licitacion()
                time.sleep(1)
            except:
                self.neo_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
                BDDD_Conection(self.neo_driver)
                self.create_licitacion()
                # time.sleep(5)
        else:
            print("NO CCAA OR NOT YEAR")
            # time.sleep(5)

    def create_licitacion(self):
        print("INSERT")

        query_adj = 'MERGE (adj:Adjudicatario {name:"' + self.adjudicatario + '"}) RETURN adj'
        self.neo_connection.run(query_adj)

        query_adj_ccaa = 'MATCH (adj:Adjudicatario), (ca:CCAA) WHERE adj.name = "' + self.adjudicatario + '" ' \
                          'AND ca.name = "' + self.ccaa + '" MERGE (ca)-[r:Asigna {anio:"' + self.anio + '",' \
                          'estado:"' + self.estado + '",presupuesto:"' + self.presupuesto + '",' \
                          'valor_estimado:"' + self.valor_estimado + '",enlace:"' + self.url + '",' \
                           'extra:"' + self.extra + '",importe_adjudicacion:"' + self.importe_adjudicacion + '"}]->(adj) RETURN r'
        # query_adj_ccaa = 'MATCH (adj:Adjudicatario {name = "' + self.adjudicatario + '" }),' \
        #                   'MATCH (ca:CCAA {name = "' + self.ccaa + '" })" CREATE  (ca)-[r:Asigna {anio:"' + self.anio + '",' \
        #                  'estado:"' + self.estado + '",presupuesto:"' + self.presupuesto + '",' \
        #                'valor_estimado:"' + self.valor_estimado + '",enlace:"' + self.url + '"}]->(adj) RETURN r'
        test = self.neo_connection.run(query_adj_ccaa)
        print(test.single()[0])
        # try:

        query_check_records = 'MATCH(n) RETURN count(n) as count'
        check_records = self.neo_connection.run(query_check_records).data()
        print("NUMBER OF RECORDS")
        print(check_records[0]['count'])
        query_check_records = 'MATCH p=()-[r:Asigna]->() RETURN count(p) as count'
        check_records_rel = self.neo_connection.run(query_check_records).data()
        print("NUMBER OF REL WITH ADJ")
        print(check_records_rel[0]['count'])
        time.sleep(1)
