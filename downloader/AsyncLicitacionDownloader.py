import asyncio
from datetime import datetime
from urllib.request import urlopen
import aiohttp
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import time
import subprocess


class AsyncLicitacionDownloader(object):
    def __init__(self, observable, schedule, url):
        self._url = url
        self._observable = observable
        self._schedule = schedule
        self._prev_url = ""

    async def async_download(self):
        instanteInicial = datetime.now()
        while self._url is not None:
            # counter = 0

            tasks = []
            await asyncio.sleep(0)
            time.sleep(1)
            instanteFinal = datetime.now()
            tiempo = instanteFinal - instanteInicial  # Devuelve un objeto timedelta
            segundos = tiempo.seconds
            print("segundos", segundos)
            if int(segundos) >= 3600:
                print("Ha pasado 1 hora, refresh de IP...")
                # subprocess.run(["dhclient –r"])
                # subprocess.run(["dhclient"])
                time.sleep(650)
                instanteInicial = datetime.now()

            try:
                content = urlopen(self._url)

                tree = ET.parse(content)
                root = tree.getroot()

                entry_tag = '{http://www.w3.org/2005/Atom}entry'
                # await asyncio.sleep(1)
                for element in root:
                    if element.tag == entry_tag:
                        new_link = element.find('{http://www.w3.org/2005/Atom}link').attrib['href']
                        print(new_link)
                        # Insertamos la task de los datos a descargar
                        tasks.append(asyncio.create_task(self.__download_data__(new_link)))
                    if 'rel' in element.attrib and element.attrib['rel'] == 'next':
                        prev_url = self._url
                        self._prev_url = prev_url
                        url = element.attrib['href']
                # Lanzar la descarga de todos los datos de esta pagina

                await asyncio.gather(*tasks)
                if prev_url is not url and counter < 1:
                    counter = counter + 1
                    print(counter)
                else:
                    url = None
                    print('Search Finished')
                #if prev_url is url:
                #  url = None
                #  print('Search Finished')
                #time.sleep(1)
            except Exception as e:
                print(e)
                print(e.__traceback__)

    async def __download_data__(self, url):
        async with aiohttp.ClientSession() as session:
            try:
                # url = "https://contrataciondelestado.es/wps/portal/!ut/p/b0/04_Sj9CPykssy0xPLMnMz0vMAfIjU1JTC3Iy87KtUlJLEnNyUuNzMpMzSxKTgQr0w_Wj9KMyU1zLcvQjMyr8Ul3TzL393RKLqozdA1NMi8yCA21t9Qtycx0BdUKI-Q!!/"
                async with session.get(url) as response:
                    # print("DOWNLOAD")
                    soup = BeautifulSoup(await response.text('utf-8'), "html.parser")
                    raw_data = soup.find_all('form')[1]
                    # print("RAW DATA", raw_data)
                    self._observable.on_next({
                        'raw_data': raw_data
                    })
            except Exception as e:
                print("Download data error -->", e)
                if "Cannot connect to host" in str(e):
                    print("Sleep for 1 hour...")
                    time.sleep(600)
                elif "Response payload" in str(e):
                    print("payload error with url: ", self._url)
                    #await asyncio.sleep(1)
                    time.sleep(1)
                    file_error_url = open("url_error_payload", "w+")
                    text_to_append = "\nResponse payload error --> " + self._url
                    file_error_url.write(text_to_append)
                    # file_error_url.close()
                #     self._url = self._prev_url
