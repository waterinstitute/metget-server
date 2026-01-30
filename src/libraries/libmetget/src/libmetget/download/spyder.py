###################################################################################################
# MIT License
#
# Copyright (c) 2026 The Water Institute
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Zach Cobell
# Contact: zcobell@thewaterinstitute.org
# Organization: The Water Institute
#
###################################################################################################
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from loguru import logger

from .httpretry import http_retry_strategy


class Spyder:
    def __init__(self, url: str) -> None:
        """
        Initilaizes a spyder object which acts as a crawler
        through posted NOAA folders of grib/grib2 data.
        """
        self.__url: str = url

    def url(self) -> str:
        return self.__url

    def filelist(self) -> List[str]:
        """
        Generates the file list at the given url
        :return: list of files.
        """
        adapter = requests.adapters.HTTPAdapter(max_retries=http_retry_strategy())

        with requests.Session() as http:
            http.mount("https://", adapter)
            http.mount("http://", adapter)

            try:
                r = http.get(self.__url, timeout=30)
                if r.ok:
                    response_text: str = r.text
                else:
                    logger.warning(
                        f"Error contacting server: {self.__url}, Status: {r.status_code}"
                    )
                    return []
            except KeyboardInterrupt as e:
                raise e
            except Exception as e:
                logger.warning(
                    f"Exception occurred while contacting server: {self.__url}"
                )
                raise e

            soup = BeautifulSoup(response_text, "html.parser")

            links: List[str] = []
            for node in soup.find_all("a"):
                linkaddr: Optional[str] = node.get("href")
                if linkaddr and not (
                    "?" in linkaddr or not (linkaddr not in self.__url)
                ):
                    links.append(self.__url + linkaddr)
            return links
