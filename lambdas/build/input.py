#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 ADCIRC Development Group
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
#
class Input:
    def __init__(self, json_data):
        import uuid
        self.__json = json_data
        self.__start_date = None
        self.__end_date = None
        self.__operator = None
        self.__version = None
        self.__filename = None
        self.__format = None
        self.__time_step = None
        self.__domains = []
        self.__parse()
        self.__uuid = str(uuid.uuid4())

    def uuid(self):
        return self.__uuid

    def format(self):
        return self.__format

    def filename(self):
        return self.__filename

    def json(self):
        return self.__json

    def version(self):
        return self.__version

    def operator(self):
        return self.__operator

    def start_date(self):
        return self.__start_date

    def end_date(self):
        return self.__end_date

    def time_step(self):
        return self.__time_step

    def num_domains(self):
        return len(self.__domains)

    def domain(self, index):
        return self.__domains[index]

    def __parse(self):
        import dateutil.parser
        from domain import Domain
        from windgrid import WindGrid
        self.__version = self.__json["version"]
        self.__operator = self.__json["operator"]
        self.__start_date = dateutil.parser.parse(self.__json["start_date"])
        self.__start_date = self.__start_date.replace(tzinfo=None)
        self.__end_date = dateutil.parser.parse(self.__json["end_date"])
        self.__end_date = self.__end_date.replace(tzinfo=None)
        self.__time_step = self.__json["time_step"]
        self.__filename = self.__json["filename"]
        self.__format = self.__json["format"]
        ndomain = len(self.__json["domain"])
        if ndomain == 0:
            raise RuntimeError("You must specify one or more wind domains")
        for i in range(ndomain):
            name = self.__json["domain"][i]["name"]
            service = self.__json["domain"][i]["service"]
            self.__domains.append(
                Domain(name, service, WindGrid(json=self.__json["domain"][i])))