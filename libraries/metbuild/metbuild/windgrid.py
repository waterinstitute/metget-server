#!/usr/bin/env python3
###################################################################################################
# MIT License
#
# Copyright (c) 2023 The Water Institute
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
class WindGrid:
    """
    WindGrid is a class that represents a wind grid and is used for
    interpolation and other operations on the wind grid
    """

    def __init__(self, json=None, no_construct=False):
        """
        Constructor for WindGrid

        Args:
            json: A dictionary containing the json data for the wind grid
            no_construct: A boolean indicating whether to construct the wind grid in the c++ code
        """
        self.__json = json
        self.__wg = None
        self.__no_construct = no_construct
        self.__valid = True
        self.__nx = 0
        self.__ny = 0
        try:
            self.__construct()
        except Exception as e:
            self.__valid = False
            raise

    def valid(self):
        """
        Returns whether the wind grid is valid

        Returns:
            A boolean indicating whether the wind grid is valid
        """
        return self.__valid

    def grid_object(self):
        """
        Returns the grid object

        Returns:
            The grid object
        """
        return self.__wg

    def nx(self) -> int:
        """
        Returns the number of grid points in the x direction

        Returns:
            The number of grid points in the x direction
        """
        if self.__wg:
            return self.__wg.ni()
        else:
            return self.__nx

    def ny(self) -> int:
        """
        Returns the number of grid points in the y direction

        Returns:
            The number of grid points in the y direction
        """
        if self.__wg:
            return self.__wg.nj()
        else:
            return self.__ny

    def bottom_left(self):
        """
        Returns the bottom left corner of the wind grid

        Returns:
            The bottom left corner of the wind grid
        """
        return self.__wg.bottom_left()

    def bottom_right(self):
        """
        Returns the bottom right corner of the wind grid

        Returns:
            The bottom right corner of the wind grid
        """
        return self.__wg.bottom_right()

    def top_left(self):
        """
        Returns the top left corner of the wind grid

        Returns:
            The top left corner of the wind grid
        """
        return self.__wg.top_left()

    def top_right(self):
        """
        Returns the top right corner of the wind grid

        Returns:
            The top right corner of the wind grid
        """
        return self.__wg.top_right()

    def di(self):
        """
        Returns the di value of the wind grid

        Returns:
            The di value of the wind grid
        """
        return self.__wg.di()

    def dj(self):
        """
        Returns the dj value of the wind grid

        Returns:
            The dj value of the wind grid
        """
        return self.__wg.dj()

    def rotation(self):
        """
        Returns the rotation of the wind grid

        Returns:
            The rotation of the wind grid
        """
        return self.__wg.rotation()

    @staticmethod
    def predefined_domain(predefined_domain_name: str):
        """
        Returns the predefined domain with the given name

        Args:
            predefined_domain_name: The name of the predefined domain

        Returns:
            The predefined domain with the given name
        """
        predefined_domain_name = predefined_domain_name.lower()
        if predefined_domain_name == "wnat":
            return -98, 10, -60, 45, 0.25, 0.25
        elif predefined_domain_name == "gom":
            return -98, 10, -75, 30, 0.25, 0.25
        elif predefined_domain_name == "global":
            return -180.0, -90.0, 180.0, 90.0, 0.25, 0.25
        raise RuntimeError("No matching predefined domain found")

    def __construct(self):
        """
        Constructs the wind grid
        """
        xinit = None
        yinit = None
        xend = None
        yend = None
        rotation = None
        dx = None
        dy = None
        nx = None
        ny = None
        if self.__json:
            if "x_init" in self.__json.keys():
                xinit = self.__json["x_init"]
            if "y_init" in self.__json.keys():
                yinit = self.__json["y_init"]
            if "x_end" in self.__json.keys():
                xend = self.__json["x_end"]
            if "y_end" in self.__json.keys():
                yend = self.__json["y_end"]
            if "rotation" in self.__json.keys():
                rotation = self.__json["rotation"]
            if "di" in self.__json.keys():
                dx = self.__json["di"]
            if "dj" in self.__json.keys():
                dy = self.__json["dj"]
            if "ni" in self.__json.keys():
                nx = self.__json["ni"]
            if "nj" in self.__json.keys():
                ny = self.__json["nj"]
            if "predefined_domain" in self.__json.keys():
                xinit, yinit, xend, yend, dx, dy = self.predefined_domain(
                    self.__json["predefined_domain"]
                )

        if xinit is None or yinit is None:
            raise RuntimeError("Lower left corner not specified")
        if (xend is None or yend is None) and (nx is None or ny is None):
            raise RuntimeError("Must specify xur/yur or nx/ny")
        if (xend is not None or yend is not None) and (
            nx is not None or ny is not None
        ):
            raise RuntimeError("Cannot specify both xur/yur and nx/ny")
        if (xend is not None and yend is None) or (yend is not None and xend is None):
            raise RuntimeError("Must specify both of xur/yur")
        if (nx is not None and ny is not None) or (ny is not None and nx is None):
            raise RuntimeError("Must specify both of nx/ny")
        if rotation is not None and (xend is not None or yend is not None):
            raise RuntimeError("Cannot specify rotation with xur/yur")
        if rotation is not None and not (nx is not None and ny is not None):
            raise RuntimeError("Must specify nx/ny with rotation")
        if dx is None or dy is None:
            raise RuntimeError("Must specify dx/dy")

        if (
            xinit is not None
            and yinit is not None
            and xend is not None
            and yend is not None
        ):
            if xend - xinit <= 0:
                raise RuntimeError("x-init/x-end pair must be logical")
            if yend - yinit <= 0:
                raise RuntimeError("y-init/y-end pair must be logical")

            self.__nx = int((xend - xinit) / dx)
            self.__ny = int((yend - yinit) / dy)

            if not self.__no_construct:
                import pymetbuild

                self.__wg = pymetbuild.Grid(xinit, yinit, xend, yend, dx, dy)
        elif (
            xinit is not None
            and yinit is not None
            and nx is not None
            and ny is not None
        ):
            if nx <= 0:
                raise RuntimeError("nx must be greater than 0")
            if ny <= 0:
                raise RuntimeError("ny must be greater than 0")
            if rotation is not None:
                rotation = 0.0

            self.__nx = nx
            self.__ny = ny

            if not self.__no_construct:
                import pymetbuild

                self.__wg = pymetbuild.Grid(xinit, yinit, nx, ny, dx, dy, rotation)
