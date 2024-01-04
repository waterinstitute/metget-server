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

from .outputgrid import OutputGrid

PREDEFINED_DOMAINS = {
    "wnat": {
        "x_init": -126.0,
        "y_init": 23.0,
        "x_end": -66.0,
        "y_end": 50.0,
        "di": 0.25,
        "dj": 0.25,
    },
    "gom": {
        "x_init": -98.0,
        "y_init": 10.0,
        "x_end": -75.0,
        "y_end": 30.0,
        "di": 0.25,
        "dj": 0.25,
    },
    "global": {
        "x_init": -180.0,
        "y_init": -90.0,
        "x_end": 180.0,
        "y_end": 90.0,
        "di": 0.25,
        "dj": 0.25,
    },
}


def grid_factory(json_data: dict) -> OutputGrid:
    """
    A factory to create a meteorological output grid.

    Args:
        json_data (dict): The json data of the meteorological output grid.

    Returns:
        OutputGrid: The meteorological output grid.
    """

    if "predefined_domain" in json_data:
        domain_data = PREDEFINED_DOMAINS[json_data["predefined_domain"]]
        x_init = domain_data["x_init"]
        y_init = domain_data["y_init"]
        x_end = domain_data["x_end"]
        y_end = domain_data["y_end"]
        dx = domain_data["di"]
        dy = domain_data["dj"]
    else:
        x_init = json_data["x_init"]
        y_init = json_data["y_init"]
        x_end = json_data["x_end"]
        y_end = json_data["y_end"]
        dx = json_data["di"]
        dy = json_data["dj"]

    return OutputGrid(
        x_lower_left=x_init,
        y_lower_left=y_init,
        x_upper_right=x_end,
        y_upper_right=y_end,
        x_resolution=dx,
        y_resolution=dy,
    )
