from metbuild.output.outputgrid import OutputGrid
from metbuild.variabletype import VariableType
from metbuild.output.dataset import Dataset
from metbuild.meteorologicalsource import MeteorologicalSource
from metbuild.interpolator import Interpolator

class Meteorology:
    def __init__(
        self,
        grid: OutputGrid,
        source_key: MeteorologicalSource,
        data_type_key: VariableType,
        backfill: bool,
        epsg: int,
    ):
        self.__grid = grid
        self.__source_key = source_key
        self.__data_type_key = data_type_key
        self.__backfill = backfill
        self.__epsg = epsg
        self.__file_1 = None
        self.__file_2 = None
        self.__interpolation_1 = None
        self.__interpolation_2 = None

    def grid(self) -> OutputGrid:
        return self.__grid

    def source_key(self) -> MeteorologicalSource:
        return self.__source_key

    def data_type_key(self) -> VariableType:
        return self.__data_type_key

    def backfill(self) -> bool:
        return self.__backfill

    def epsg(self) -> int:
        return self.__epsg

    def set_next_file(self, filename: str) -> None:
        if self.__file_1 is None:
            self.__file_1 = filename
        elif self.__file_2 is None:
            self.__file_2 = filename
        else:
            self.__file_1 = self.__file_2
            self.__file_2 = filename


