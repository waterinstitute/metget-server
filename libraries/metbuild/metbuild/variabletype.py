from enum import Enum


class VariableType(Enum):
    """Enum class for the type of meteorological variable"""

    WIND_PRESSURE = 1
    PRESSURE = 2
    WIND = 3
    PRECIPITATION = 4
    TEMPERATURE = 5
    HUMIDITY = 6
    ICE = 7

    @staticmethod
    def from_string(data_type: str):
        """
        Converts a string to a VariableType

        Args:
            data_type: The string to convert to a VariableType

        Returns:
            The VariableType corresponding to the string
        """
        if data_type == "wind_pressure":
            return VariableType.WIND_PRESSURE
        elif data_type == "pressure":
            return VariableType.PRESSURE
        elif data_type == "wind":
            return VariableType.WIND
        elif data_type == "precipitation" or data_type == "rain":
            return VariableType.PRECIPITATION
        elif data_type == "temperature":
            return VariableType.TEMPERATURE
        elif data_type == "humidity":
            return VariableType.HUMIDITY
        elif data_type == "ice":
            return VariableType.ICE
        else:
            raise ValueError("Invalid data type: {:s}".format(data_type))
