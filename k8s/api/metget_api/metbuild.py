class MetBuildRequest:
    def __init__(self, request_id: str, json_data: dict):
        from metget_api.metbuild.input import Input

        self.__json_data = json_data
        self.__request_id = request_id
        self.__input = Input(json_data, None, None, None, None)
        self.__error = []
