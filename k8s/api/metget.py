#!/usr/bin/env python3

from flask import Flask, redirect, request, make_response, jsonify
from flask_restful import Resource, Api
from flask_limiter import Limiter, RequestLimit
from flask_limiter.util import get_remote_address
from flask_cors import CORS
from metget_api.access_control import AccessControl

application = Flask(__name__)
api = Api(application)
limiter = Limiter(
    get_remote_address,
    app=application,
    storage_uri="memory://",
)

CORS(application)


def ratelimit_error_responder(request_limit: RequestLimit):
    """
    This method is used to return a 429 error when the user has exceeded the
    rate limit

    Args:
        request_limit: The request limit object

    Returns:
        A 429 error response
    """
    return make_response(jsonify({"error": "rate_limit_exceeded"}), 429)


@application.route("/")
def index():
    """
    When the user hits the root path, redirect them to the MetGet homepage
    """
    return redirect(location="http://metget.org", code=302)


class MetGetStatus(Resource):
    """
    This class is used to check the status of the MetGet API and see what
    data is currently available in the database

    This is found at the /status path

    It may take url query arguments of:
        model: Name of the meteorolgoical model to return. Default is 'all'
        limit: Maximum number of days worth of data to return. Default is 7
    """

    decorators = [limiter.limit("10/second", on_breach=ratelimit_error_responder)]

    @staticmethod
    def get():
        """
        This method is used to check the status of the MetGet API and see what
        data is currently available in the database
        """
        authorized = AccessControl.check_authorization_token(request.headers)
        if authorized:
            from metget_api.status import Status

            s = Status()
            status_data, status_code = s.get_status(request)
            return {"statusCode": status_code, "body": status_data}, status_code
        else:
            return AccessControl.unauthorized_response()


class MetGetBuild(Resource):
    """
    This class is used to build a new MetGet request into a 2d wind field

    This is found at the /build path
    """

    decorators = [limiter.limit("10/second", on_breach=ratelimit_error_responder)]

    @staticmethod
    def post():
        """
        This method is used to build a new MetGet request into a 2d wind field
        """
        authorized = AccessControl.check_authorization_token(request.headers)
        if authorized:
            return MetGetBuild.__build()
        else:
            return AccessControl.unauthorized_response()

    @staticmethod
    def __build():
        """
        This method is used to build a new MetGet request into a 2d wind field
        """
        import uuid

        from metget_api.metbuildrequest import MetBuildRequest

        request_uuid = str(uuid.uuid4())
        request_api_key = request.headers.get("x-api-key")
        request_source_ip = request.remote_addr
        request_json = request.get_json()

        request_obj = MetBuildRequest(
            request_api_key, request_source_ip, request_uuid, request_json
        )
        message, status_code = request_obj.generate_request()

        return message, status_code


class MetGetCheckRequest(Resource):
    """
    Allows users to check on the status of a request that is currently being built

    The request is specified as a query string parameter "request-id" to the get method
    """

    decorators = [limiter.limit("10/second", on_breach=ratelimit_error_responder)]

    @staticmethod
    def get():
        authorized = AccessControl.check_authorization_token(request.headers)
        if authorized:
            from metget_api.check_request import CheckRequest

            c = CheckRequest()
            message, status = c.get(request)
            return message, status
        else:
            return AccessControl.unauthorized_response()


class MetGetTrack(Resource):
    """
    Allows users to query a storm track in geojson format from the MetGet database

    The endpoint takes the following query parameters:
        - advisory - The nhc advisory number
        - basin - The nhc basin (i.e. al, wp)
        - storm - The nhc storm number
        - type - Type of track to return (best or forecast)
        - year - The year that the storm occurs

    """

    decorators = [limiter.limit("10/second", on_breach=ratelimit_error_responder)]

    @staticmethod
    def get():
        from metget_api.stormtrack import StormTrack

        # authorized = AccessControl.check_authorization_token(request.headers)
        # if authorized:
        #    return self.__get_storm_track()
        # else:
        #    return AccessControl.unauthorized_response()
        # ...We currently have the stormtrack endpoint without authorization so that
        # web portals can use freely. One day it can be locked up if desired using
        # the above

        s = StormTrack()
        message, status = s.get(request)
        return message, status


# ...Add the resources to the API
api.add_resource(MetGetStatus, "/status")
api.add_resource(MetGetBuild, "/build")
api.add_resource(MetGetCheckRequest, "/check")
api.add_resource(MetGetTrack, "/stormtrack")

if __name__ == "__main__":
    """
    If the script is run directly, start the application server
    using flask's built-in server. This is for testing purposes
    only and should not be used in production.
    """
    application.run(host="0.0.0.0", port=5000)
