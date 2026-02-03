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

import os
import uuid
from datetime import datetime
from typing import ClassVar, Tuple

import libmetget.version
import sqlalchemy
from flask import Flask, Response, jsonify, make_response, redirect, request
from flask_cors import CORS
from flask_healthz import HealthError, healthz
from flask_limiter import Limiter, RequestLimit
from flask_limiter.util import get_remote_address
from flask_restful import Api, Resource
from libmetget.database.database import Database
from loguru import logger
from prometheus_flask_exporter.multiprocess import GunicornPrometheusMetrics

from .access_control import AccessControl
from .adeck import ADeck
from .check_request import CheckRequest
from .metbuildrequest import MetBuildRequest
from .status import Status
from .stormtrack import StormTrack

application = Flask(__name__)
api = Api(application)
limiter = Limiter(
    get_remote_address,
    app=application,
    storage_uri="memory://",
)

CORS(application)

# Prometheus metrics exporter - compatible with gunicorn multi-worker
# Metrics exposed at /metrics on the main app port
metrics = GunicornPrometheusMetrics(application, path="/metrics", group_by="endpoint")

# Loguru handles log levels automatically


def ratelimit_error_responder(request_limit: RequestLimit) -> Response:
    """
    This method is used to return a 429 error when the user has exceeded the
    rate limit.

    Args:
        request_limit: The request limit object

    Returns:
        A 429 error response

    """
    return make_response(jsonify({"error": "rate_limit_exceeded"}), 429)


@application.route("/")
def index() -> Response:
    """
    When the user hits the root path, redirect them to the MetGet homepage.
    """
    return redirect(location="http://thewaterinstitute.org", code=302)


class MetGetStatus(Resource):
    """
    This class is used to check the status of the MetGet API and see what
    data is currently available in the database.

    This is found at the /status path

    It may take url query arguments of:
        model: Name of the meteorological model to return. Default is 'all'
        limit: Maximum number of days worth of data to return. Default is 7
    """

    decorators: ClassVar = [
        limiter.limit("30/second", on_breach=ratelimit_error_responder)
    ]

    @staticmethod
    def get() -> Tuple[dict, int]:
        """
        This method is used to check the status of the MetGet API and see what
        data is currently available in the database.
        """
        authorized = AccessControl.check_authorization_token(request.headers)
        if authorized:
            s = Status()
            status_data, status_code = s.get_status(request)
            return {"statusCode": status_code, "body": status_data}, status_code
        return AccessControl.unauthorized_response()


class MetGetBuild(Resource):
    """
    This class is used to build a new MetGet request into a 2d wind field.

    This is found at the /build path
    """

    decorators: ClassVar = [
        limiter.limit("30/second", on_breach=ratelimit_error_responder)
    ]

    @staticmethod
    def post() -> Tuple[dict, int]:
        """
        This method is used to build a new MetGet request into a 2d wind field.
        """
        authorized = AccessControl.check_authorization_token(request.headers)
        if authorized:
            return MetGetBuild.__build()
        return AccessControl.unauthorized_response()

    @staticmethod
    def __build() -> Tuple[dict, int]:
        """
        This method is used to build a new MetGet request into a 2d wind field.
        """
        request_uuid = str(uuid.uuid4())
        request_api_key = request.headers.get("x-api-key")
        request_source_ip = request.environ.get(
            "HTTP_X_FORWARDED_FOR", request.remote_addr
        )
        request_json = request.get_json()

        request_obj = MetBuildRequest(
            request_api_key, request_source_ip, request_uuid, request_json
        )
        message, status_code = request_obj.generate_request()

        return message, status_code


class MetGetCheckRequest(Resource):
    """
    Allows users to check on the status of a request that is currently being built.

    The request is specified as a query string parameter "request-id" to the get method
    """

    decorators: ClassVar = [
        limiter.limit("30/second", on_breach=ratelimit_error_responder)
    ]

    @staticmethod
    def get() -> Tuple[dict, int]:
        authorized = AccessControl.check_authorization_token(request.headers)
        if authorized:
            c = CheckRequest()
            message, status = c.get(request)
            return message, status
        return AccessControl.unauthorized_response()


class MetGetADeck(Resource):
    """
    Allows the user to query storm tracks from the A-Deck database.

    This endpoint takes the following query parameters:
    - year - The year that the storm occurs
    - model - The model that the storm track is from
    - basin - The basin that the storm is in
    - storm - The storm number
    - cycle - The cycle of the storm track (i.e. datetime)
    """

    decorators: ClassVar = [
        limiter.limit("30/second", on_breach=ratelimit_error_responder)
    ]

    @staticmethod
    def get(
        year: str, basin: str, model: str, storm: str, cycle: str
    ) -> Tuple[dict, int]:
        """
        Method to handle the GET request for the ADeck endpoint.

        Args:
            year: The year that the storm occurs
            basin: The basin that the storm is in
            model: The model that the storm track is from
            storm: The storm number or 'all' for all active storms at the given cycle
            cycle: The cycle of the storm track (i.e. datetime)

        Returns:
            The response for the ADeck endpoint

        """
        authorized = AccessControl.check_authorization_token(
            request.headers, with_whitelist=True
        )
        if authorized:
            try:
                if isinstance(storm, str) and storm.lower() == "all":
                    storm = "all"
                else:
                    storm = int(storm)
                    if storm < 0 or storm > 99:
                        raise ValueError
            except ValueError:
                return {
                    "statusCode": 400,
                    "body": {"message": "Invalid storm number"},
                }, 400

            try:
                cycle = datetime.fromisoformat(cycle)
            except ValueError:
                return {
                    "statusCode": 400,
                    "body": {"message": "Invalid cycle format"},
                }, 400

            message, status_code = ADeck.get(year, basin, model, storm, cycle)
        else:
            message = AccessControl.unauthorized_response()
            status_code = 401

        return {"statusCode": status_code, "body": message}, status_code


class MetGetTrack(Resource):
    """
    Allows users to query a storm track in geojson format from the MetGet database.

    The endpoint takes the following query parameters:
        - advisory - The nhc advisory number
        - basin - The nhc basin (i.e. al, wp)
        - storm - The nhc storm number
        - type - Type of track to return (best or forecast)
        - year - The year that the storm occurs

    """

    decorators: ClassVar = [
        limiter.limit("30/second", on_breach=ratelimit_error_responder)
    ]

    @staticmethod
    def get() -> Tuple[dict, int]:
        # authorized = AccessControl.check_authorization_token(request.headers)
        # if authorized:
        #    return self.__get_storm_track()
        # else:
        #    return AccessControl.unauthorized_response()
        # ...We currently have the storm track endpoint without authorization so that
        # web portals can use freely. One day it can be locked up if desired using
        # the above

        message, status = StormTrack.get(request)
        return message, status


class MetGetCredits(Resource):
    """
    Allows the user to query the current credit balance for
    their API key. This endpoint uses the API key passed in
    with the header and takes no parameters.
    """

    @staticmethod
    def get() -> Tuple[dict, int]:
        authorized = AccessControl.check_authorization_token(request.headers)
        if authorized:
            user_token = request.headers.get("x-api-key")
            credit_data = AccessControl.get_credit_balance(user_token)
            if credit_data["credit_limit"] == 0.0:
                credit_data["credit_balance"] = 0.0
            return {"statusCode": 200, "body": credit_data}, 200
        return AccessControl.unauthorized_response()


class MetGetDashboard(Resource):
    """
    Serves the MetGet status dashboard HTML interface.

    This dashboard provides a web interface to visualize meteorological
    data status by making calls to the existing status API endpoints.
    """

    @staticmethod
    def get() -> Response:
        """
        Serves the dashboard HTML page that displays real-time status
        of meteorological models and NHC data.

        This endpoint is publicly accessible to allow users to view the dashboard.
        The dashboard will need valid API credentials to fetch data from the status endpoints.
        """
        return MetGetDashboard._load_dashboard_html()

    @staticmethod
    def _load_dashboard_html() -> Response:
        """Load and return the dashboard HTML template."""
        template_path = os.path.join(
            os.path.dirname(__file__), "templates", "dashboard.html"
        )
        try:
            with open(template_path, encoding="utf-8") as f:
                dashboard_html = f.read()
            return Response(dashboard_html, mimetype="text/html")
        except FileNotFoundError:
            error_html = "<html><body><h1>Dashboard template not found</h1><p>Please ensure dashboard.html exists in the templates directory.</p></body></html>"
            return Response(error_html, mimetype="text/html", status=404)


def health_ready() -> None:
    """
    This method is used to check the readiness of the MetGet API.

    This function checks:
        - The database connection
        - More, to be added later
    """
    try:
        db = Database()
        session = db.session()
        session.execute(sqlalchemy.text("SELECT 1"))
    except Exception as e:
        logger.error("HealthCheck failed at database connection: " + str(e))
        msg = "Database connection failed: " + str(e)
        raise HealthError(msg) from e


# ...Add the resources to the API
api.add_resource(MetGetStatus, "/status")
api.add_resource(MetGetBuild, "/build")
api.add_resource(MetGetCheckRequest, "/check")
api.add_resource(MetGetTrack, "/stormtrack")
api.add_resource(
    MetGetADeck,
    "/adeck/<string:year>/<string:basin>/<string:model>/<string:storm>/<string:cycle>",
)
api.add_resource(MetGetCredits, "/credits")
api.add_resource(MetGetDashboard, "/dashboard")

# ...Add the health check
HEALTHZ = {
    "live": lambda: None,
    "ready": application.name + ".health_ready",
}
application.register_blueprint(healthz, url_prefix="/healthz")
application.config.update(HEALTHZ=HEALTHZ)
logger.info(
    f"Running metget-server API version: {libmetget.version.get_metget_version():s}"
)


if __name__ == "__main__":
    """
    If the script is run directly, start the application server
    using flask's built-in server. This is for testing purposes
    only and should not be used in production.
    """
    application.run(host="0.0.0.0", port=8080)
