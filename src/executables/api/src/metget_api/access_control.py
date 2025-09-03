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
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Tuple

from flask import request
from libmetget.database.database import Database
from libmetget.database.tables import AuthTable, RequestTable
from sqlalchemy import func, or_

CREDIT_MULTIPLIER = 100000.0

# ...List of whitelisted domains that can bypass the API key
WHITELISTED_DOMAINS = [".floodid.org", "localhost:5173"]


class AccessControl:
    """
    This class is used to check if the user is authorized to use the API.
    """

    def __init__(self) -> None:
        pass

    @staticmethod
    def hash_access_token(token: str) -> str:
        """
        This method is used to hash the access token before comparison.
        """
        return sha256(token.encode()).hexdigest()

    @staticmethod
    def check_whitelisted_domain() -> bool:
        """
        Checks if the request is coming from a whitelisted domain. Whitelisted
        domains can bypass the API key check for some functions.

        Returns:
            bool: True if the request is coming from a whitelisted domain and False if not

        """
        referrer = request.headers.get("Referer")
        if referrer is None:
            return False
        return any(domain in referrer for domain in WHITELISTED_DOMAINS)

    @staticmethod
    def is_authorized(api_key: str, with_whitelist: bool = False) -> bool:
        """
        This method is used to check if the user is authorized to use the API
        The method returns True if the user is authorized and False if not
        Keys are hashed before being compared to the database to prevent
        accidental exposure of the keys and/or sql injection.

        Args:
            api_key (str): The API key used to authenticate the request
            with_whitelist (bool): A boolean to determine if the whitelist should be checked

        Returns:
            bool: True if the user is authorized and False if not

        """
        if with_whitelist:
            whitelist_authorized = AccessControl.check_whitelisted_domain()
            if whitelist_authorized:
                return True
        elif api_key is None or api_key == "":
            return False

        with Database() as db, db.session() as session:
            api_key_db = (
                session.query(
                    AuthTable.id,
                    AuthTable.key,
                )
                .filter(AuthTable.key == api_key)
                .filter(AuthTable.enabled == True)  # noqa: E712
                .filter(AuthTable.expiration >= datetime.now(timezone.utc))
                .first()
            )

        if api_key_db is None:
            return False

        api_key_hash = AccessControl.hash_access_token(str(api_key))
        api_key_db_hash = AccessControl.hash_access_token(api_key_db.key.strip())

        return bool(api_key_db_hash == api_key_hash)

    @staticmethod
    def check_authorization_token(headers: dict, with_whitelist: bool = False) -> bool:
        """
        This method is used to check if the user is authorized to use the API
        The method returns True if the user is authorized and False if not.

        Args:
            headers (dict): The headers from the request
            with_whitelist (bool): A boolean to determine if the whitelist should be checked

        Returns:
            bool: True if the user is authorized and False if not

        """
        user_token = headers.get("x-api-key")
        gatekeeper = AccessControl()
        return bool(gatekeeper.is_authorized(user_token, with_whitelist))

    @staticmethod
    def unauthorized_response() -> Tuple[dict, int]:
        status = 401
        return {
            "statusCode": status,
            "body": {"message": "ERROR: Unauthorized"},
        }, status

    @staticmethod
    def get_credit_balance(api_key: str) -> dict:
        """
        This method is used to get the credit balance for the user.

        Args:
            api_key (str): The API key used to authenticate the request

        Returns:
            dict: A dictionary containing the credit limit, credits used, and credit balance

        """
        with Database() as db, db.session() as session:
            # ...Queries the database for the credit limit for the user
            credit_limit = (
                session.query(AuthTable.credit_limit).filter_by(key=api_key).first()[0]
            )
            credit_limit = float(credit_limit)

            # ...Queries the database for the credit used for the user over the last 30 days
            start_date = datetime.now(timezone.utc) - timedelta(days=30)
            credit_used = (
                session.query(func.sum(RequestTable.credit_usage))
                .filter(RequestTable.last_date >= start_date)
                .filter(RequestTable.api_key == api_key)
                .filter(
                    or_(
                        RequestTable.status == "completed",
                        RequestTable.status == "running",
                    )
                )
                .first()[0]
            )

        if credit_used is None:
            credit_used = 0.0
        else:
            credit_used = float(credit_used) / CREDIT_MULTIPLIER

        credit_balance = credit_limit - credit_used

        return {
            "credit_limit": credit_limit,
            "credits_used": credit_used,
            "credit_balance": credit_balance,
        }
