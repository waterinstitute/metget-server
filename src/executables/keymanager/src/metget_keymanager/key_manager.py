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
from datetime import datetime

CREDIT_MULTIPLIER = 100000.0


def add_key(
    key: str, user: str, description: str, api_credits: int, expiration_date: datetime
):
    """
    Add a new key to the database

    Args:
        key (str): The key to add. If not provided, a key will be generated
        user (str): The user associated with the key
        description (str): The description of the key
        api_credits (int): The number of monthly credits to assign to the key
        expiration_date (datetime): Date when the key expires

    Returns:
        None
    """
    import secrets

    from libmetget.database.database import Database
    from libmetget.database.tables import AuthTable

    # Generate a url safe token 40 characters long without any special characters
    # if a key was not provided
    if key:
        token = key
    else:
        token = secrets.token_urlsafe(80).replace("-", "").replace("_", "")[:40]

    if len(token) != 40:
        msg = "Token is not 40 characters long"
        raise ValueError(msg)

    if not user:
        msg = "No user name specified"
        raise ValueError(msg)

    if not description:
        msg = "No user description specified"
        raise ValueError(msg)

    if not api_credits:
        msg = "No credit count specified"
        raise ValueError(msg)

    if not expiration_date:
        msg = "No expiration date specified"
        raise ValueError(msg)

    # Add the key to the database
    with Database() as db, db.session() as session:
        session.add(
            AuthTable(
                key=token,
                username=user,
                description=description,
                enabled=True,
                credit_limit=int(float(api_credits) * CREDIT_MULTIPLIER),
                expiration=expiration_date,
            )
        )
        session.commit()

    # Print the key to the user
    print(f"The key for user {user} is '{token}'")


def update_key(
    key: str, user: str, description: str, api_credits: int, expiration_date: datetime
):
    """
    Update an existing key in the database

    Args:
        key (str): The key to update
        user (str): The user associated with the key
        description (str): The description of the key
        api_credits (int): The number of monthly credits to assign to the key
        expiration_date (datetime): Date when the key expires

    Returns:
        None
    """
    from libmetget.database.database import Database
    from libmetget.database.tables import AuthTable

    with Database() as db, db.session() as session:
        current_key = session.query(AuthTable).filter_by(key=key).first()

        if not current_key:
            msg = "Key does not exist"
            raise Exception(msg)

        if user:
            current_key.username = user
        if description:
            current_key.description = description
        if api_credits:
            current_key.credit_limit = int(float(api_credits) * CREDIT_MULTIPLIER)
        if expiration_date:
            current_key.expiration = expiration_date
        session.commit()


def enable_disable_key(key: str, enabled: bool):
    """
    Enable or disable an existing key in the database

    Args:
        key (str): The key to enable or disable
        enabled (bool): Whether to enable or disable the key

    Returns:
        None
    """
    from libmetget.database.database import Database
    from libmetget.database.tables import AuthTable

    with Database() as db, db.session() as session:
        current_key = session.query(AuthTable).filter_by(key=key).first()

        if not current_key:
            msg = "Key does not exist"
            raise Exception(msg)

        current_key.enabled = enabled

        session.commit()


def remove_key(key: str):
    """
    Remove an existing key from the database

    Args:
        key (str): The key to remove
    """

    from libmetget.database.database import Database
    from libmetget.database.tables import AuthTable

    with Database() as db, db.session() as session:
        current_key = session.query(AuthTable).filter_by(key=key).first()

        if not current_key:
            msg = "Key does not exist"
            raise Exception(msg)

        session.delete(current_key)
        session.commit()


def list_keys():
    """
    List all keys in the database
    """
    from datetime import datetime, timedelta

    import prettytable
    from libmetget.database.database import Database
    from libmetget.database.tables import AuthTable, RequestTable
    from sqlalchemy import func, or_

    with Database() as db, db.session() as session:
        table = prettytable.PrettyTable()
        table.field_names = [
            "Key",
            "User",
            "Description",
            "Enabled",
            "Expiration",
            "Credit Limit",
            "Credits Used",
            "Credit Balance",
        ]

        auth_keys = session.query(
            AuthTable.key,
            AuthTable.username,
            AuthTable.description,
            AuthTable.enabled,
            AuthTable.expiration,
            AuthTable.credit_limit,
        ).all()

        for key in auth_keys:
            start_date = datetime.utcnow() - timedelta(days=30)
            credit_used = (
                session.query(func.sum(RequestTable.credit_usage))
                .filter(RequestTable.last_date >= start_date)
                .filter(RequestTable.api_key == key.key.strip())
                .filter(
                    or_(
                        RequestTable.status == "completed",
                        RequestTable.status == "running",
                    )
                )
                .first()[0]
            )

            if credit_used:
                credit_used = float(credit_used) / CREDIT_MULTIPLIER
            else:
                credit_used = 0.0

            if key.credit_limit == 0:
                credit_limit = "Unlimited"
                credit_balance = "Unlimited"
            else:
                credit_limit = float(key.credit_limit) / CREDIT_MULTIPLIER
                credit_balance = float(credit_limit - credit_used)

            table.add_row(
                [
                    key.key,
                    key.username,
                    key.description,
                    key.enabled,
                    key.expiration,
                    credit_limit,
                    credit_used,
                    credit_balance,
                ]
            )

    print(table)


def key_manager():
    """
    Main function
    """
    import argparse

    from libmetget.version import get_metget_version

    parser = argparse.ArgumentParser(description="MetGet APIKey Manager")
    parser.add_argument(
        "command",
        help="Command to execute",
        choices=["add", "update", "enable", "disable", "remove", "list"],
    )
    parser.add_argument("--key", help="Name of the key to operate on")
    parser.add_argument("--user", help="User associated with the key")
    parser.add_argument("--description", help="Description associated with the key")
    parser.add_argument(
        "--credits", help="Number of monthly credits to assign to the key"
    )
    parser.add_argument(
        "--expires", type=datetime.fromisoformat, help="Expiration date of the key"
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"MetGet APIKey Manager Version: {get_metget_version()}",
    )

    args = parser.parse_args()

    if args.command == "add":
        add_key(args.key, args.user, args.description, args.credits, args.expires)
    elif args.command == "update":
        update_key(args.key, args.user, args.description, args.credits, args.expires)
    elif args.command == "enable":
        enable_disable_key(args.key, True)
    elif args.command == "disable":
        enable_disable_key(args.key, False)
    elif args.command == "remove":
        remove_key(args.key)
    elif args.command == "list":
        list_keys()


if __name__ == "__main__":
    key_manager()
