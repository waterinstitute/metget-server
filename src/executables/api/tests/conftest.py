###################################################################################################
# Pytest configuration for the metget_api tests.
#
# Importing libmetget.database.tables (a transitive import of metget_api.adeck)
# reads a couple of table names from the environment at import time. Set dummy
# values here, before any test module is collected, so the import succeeds
# without a live database.
###################################################################################################
import os

os.environ.setdefault("METGET_API_KEY_TABLE", "apikey")
os.environ.setdefault("METGET_REQUEST_TABLE", "request")
