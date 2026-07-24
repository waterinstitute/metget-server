###################################################################################################
# Pytest configuration for the metget_build tests.
#
# Importing metget_build.message_handler transitively imports libmetget.database.tables, which
# reads a couple of table names from the environment at import time (and message_handler itself
# reads METGET_S3_BUCKET/METGET_S3_BUCKET_UPLOAD at call time). Set harmless defaults here, before
# any test module is collected, so the import succeeds without a live database or deployed
# environment. Variables already set externally are not overridden. Mirrors
# src/executables/api/tests/conftest.py and src/libraries/libmetget/tests/conftest.py.
###################################################################################################
import os

_TEST_ENVIRONMENT_DEFAULTS = {
    "METGET_API_KEY_TABLE": "apikeys",
    "METGET_REQUEST_TABLE": "requests",
    "METGET_S3_BUCKET": "metget-test-bucket",
    "METGET_S3_BUCKET_UPLOAD": "metget-test-bucket-upload",
}

for _key, _value in _TEST_ENVIRONMENT_DEFAULTS.items():
    os.environ.setdefault(_key, _value)
