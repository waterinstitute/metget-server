###################################################################################################
# Shared pytest configuration. Several libmetget modules read environment variables at import
# time (e.g. the database table names in tables.py), so harmless defaults are provided here to
# allow the test suite to run without a deployed environment. Variables which are already set
# externally are not overridden.
###################################################################################################
import os

_TEST_ENVIRONMENT_DEFAULTS = {
    "METGET_API_KEY_TABLE": "apikeys",
    "METGET_REQUEST_TABLE": "requests",
    "METGET_S3_BUCKET": "metget-test-bucket",
    "METGET_S3_BUCKET_UPLOAD": "metget-test-bucket-upload",
    "METGET_DATABASE": "metget",
    "METGET_DATABASE_USER": "metget",
    "METGET_DATABASE_PASSWORD": "metget",
}

for _key, _value in _TEST_ENVIRONMENT_DEFAULTS.items():
    os.environ.setdefault(_key, _value)
