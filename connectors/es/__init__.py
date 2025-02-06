#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import warnings

from connectors.es.client import ESClient  # NOQA
from connectors.es.document import ESDocument, InvalidDocumentSourceError  # NOQA
from connectors.es.index import ESIndex  # NOQA

from elasticsearch.exceptions import GeneralAvailabilityWarning

warnings.filterwarnings("ignore", category=GeneralAvailabilityWarning)

TIMESTAMP_FIELD = "_timestamp"
DEFAULT_LANGUAGE = "en"
