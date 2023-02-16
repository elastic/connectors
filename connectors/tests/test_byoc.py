#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import json
import os
from copy import deepcopy
from datetime import datetime
from unittest import mock
from unittest.mock import ANY, AsyncMock, Mock, call, patch

import pytest

from connectors.byoc import (
    CONNECTORS_INDEX,
    IDLE_JOBS_THRESHOLD,
    Connector,
    ConnectorIndex,
    Features,
    Filter,
    Filtering,
    JobStatus,
    JobTriggerMethod,
    Pipeline,
    Status,
    SyncJob,
    SyncJobIndex,
    iso_utc,
)
from connectors.config import load_config
from connectors.filtering.validation import (
    FilteringValidationResult,
    FilteringValidationState,
    InvalidFilteringError,
    ValidationTarget,
)
from connectors.source import BaseDataSource
from connectors.tests.commons import AsyncIterator

CONFIG = os.path.join(os.path.dirname(__file__), "config.yml")

DEFAULT_DOMAIN = "DEFAULT"

DRAFT_ADVANCED_SNIPPET = {"value": {"query": {"options": {}}}}

DRAFT_RULE_ONE_ID = 1
DRAFT_RULE_TWO_ID = 2

ACTIVE_ADVANCED_SNIPPET = {"value": {"find": {"settings": {}}}}

ACTIVE_RULE_ONE_ID = 3
ACTIVE_RULE_TWO_ID = 4

ACTIVE_FILTER_STATE = "active"
DRAFT_FILTER_STATE = "draft"

FILTERING_VALIDATION_VALID = {"state": "valid", "errors": []}
FILTERING_VALIDATION_INVALID = {"state": "invalid", "errors": []}
FILTERING_VALIDATION_EDITED = {"state": "edited", "errors": []}

FILTER_VALIDATION_STATE_VALID = {
    "advanced_snippet": ACTIVE_ADVANCED_SNIPPET,
    "rules": [{"id": DRAFT_RULE_ONE_ID}],
    "validation": FILTERING_VALIDATION_VALID,
}

FILTER_VALIDATION_STATE_INVALID = FILTER_VALIDATION_STATE_VALID | {
    "validation": FILTERING_VALIDATION_INVALID
}
FILTER_VALIDATION_STATE_EDITED = FILTER_VALIDATION_STATE_VALID | {
    "validation": FILTERING_VALIDATION_EDITED
}

DRAFT_FILTERING_DEFAULT_DOMAIN = {
    "advanced_snippet": DRAFT_ADVANCED_SNIPPET,
    "rules": [{"id": DRAFT_RULE_ONE_ID}, {"id": DRAFT_RULE_TWO_ID}],
    "validation": FILTERING_VALIDATION_VALID,
}

ACTIVE_FILTERING_DEFAULT_DOMAIN = {
    "advanced_snippet": ACTIVE_ADVANCED_SNIPPET,
    "rules": [{"id": ACTIVE_RULE_ONE_ID}, {"id": ACTIVE_RULE_TWO_ID}],
    "validation": FILTERING_VALIDATION_VALID,
}

FILTERING_VALIDATION_DEFAULT_DOMAIN_WITH_ERRORS = {
    "state": "invalid",
    "errors": [
        {"ids": ["1"], "errors": ["some error"]},
        {"ids": ["2"], "errors": ["another error"]},
    ],
}

FILTERING_DEFAULT_DOMAIN_DRAFT_AFTER_UPDATE = {
    "domain": DEFAULT_DOMAIN,
    "draft": DRAFT_FILTERING_DEFAULT_DOMAIN
    | {"validation": FILTERING_VALIDATION_DEFAULT_DOMAIN_WITH_ERRORS},
    "active": ACTIVE_FILTERING_DEFAULT_DOMAIN,
}

FILTERING_DEFAULT_DOMAIN_ACTIVE_AFTER_UPDATE = {
    "domain": DEFAULT_DOMAIN,
    "draft": DRAFT_FILTERING_DEFAULT_DOMAIN,
    "active": ACTIVE_FILTERING_DEFAULT_DOMAIN
    | {"validation": FILTERING_VALIDATION_DEFAULT_DOMAIN_WITH_ERRORS},
}

FILTERING_DEFAULT_DOMAIN = {
    "domain": DEFAULT_DOMAIN,
    "draft": DRAFT_FILTERING_DEFAULT_DOMAIN,
    "active": ACTIVE_FILTERING_DEFAULT_DOMAIN,
}

FILTERING_OTHER_DOMAIN = {
    "domain": "other",
    "draft": {},
    "active": {},
    "validation": {
        "state": "invalid",
        "errors": [{"ids": ["1"], "messages": ["some messages"]}],
    },
}

CONNECTOR_ID = 1

DOC_SOURCE_FILTERING = [FILTERING_DEFAULT_DOMAIN, FILTERING_OTHER_DOMAIN]

DOC_SOURCE = {
    "configuration": {"key": "value"},
    "description": "description",
    "error": "none",
    "features": {},
    "filtering": DOC_SOURCE_FILTERING,
    "index_name": "search-index",
    "name": "MySQL",
    "pipeline": {},
    "scheduling": {},
    "service_type": "SERVICE",
    "status": "connected",
    "language": "en",
    "sync_now": False,
}

EXPECTED_FILTERING_AFTER_UPDATE_DRAFT = {
    "filtering": [FILTERING_DEFAULT_DOMAIN_DRAFT_AFTER_UPDATE, FILTERING_OTHER_DOMAIN]
}

EXPECTED_FILTERING_AFTER_UPDATE_ACTIVE = {
    "filtering": [FILTERING_DEFAULT_DOMAIN_ACTIVE_AFTER_UPDATE, FILTERING_OTHER_DOMAIN]
}

OTHER_DOMAIN_ONE = "other-domain-1"
OTHER_DOMAIN_TWO = "other-domain-2"
NON_EXISTING_DOMAIN = "non-existing-domain"

EMPTY_FILTER = Filter()

FILTERING = [
    {
        "domain": Filtering.DEFAULT_DOMAIN,
        "draft": DRAFT_FILTERING_DEFAULT_DOMAIN,
        "active": ACTIVE_FILTERING_DEFAULT_DOMAIN,
        "validation": FILTERING_VALIDATION_VALID,
    },
    {
        "domain": OTHER_DOMAIN_ONE,
        "draft": {},
        "active": {},
        "validation": FILTERING_VALIDATION_VALID,
    },
    {
        "domain": OTHER_DOMAIN_TWO,
        "draft": {},
        "active": {},
        "validation": FILTERING_VALIDATION_VALID,
    },
]

EMPTY_FILTERING = Filter()

ADVANCED_RULES_EMPTY = {"advanced_snippet": {}}

ADVANCED_RULES = {"db": {"table": "SELECT * FROM db.table"}}

ADVANCED_RULES_NON_EMPTY = {"advanced_snippet": ADVANCED_RULES}

RULES = [
    {
        "id": 1,
    }
]
BASIC_RULES_NON_EMPTY = {"rules": RULES}
ADVANCED_AND_BASIC_RULES_NON_EMPTY = {
    "advanced_snippet": {"db": {"table": "SELECT * FROM db.table"}},
    "rules": RULES,
}


@pytest.fixture(autouse=True)
def patch_validate_filtering_in_byoc():
    with mock.patch(
        "connectors.byoc.SyncJob.validate_filtering", return_value=AsyncMock()
    ) as validate_filtering_mock:
        yield validate_filtering_mock


def test_utc():
    # All dates are in ISO 8601 UTC so we can serialize them
    now = datetime.utcnow()
    then = json.loads(json.dumps({"date": iso_utc(when=now)}))["date"]
    assert now.isoformat() == then


mongo = {
    "api_key_id": "",
    "configuration": {
        "host": {"value": "mongodb://127.0.0.1:27021", "label": "MongoDB Host"},
        "database": {"value": "sample_airbnb", "label": "MongoDB Database"},
        "collection": {
            "value": "listingsAndReviews",
            "label": "MongoDB Collection",
        },
    },
    "index_name": "search-airbnb",
    "service_type": "mongodb",
    "status": "configured",
    "language": "en",
    "last_sync_status": None,
    "last_sync_error": "",
    "last_synced": "",
    "last_seen": "",
    "created_at": "",
    "updated_at": "",
    "scheduling": {"enabled": True, "interval": "0 * * * *"},
    "sync_now": True,
}


@pytest.mark.asyncio
async def test_heartbeat(mock_responses, patch_logger):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        payload={
            "hits": {"hits": [{"_id": "1", "_source": mongo}], "total": {"value": 1}}
        },
        headers=headers,
    )

    for _ in range(10):
        mock_responses.put(
            "http://nowhere.com:9200/.elastic-connectors/_doc/1",
            payload={"_id": "1"},
            headers=headers,
        )

    connectors = ConnectorIndex(config)
    conns = []

    async for connector in connectors.supported_connectors(
        native_service_types=["mongodb"]
    ):
        connector.start_heartbeat(0.2)
        connector.start_heartbeat(1.0)  # NO-OP
        conns.append(connector)

    await asyncio.sleep(0.4)
    await conns[0].close()
    await connectors.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "native_service_types, connector_ids, expected_connector_count",
    [
        ([], [], 0),
        (["mongodb"], [], 1),
        ([], ["1"], 1),
        (["mongodb"], ["1"], 1),
    ],
)
async def test_supported_connectors(
    native_service_types, connector_ids, expected_connector_count, mock_responses
):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    native_connectors_query = {
        "bool": {
            "filter": [
                {"term": {"is_native": True}},
                {"terms": {"service_type": native_service_types}},
            ]
        }
    }

    custom_connectors_query = {
        "bool": {
            "filter": [
                {"term": {"is_native": False}},
                {"terms": {"_id": connector_ids}},
            ]
        }
    }

    if len(native_service_types) > 0 and len(connector_ids) > 0:
        query = {"bool": {"should": [native_connectors_query, custom_connectors_query]}}
    elif len(native_service_types) > 0:
        query = native_connectors_query
    elif len(connector_ids) > 0:
        query = custom_connectors_query
    else:
        query = {}

    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        body={"query": query},
        payload={
            "hits": {"hits": [{"_id": "1", "_source": mongo}], "total": {"value": 1}}
        },
        headers=headers,
    )

    connector_index = ConnectorIndex(config)
    connectors = [
        connector
        async for connector in connector_index.supported_connectors(
            native_service_types=native_service_types, connector_ids=connector_ids
        )
    ]
    await connector_index.close()

    assert len(connectors) == expected_connector_count
    if expected_connector_count > 0:
        assert connectors[0].service_type == mongo["service_type"]


@pytest.mark.asyncio
async def test_all_connectors(mock_responses):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        payload={
            "hits": {"hits": [{"_id": "1", "_source": mongo}], "total": {"value": 1}}
        },
        headers=headers,
    )

    connectors = ConnectorIndex(config)
    conns = []
    async for connector in connectors.all_connectors():
        conns.append(connector)
    await connectors.close()

    assert len(conns) == 1


class StubIndex:
    def __init__(self):
        self.client = None

    async def save(self, connector):
        pass


@pytest.mark.asyncio
async def test_properties(mock_responses):
    connector_src = {
        "service_type": "test",
        "index_name": "search-some-index",
        "configuration": {},
        "language": "en",
        "scheduling": {},
        "status": "created",
    }

    connector = Connector(StubIndex(), "test", connector_src, {})

    assert connector.status == Status.CREATED
    assert connector.service_type == "test"
    connector.service_type = "test2"
    assert connector.service_type == "test2"
    assert connector._dirty

    await connector.sync_doc()
    assert not connector._dirty

    # setting some config with a value that is None
    connector.configuration = {"cool": {"value": "foo"}, "cool2": {"value": None}}

    assert connector.status == Status.NEEDS_CONFIGURATION

    # setting some config
    connector.configuration = {"cool": {"value": "foo"}, "cool2": {"value": "baz"}}

    assert connector.status == Status.CONFIGURED

    with pytest.raises(TypeError):
        connector.status = 1234


@pytest.mark.asyncio
async def test_sync_job_properties():
    sync_job_src = {
        "_id": "test",
        "_source": {
            "status": "error",
            "error": "something wrong",
            "indexed_document_count": 10,
            "indexed_document_volume": 20,
            "deleted_document_count": 30,
            "total_document_count": 100,
            "connector": {
                "id": "connector_id",
                "service_type": "test",
                "index_name": "search-some-index",
                "configuration": {},
                "language": "en",
                "filtering": {},
                "pipeline": {},
            },
        },
    }

    index = Mock()
    sync_job = SyncJob(elastic_index=index, doc_source=sync_job_src)

    assert sync_job.id == "test"
    assert sync_job.status == JobStatus.ERROR
    assert sync_job.error == "something wrong"
    assert sync_job.connector_id == "connector_id"
    assert sync_job.service_type == "test"
    assert sync_job.configuration.is_empty()
    assert sync_job.index_name == "search-some-index"
    assert sync_job.language == "en"
    assert sync_job.indexed_document_count == 10
    assert sync_job.indexed_document_volume == 20
    assert sync_job.deleted_document_count == 30
    assert sync_job.total_document_count == 100
    assert isinstance(sync_job.filtering, Filter)
    assert isinstance(sync_job.pipeline, Pipeline)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "validation_result_state, validation_result_errors, should_raise_exception",
    [
        (
            FilteringValidationState.VALID,
            [],
            False,
        ),
        (
            FilteringValidationState.INVALID,
            ["something wrong"],
            True,
        ),
    ],
)
async def test_sync_job_validate_filtering(
    validation_result_state, validation_result_errors, should_raise_exception
):
    source = {"_id": "1"}
    index = Mock()
    validator = Mock()
    validation_result = FilteringValidationResult(
        state=validation_result_state, errors=validation_result_errors
    )
    validator.validate_filtering = AsyncMock(return_value=validation_result)

    sync_job = SyncJob(elastic_index=index, doc_source=source)

    try:
        await sync_job.validate_filtering(validator=validator)
    except InvalidFilteringError:
        assert should_raise_exception


@pytest.mark.asyncio
async def test_sync_job_claim(patch_logger):
    source = {"_id": "1"}
    index = Mock()
    index.update = AsyncMock(return_value=1)
    expected_doc_source_update = {
        "status": JobStatus.IN_PROGRESS.value,
        "started_at": ANY,
        "last_seen": ANY,
        "worker_hostname": ANY,
    }

    sync_job = SyncJob(elastic_index=index, doc_source=source)
    await sync_job.claim()

    index.update.assert_called_with(doc_id=sync_job.id, doc=expected_doc_source_update)


@pytest.mark.asyncio
async def test_sync_job_done(patch_logger):
    source = {"_id": "1"}
    index = Mock()
    index.update = AsyncMock(return_value=1)
    expected_doc_source_update = {
        "last_seen": ANY,
        "completed_at": ANY,
        "status": JobStatus.COMPLETED.value,
        "error": None,
    }

    sync_job = SyncJob(elastic_index=index, doc_source=source)
    await sync_job.done()

    index.update.assert_called_with(doc_id=sync_job.id, doc=expected_doc_source_update)


@pytest.mark.asyncio
async def test_sync_job_fail(patch_logger):
    source = {"_id": "1"}
    message = "something wrong"
    index = Mock()
    index.update = AsyncMock(return_value=1)
    expected_doc_source_update = {
        "last_seen": ANY,
        "completed_at": ANY,
        "status": JobStatus.ERROR.value,
        "error": message,
    }

    sync_job = SyncJob(elastic_index=index, doc_source=source)
    await sync_job.fail(message)

    index.update.assert_called_with(doc_id=sync_job.id, doc=expected_doc_source_update)


@pytest.mark.asyncio
async def test_sync_job_cancel(patch_logger):
    source = {"_id": "1"}
    index = Mock()
    index.update = AsyncMock(return_value=1)
    expected_doc_source_update = {
        "last_seen": ANY,
        "completed_at": ANY,
        "status": JobStatus.CANCELED.value,
        "error": None,
        "canceled_at": ANY,
    }

    sync_job = SyncJob(elastic_index=index, doc_source=source)
    await sync_job.cancel()

    index.update.assert_called_with(doc_id=sync_job.id, doc=expected_doc_source_update)


@pytest.mark.asyncio
async def test_sync_job_suspend(patch_logger):
    source = {"_id": "1"}
    index = Mock()
    index.update = AsyncMock(return_value=1)
    expected_doc_source_update = {
        "last_seen": ANY,
        "status": JobStatus.SUSPENDED.value,
        "error": None,
    }

    sync_job = SyncJob(elastic_index=index, doc_source=source)
    await sync_job.suspend()

    index.update.assert_called_with(doc_id=sync_job.id, doc=expected_doc_source_update)


class Banana(BaseDataSource):
    """Banana"""

    @classmethod
    def get_default_configuration(cls):
        return {"one": {"value": None}}


@pytest.mark.asyncio
async def test_prepare(mock_responses):
    class Client:
        pass

    class Index:
        client = Client()

        async def save(self, conn):
            pass

    # generic empty doc created by the user through the Kibana UI
    # when it's created that way, the service type is None,
    # so it's up to the connector to set it back to its value
    doc = {
        "status": "created",
        "service_type": None,
        "index_name": "test",
        "configuration": {},
        "language": "en",
        "scheduling": {"enabled": False},
    }
    connector = Connector(Index(), "1", doc, {})

    config = {
        "connector_id": "1",
        "service_type": "mongodb",
        "sources": {"mongodb": "connectors.tests.test_byoc:Banana"},
    }

    await connector.prepare(config)
    assert connector.source_klass.__doc__ == "Banana"
    assert connector.status == Status.NEEDS_CONFIGURATION


@pytest.mark.parametrize(
    "filtering_json, filter_state, domain, expected_filter",
    [
        (
            FILTERING,
            ACTIVE_FILTER_STATE,
            Filtering.DEFAULT_DOMAIN,
            ACTIVE_FILTERING_DEFAULT_DOMAIN,
        ),
        (
            FILTERING,
            DRAFT_FILTER_STATE,
            Filtering.DEFAULT_DOMAIN,
            DRAFT_FILTERING_DEFAULT_DOMAIN,
        ),
        (FILTERING, ACTIVE_FILTER_STATE, OTHER_DOMAIN_ONE, EMPTY_FILTER),
        (FILTERING, ACTIVE_FILTER_STATE, OTHER_DOMAIN_TWO, EMPTY_FILTER),
        # domains which do not exist should return an empty filter per default
        (FILTERING, ACTIVE_FILTER_STATE, NON_EXISTING_DOMAIN, EMPTY_FILTER),
        # if filtering is not present always return an empty filter
        ([], ACTIVE_FILTER_STATE, Filtering.DEFAULT_DOMAIN, EMPTY_FILTER),
        ([], ACTIVE_FILTER_STATE, NON_EXISTING_DOMAIN, EMPTY_FILTER),
        (None, ACTIVE_FILTER_STATE, Filtering.DEFAULT_DOMAIN, EMPTY_FILTER),
        (None, ACTIVE_FILTER_STATE, NON_EXISTING_DOMAIN, EMPTY_FILTER),
    ],
)
def test_get_filter(filtering_json, filter_state, domain, expected_filter):
    filtering = Filtering(filtering_json)

    assert filtering.get_filter(filter_state, domain) == expected_filter


@pytest.mark.parametrize(
    "domain, expected_filter",
    [
        (DEFAULT_DOMAIN, ACTIVE_FILTERING_DEFAULT_DOMAIN),
        (None, ACTIVE_FILTERING_DEFAULT_DOMAIN),
    ],
)
def test_get_active_filter(domain, expected_filter):
    filtering = Filtering(FILTERING)

    if domain is not None:
        assert filtering.get_active_filter(domain) == expected_filter
    else:
        assert filtering.get_active_filter() == expected_filter


@pytest.mark.parametrize(
    "domain, expected_filter",
    [
        (DEFAULT_DOMAIN, DRAFT_FILTERING_DEFAULT_DOMAIN),
        (None, DRAFT_FILTERING_DEFAULT_DOMAIN),
    ],
)
def test_get_draft_filter(domain, expected_filter):
    filtering = Filtering(FILTERING)

    if domain is not None:
        assert filtering.get_draft_filter(domain) == expected_filter
    else:
        assert filtering.get_draft_filter() == expected_filter


@pytest.mark.parametrize(
    "filtering, expected_transformed_filtering",
    [
        (
            {"advanced_snippet": {"value": {"query": {}}}, "rules": []},
            {"advanced_snippet": {"value": {"query": {}}}, "rules": []},
        ),
        (
            {"advanced_snippet": {"value": {}}, "rules": []},
            {"advanced_snippet": {"value": {}}, "rules": []},
        ),
        ({"advanced_snippet": {}, "rules": []}, {"advanced_snippet": {}, "rules": []}),
        ({}, {"advanced_snippet": {}, "rules": []}),
        (None, {"advanced_snippet": {}, "rules": []}),
    ],
)
def test_transform_filtering(filtering, expected_transformed_filtering):
    assert (
        Filter(filter_=filtering).transform_filtering()
        == expected_transformed_filtering
    )


@pytest.mark.parametrize(
    "validation_result, validation_target, expected_doc_source_update",
    [
        (
            FILTERING_VALIDATION_DEFAULT_DOMAIN_WITH_ERRORS,
            ValidationTarget.DRAFT,
            EXPECTED_FILTERING_AFTER_UPDATE_DRAFT,
        ),
        (
            FILTERING_VALIDATION_DEFAULT_DOMAIN_WITH_ERRORS,
            ValidationTarget.ACTIVE,
            EXPECTED_FILTERING_AFTER_UPDATE_ACTIVE,
        ),
    ],
)
@pytest.mark.asyncio
async def test_update_filtering_validation(
    validation_result, validation_target, expected_doc_source_update
):
    config = {"host": "https://nowhere.com:9200", "user": "tarek", "password": "blah"}

    connector = Mock()
    connector.filtering.to_list.return_value = DOC_SOURCE_FILTERING
    connector.id = CONNECTOR_ID

    validation_result_mock = Mock()
    validation_result_mock.to_dict = Mock(return_value=validation_result)

    client = Mock()
    client.update = AsyncMock(return_value=1)

    index = ConnectorIndex(config)
    index.client = client

    await index.update_filtering_validation(
        connector, validation_result_mock, validation_target
    )

    assert client.update.call_args_list == [
        call(
            index=CONNECTORS_INDEX,
            id=CONNECTOR_ID,
            doc=expected_doc_source_update,
            retry_on_conflict=ANY,
        )
    ]


@pytest.mark.parametrize(
    "features_json, feature_enabled",
    [
        (
            {
                "sync_rules": {
                    "basic": {"enabled": True},
                    "advanced": {"enabled": True},
                }
            },
            {
                Features.BASIC_RULES_NEW: True,
                Features.ADVANCED_RULES_NEW: True,
                Features.BASIC_RULES_OLD: False,
                Features.ADVANCED_RULES_OLD: False,
            },
        ),
        (
            {
                "sync_rules": {
                    "basic": {"enabled": True},
                    "advanced": {"enabled": False},
                }
            },
            {
                Features.BASIC_RULES_NEW: True,
                Features.ADVANCED_RULES_NEW: False,
                Features.BASIC_RULES_OLD: False,
                Features.ADVANCED_RULES_OLD: False,
            },
        ),
        (
            {
                "sync_rules": {
                    "basic": {"enabled": False},
                    "advanced": {"enabled": False},
                }
            },
            {
                Features.BASIC_RULES_NEW: False,
                Features.ADVANCED_RULES_NEW: False,
                Features.BASIC_RULES_OLD: False,
                Features.ADVANCED_RULES_OLD: False,
            },
        ),
        (
            {"filtering_advanced_config": True, "filtering_rules": True},
            {
                Features.BASIC_RULES_NEW: False,
                Features.ADVANCED_RULES_NEW: False,
                Features.BASIC_RULES_OLD: True,
                Features.ADVANCED_RULES_OLD: True,
            },
        ),
        (
            {"filtering_advanced_config": False, "filtering_rules": False},
            {
                Features.BASIC_RULES_NEW: False,
                Features.ADVANCED_RULES_NEW: False,
                Features.BASIC_RULES_OLD: False,
                Features.ADVANCED_RULES_OLD: False,
            },
        ),
        (
            {"filtering_advanced_config": True, "filtering_rules": False},
            {
                Features.BASIC_RULES_NEW: False,
                Features.ADVANCED_RULES_NEW: False,
                Features.BASIC_RULES_OLD: False,
                Features.ADVANCED_RULES_OLD: True,
            },
        ),
        (
            {
                "sync_rules": {
                    "basic": {"enabled": True},
                    "advanced": {"enabled": True},
                },
                "filtering_advanced_config": True,
                "filtering_rules": True,
            },
            {
                Features.BASIC_RULES_NEW: True,
                Features.ADVANCED_RULES_NEW: True,
                Features.BASIC_RULES_OLD: True,
                Features.ADVANCED_RULES_OLD: True,
            },
        ),
        (
            None,
            {
                Features.BASIC_RULES_NEW: False,
                Features.ADVANCED_RULES_NEW: False,
                Features.BASIC_RULES_OLD: False,
                Features.ADVANCED_RULES_OLD: False,
            },
        ),
        (
            {},
            {
                Features.BASIC_RULES_NEW: False,
                Features.ADVANCED_RULES_NEW: False,
                Features.BASIC_RULES_OLD: False,
                Features.ADVANCED_RULES_OLD: False,
            },
        ),
    ],
)
def test_feature_enabled(features_json, feature_enabled):
    features = Features(features_json)

    assert all(
        features.feature_enabled(feature)
        if enabled
        else not features.feature_enabled(feature)
        for feature, enabled in feature_enabled.items()
    )


@pytest.mark.parametrize(
    "features_json, sync_rules_enabled",
    [
        (
            {
                "sync_rules": {
                    "basic": {"enabled": True},
                    "advanced": {"enabled": False},
                },
                "filtering_advanced_config": False,
                "filtering_rules": False,
            },
            True,
        ),
        (
            {
                "sync_rules": {
                    "basic": {"enabled": False},
                    "advanced": {"enabled": True},
                },
                "filtering_advanced_config": False,
                "filtering_rules": False,
            },
            True,
        ),
        (
            {
                "sync_rules": {
                    "basic": {"enabled": False},
                    "advanced": {"enabled": False},
                },
                "filtering_advanced_config": True,
                "filtering_rules": False,
            },
            True,
        ),
        (
            {
                "sync_rules": {
                    "basic": {"enabled": False},
                    "advanced": {"enabled": False},
                },
                "filtering_advanced_config": False,
                "filtering_rules": True,
            },
            True,
        ),
        (
            {
                "sync_rules": {
                    "basic": {"enabled": False},
                    "advanced": {"enabled": False},
                },
                "filtering_advanced_config": False,
                "filtering_rules": False,
            },
            False,
        ),
        ({"other_feature": True}, False),
        (None, False),
        ({}, False),
    ],
)
def test_sync_rules_enabled(features_json, sync_rules_enabled):
    features = Features(features_json)

    assert features.sync_rules_enabled() == sync_rules_enabled


@pytest.mark.parametrize(
    "nested_dict, keys, default, expected",
    [
        # extract True
        ({"a": {"b": {"c": True}}}, ["a", "b", "c"], False, True),
        (
            {"a": {"b": {"c": True}}},
            # "d" doesn't exist -> fall back to False
            ["a", "b", "c", "d"],
            False,
            False,
        ),
        (
            {"a": {"b": {"c": True}}},
            # "wrong_key" doesn't exist -> fall back to False
            ["wrong_key", "b", "c"],
            False,
            False,
        ),
        # fallback to True
        (None, ["a", "b", "c"], True, True),
    ],
)
def test_nested_get(nested_dict, keys, default, expected):
    assert expected == Features(nested_dict)._nested_feature_enabled(keys, default)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sync_now, trigger_method",
    [
        (True, JobTriggerMethod.ON_DEMAND),
        (False, JobTriggerMethod.SCHEDULED),
    ],
)
@patch("connectors.byoc.SyncJobIndex.index")
async def test_create_job(
    index_method, sync_now, trigger_method, patch_logger, set_env
):
    connector = Mock()
    connector.id = "id"
    connector.index_name = "index_name"
    connector.language = "en"
    config = load_config(CONFIG)
    connector.sync_now = sync_now
    connector.filtering.get_active_filter.transform_filtering.return_value = Filter()
    connector.pipeline = Pipeline({})

    expected_index_doc = {
        "connector": ANY,
        "trigger_method": trigger_method.value,
        "status": JobStatus.PENDING.value,
        "created_at": ANY,
        "last_seen": ANY,
    }

    sync_job_index = SyncJobIndex(elastic_config=config["elasticsearch"])
    await sync_job_index.create(connector=connector)

    index_method.assert_called_with(expected_index_doc)


@pytest.mark.asyncio
@patch("connectors.byoc.SyncJobIndex.get_all_docs")
async def test_pending_jobs(get_all_docs, patch_logger, set_env):
    job = Mock()
    get_all_docs.return_value = AsyncIterator([job])
    config = load_config(CONFIG)
    connector_ids = [1, 2]
    expected_query = {
        "bool": {
            "must": [
                {
                    "terms": {
                        "status": [
                            JobStatus.PENDING.value,
                            JobStatus.SUSPENDED.value,
                        ]
                    }
                },
                {"terms": {"connector.id": connector_ids}},
            ]
        }
    }

    sync_job_index = SyncJobIndex(elastic_config=config["elasticsearch"])
    jobs = [
        job async for job in sync_job_index.pending_jobs(connector_ids=connector_ids)
    ]

    get_all_docs.assert_called_with(query=expected_query)
    assert len(jobs) == 1
    assert jobs[0] == job


@pytest.mark.asyncio
@patch("connectors.byoc.SyncJobIndex.get_all_docs")
async def test_orphaned_jobs(get_all_docs, patch_logger, set_env):
    job = Mock()
    get_all_docs.return_value = AsyncIterator([job])
    config = load_config(CONFIG)
    connector_ids = [1, 2]
    expected_query = {"bool": {"must_not": {"terms": {"connector.id": connector_ids}}}}

    sync_job_index = SyncJobIndex(elastic_config=config["elasticsearch"])
    jobs = [
        job async for job in sync_job_index.orphaned_jobs(connector_ids=connector_ids)
    ]

    get_all_docs.assert_called_with(query=expected_query)
    assert len(jobs) == 1
    assert jobs[0] == job


@pytest.mark.asyncio
@patch("connectors.byoc.SyncJobIndex.get_all_docs")
async def test_idle_jobs(get_all_docs, patch_logger, set_env):
    job = Mock()
    get_all_docs.return_value = AsyncIterator([job])
    config = load_config(CONFIG)
    connector_ids = [1, 2]
    expected_query = {
        "bool": {
            "filter": [
                {"terms": {"connector.id": connector_ids}},
                {
                    "terms": {
                        "status": [
                            JobStatus.IN_PROGRESS.value,
                            JobStatus.CANCELING.value,
                        ]
                    }
                },
                {"range": {"last_seen": {"lte": f"now-{IDLE_JOBS_THRESHOLD}s"}}},
            ]
        }
    }

    sync_job_index = SyncJobIndex(elastic_config=config["elasticsearch"])
    jobs = [job async for job in sync_job_index.idle_jobs(connector_ids=connector_ids)]

    get_all_docs.assert_called_with(query=expected_query)
    assert len(jobs) == 1
    assert jobs[0] == job


@pytest.mark.parametrize(
    "filtering, should_advanced_rules_be_present",
    [
        ({"advanced_snippet": {"value": ADVANCED_RULES_NON_EMPTY}}, True),
        ({"advanced_snippet": {"value": ADVANCED_AND_BASIC_RULES_NON_EMPTY}}, True),
        ({"advanced_snippet": {"value": {}}}, False),
        ({"advanced_snippet": {"value": None}}, False),
        ({"advanced_snippet": {"value": {}}, "rules": BASIC_RULES_NON_EMPTY}, False),
        (None, False),
    ],
)
def test_advanced_rules_present(filtering, should_advanced_rules_be_present):
    assert Filter(filtering).has_advanced_rules() == should_advanced_rules_be_present


@pytest.mark.parametrize(
    "filtering, validation_state, has_expected_validation_state",
    [
        (FILTER_VALIDATION_STATE_VALID, FilteringValidationState.EDITED, False),
        (FILTER_VALIDATION_STATE_INVALID, FilteringValidationState.EDITED, False),
        (FILTER_VALIDATION_STATE_EDITED, FilteringValidationState.EDITED, True),
    ],
)
def test_has_validation_state(
    filtering, validation_state, has_expected_validation_state
):
    assert (
        Filter(filtering).has_validation_state(validation_state)
        == has_expected_validation_state
    )


@pytest.mark.parametrize(
    "filtering, expected_filtering_calls",
    [
        (None, [Filter()]),
        (
            Filter({"advanced_snippet": {}, "rules": []}),
            [Filter({"advanced_snippet": {}, "rules": []})],
        ),
    ],
)
@pytest.mark.asyncio
async def test_prepare_docs(filtering, expected_filtering_calls):
    doc_source_copy = deepcopy(DOC_SOURCE)
    connector = Connector(StubIndex(), "1", doc_source_copy, {})

    docs_generator_fake = AsyncIterator([(doc_source_copy, None)])
    connector.data_provider = AsyncMock()
    connector.data_provider.get_docs = docs_generator_fake

    async for yielded_doc in connector.prepare_docs(
        connector.data_provider, filtering=filtering
    ):
        assert yielded_doc is not None

    assert docs_generator_fake.call_kwargs == [
        ("filtering", expected_filtering)
        for expected_filtering in expected_filtering_calls
    ]
    assert all(
        type(filter_) == Filter for _, filter_ in docs_generator_fake.call_kwargs
    )


@pytest.mark.parametrize(
    "key, value, default_value",
    [
        ("name", "foobar", "ent-search-generic-ingestion"),
        ("extract_binary_content", False, True),
        ("reduce_whitespace", False, True),
        ("run_ml_inference", False, True),
    ],
)
def test_pipeline_properties(key, value, default_value):
    assert Pipeline({})[key] == default_value
    assert Pipeline({key: value})[key] == value


@pytest.mark.parametrize(
    "filtering, expected_advanced_rules",
    [
        ({"advanced_snippet": {"value": {"query": {}}}, "rules": []}, {"query": {}}),
        ({"advanced_snippet": {"value": {}}, "rules": []}, {}),
        ({"advanced_snippet": {}, "rules": []}, {}),
        ({}, {}),
        (None, {}),
    ],
)
def test_get_advanced_rules(filtering, expected_advanced_rules):
    assert Filter(filtering).get_advanced_rules() == expected_advanced_rules
