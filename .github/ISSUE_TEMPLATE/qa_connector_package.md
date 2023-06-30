---
name: Non-regression QA
about: Create a non-regression QA issue for one connector client
title: '[8.x QA] Validate connector client'
labels: testing
assignees: ''

---

## Non-regression QA

**Note:** always test with the latest Build Candidate on Elastic Cloud, using the full Elastic stack

- [ ] Start the whole stack from scratch and navigate to Enterprise Search
- [ ] Check that no indices are shown in the top level Indices list
- [ ] Click on "Create an Elasticsearch index" - a new page is open where you can select an ingestion method
- [ ] Choose Connector -> Use a connector
- [ ] Choose the connector you want to test and Continue
- [ ] Create an index with a valid name and Universal language
-------

- [ ] Connector name and description are editable on the Configurations page
- [ ] Connector can be deleted from the Indices page
- [ ] Connector can be deleted from the Indices page and it can be recreated with the same name after
- [ ] Pull connectors repository, run `make install` but do not run connector yet


- [ ] Verify that you are redirected to "configuration" page where you can create an api key and can copy connector id / whole section of config into the connector
- [ ] Update connector configuration with the api_key and connector_id, plus choose a service_type to test and set it in config
- [ ] Start the connector by running `make run` - verify that it starts and does not actually do anything yet
- [ ] Wait for the Kibana page with the connector configuration to update and verify that it's possible to edit connector configuration now
- [ ] Edit and save connector configuration, then reload the page and verify that configuration is properly saved
- [ ] Click on "Set schedule and sync" and verify that you're redirected to the scheduling tab
- [ ] Enable scheduling for frequency = every minute and save schedule; refresh the page and verify that the changes were stored
- [ ] Switch to the connector and wait for a minute or two, verify that connector starts to ingest data
- [ ] Verify that the data from the connector appears in the expected index
- [ ] Verify that on the index list page index information is updated properly, showing expected number of documents and new index size
- [ ] Verify that on the connector overview page "Document Count" is updated to reflect the number of documents in the index
- [ ] Verify that you can see ingested documents in `documents` tab
- [ ] Verify that index mappings are correct on the `index mappings` tab


**Record a short demo showing the connectors' configuration and that there were documents ingested**
