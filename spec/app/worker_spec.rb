#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.
#

# frozen_string_literal: true

require 'yaml'
require 'app/worker'

describe App::Worker do
  it 'should raise error for invalid service type' do
    allow(App::Config).to receive(:[]).with(:disable_warnings).and_return(true)
    allow(App::Config).to receive(:[]).with(:service_type).and_return('foobar')
    allow(Connectors::REGISTRY).to receive(:connector_class).and_return(nil)
    expect { described_class.start! }.to raise_error('foobar is not a supported connector')
  end

  it 'should discard warnings from the Elasticsearch lib' do
    config = {
      :disable_warnings => false,
      :service_type => 'stub_connector',
      :connector_id => '1',
      :elasticsearch => {
      :api_key => 'key',
      :hosts => 'http://notreallyaserver'
      }
    }

    App::Config = config

    body = {
      'version' => {
        'number' => '8.4.0'
      }
    }

    # missing x-elastic-product on purpose
    stub_request(:get, 'http://notreallyaserver:9200/')
      .to_return(
        status: 401,
        body: YAML.dump(body),
        headers: {
          'content-type' => 'application/yaml'
        }
      )

    settings = Core::ConnectorSettings.new(
      {
        '_source'.to_sym => {
          'index_name' => 'index'
        }
      }
    )
    expect(Core::ElasticConnectorActions).to receive(:ensure_index_exists).at_least(3).times
    expect(Core::ConnectorSettings).to receive(:fetch).and_return(settings)

    allow(App::Worker).to receive(:start_heartbeat_task)
    allow(App::Worker).to receive(:start_polling_jobs)

    described_class.start!

    # expect { described_class.start!}.to output("my message").to_stdout
  end
end
