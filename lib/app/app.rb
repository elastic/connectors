#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.
#

# frozen_string_literal: true

$LOAD_PATH << '../'

require 'app/cli'
require 'app/worker'
require 'app/config'
require 'core'
require 'utility/logger'

module App
  def self.pre_flight_check
    raise "#{App::Config[:service_type]} is not a supported connector" unless Connectors::REGISTRY.registered?(App::Config[:service_type])
    Core::ElasticConnectorActions.ensure_connectors_index_exists
    Core::ElasticConnectorActions.ensure_job_index_exists
    connector_settings = Core::ConnectorSettings.fetch(App::Config[:connector_id])
    Core::ElasticConnectorActions.ensure_content_index_exists(
      connector_settings.index_name,
      App::Config[:use_analysis_icu],
      App::Config[:content_language_code]
    )
  end

  # Set UTC as the timezone
  ENV['TZ'] = 'UTC'

  Utility::Logger.level = App::Config[:log_level]
  Utility::Logger.info('Running pre-flight check.')
  pre_flight_check

  ARGV.first == '--console' ? App::Cli.start! : App::Worker.start!
end
