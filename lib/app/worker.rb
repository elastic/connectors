#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.
#

# frozen_string_literal: true

require 'active_support'
require 'connectors'
require 'core'
require 'utility'
require 'app/config'
require 'concurrent'

module App
  module Worker
    POLL_IDLING = (App::Config[:idle_timeout] || 60).to_i

    class << self
      def start!
        if App::Config[:disable_warnings] || true
          Kernel.silence_warnings do   # intercepts `warn` calls emited by the Elasticsearch client
            _start
          end
        else
          _start
        end
      end

      def _start
        Utility::Logger.info('Running pre-flight check.')
        pre_flight_check
        Utility::Logger.info('Starting connector service workers.')
        start_heartbeat_task
        start_polling_jobs
      end

      private

      def pre_flight_check
        raise "#{App::Config[:service_type]} is not a supported connector" unless Connectors::REGISTRY.registered?(App::Config[:service_type])
        begin
          Core::ElasticConnectorActions.ensure_connectors_index_exists
          Core::ElasticConnectorActions.ensure_job_index_exists
          connector_settings = Core::ConnectorSettings.fetch(App::Config[:connector_id])
          Core::ElasticConnectorActions.ensure_content_index_exists(connector_settings.index_name)
        rescue Elastic::Transport::Transport::Errors::Unauthorized => e
          raise 'Elasticsearch is not authorizing access'
        end
      end

      def start_heartbeat_task
        connector_id = App::Config[:connector_id]
        service_type = App::Config[:service_type]
        interval_seconds = 60 # seconds
        Utility::Logger.debug("Starting heartbeat timer task with interval #{interval_seconds} seconds.")
        task = Concurrent::TimerTask.new(execution_interval: interval_seconds, run_now: true) do
          Utility::Logger.debug("Sending heartbeat for the connector #{connector_id}")
          Core::Heartbeat.send(connector_id, service_type)
        rescue StandardError => e
          Utility::ExceptionTracking.log_exception(e, 'Heartbeat timer encountered unexpected error.')
        end

        Utility::Logger.info('Successfully started heartbeat task.')

        task.execute
      end

      def start_polling_jobs
        Utility::Logger.info('Polling Elasticsearch for synchronisation jobs to run.')
        loop do
          job_runner = create_sync_job_runner
          job_runner.execute
        rescue StandardError => e
          Utility::ExceptionTracking.log_exception(e, 'Sync failed due to unexpected error.')
        ensure
          if POLL_IDLING > 0
            Utility::Logger.info("Sleeping for #{POLL_IDLING} seconds.")
            sleep(POLL_IDLING)
          end
        end
      end

      def create_sync_job_runner
        connector_settings = Core::ConnectorSettings.fetch(App::Config[:connector_id])

        Core::SyncJobRunner.new(connector_settings, App::Config[:service_type])
      end
    end
  end
end
