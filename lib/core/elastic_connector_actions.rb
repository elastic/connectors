#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.
#

# frozen_string_literal: true
#
require 'active_support/core_ext/hash'
require 'utility/elasticsearch/index/mappings'
require 'utility/elasticsearch/index/text_analysis_settings'
require 'connectors/connector_status'
require 'connectors/sync_status'

module Core
  class ElasticConnectorActions
    CONNECTORS_INDEX = '.elastic-connectors'
    JOB_INDEX = '.elastic-connectors-sync-jobs'

    class << self

      def force_sync(connector_id)
        update_connector_fields(connector_id, :scheduling => { :enabled => true }, :sync_now => true)
      end

      def create_connector(index_name, service_type)
        body = {
          :scheduling => { :enabled => true },
          :index_name => index_name,
          :service_type => service_type
        }
        response = client.index(:index => CONNECTORS_INDEX, :body => body)
        response['_id']
      end

      def get_connector(connector_id)
        client.get(:index => CONNECTORS_INDEX, :id => connector_id, :ignore => 404).with_indifferent_access
      end

      def update_connector_configuration(connector_id, configuration)
        update_connector_fields(connector_id, :configuration => configuration)
      end

      def enable_connector_scheduling(connector_id, cron_expression)
        payload = { :enabled => true, :interval => cron_expression }
        update_connector_fields(connector_id, :scheduling => payload)
      end

      def disable_connector_scheduling(connector_id)
        payload = { :enabled => false }
        update_connector_fields(connector_id, :scheduling => payload)
      end

      def set_configurable_field(connector_id, field_name, label, value)
        payload = { field_name => { :value => value, :label => label } }
        update_connector_fields(connector_id, :configuration => payload)
      end

      def claim_job(connector_id)
        update_connector_fields(connector_id,
                                :sync_now => false,
                                :last_sync_status => Connectors::SyncStatus::IN_PROGRESS,
                                :last_synced => Time.now)

        body = {
          :connector_id => connector_id,
          :status => Connectors::SyncStatus::IN_PROGRESS,
          :worker_hostname => Socket.gethostname,
          :created_at => Time.now
        }
        job = client.index(:index => JOB_INDEX, :body => body)

        job['_id']
      end

      def update_connector_status(connector_id, status)
        update_connector_fields(connector_id, :status => status)
      end

      def complete_sync(connector_id, job_id, status)
        sync_status = status[:error] ? Connectors::SyncStatus::FAILED : Connectors::SyncStatus::COMPLETED

        update_connector_fields(connector_id,
                                :last_sync_status => sync_status,
                                :last_sync_error => status[:error],
                                :last_synced => Time.now)

        body = {
          :doc => {
            :status => sync_status,
            :completed_at => Time.now
          }.merge(status)
        }
        client.update(:index => JOB_INDEX, :id => job_id, :body => body)
      end

      def fetch_document_ids(index_name)
        page_size = 1000
        result = []
        begin
          pit_id = client.open_point_in_time(:index => index_name, :keep_alive => '1m', :expand_wildcards => 'all')['id']
          body = {
            :query => { :match_all => {} },
            :sort => [{ :id => { :order => :asc } }],
            :pit => {
              :id => pit_id,
              :keep_alive => '1m'
            },
            :size => page_size,
            :_source => false
          }
          loop do
            response = client.search(:body => body)
            hits = response['hits']['hits']

            ids = hits.map { |h| h['_id'] }
            result += ids
            break if hits.size < page_size

            body[:search_after] = hits.last['sort']
            body[:pit][:id] = response['pit_id']
          end
        ensure
          client.close_point_in_time(:index => index_name, :body => { :id => pit_id })
        end

        result
      end

      # should only be used in CLI
      def ensure_content_index_exists(index_name, use_icu_locale = false, language_code = nil)
        settings = Utility::Elasticsearch::Index::TextAnalysisSettings.new(:language_code => language_code, :analysis_icu => use_icu_locale).to_h
        mappings = Utility::Elasticsearch::Index::Mappings.default_text_fields_mappings(:connectors_index => true)

        body_payload = { settings: settings, mappings: mappings }
        ensure_index_exists(index_name, body_payload)
      end

      # should only be used in CLI
      def ensure_index_exists(index_name, body = {})
        if client.indices.exists?(:index => index_name)
          return unless body[:mappings]
          Utility::Logger.debug("Index #{index_name} already exists. Checking mappings...")
          Utility::Logger.debug("New mappings: #{body[:mappings]}")
          response = client.indices.get_mapping(:index => index_name)
          existing = response[index_name]['mappings']
          if existing.empty?
            Utility::Logger.debug("Index #{index_name} has no mappings. Adding mappings...")
            client.indices.put_mapping(:index => index_name, :body => body[:mappings], :expand_wildcards => 'all')
            Utility::Logger.debug("Index #{index_name} mappings added.")
          else
            Utility::Logger.debug("Index #{index_name} already has mappings: #{existing}. Skipping...")
          end
        else
          client.indices.create(:index => index_name, :body => body)
          Utility::Logger.debug("Created index #{index_name}")
        end
      end

      def system_index_body(alias_name: nil, mappings: nil)
        body = {
          :settings => {
            :index => {
              :hidden => true,
              :number_of_replicas => 0,
              :auto_expand_replicas => '0-5'
            }
          }
        }
        body[:aliases] = { alias_name => { :is_write_index => true } } unless alias_name.nil? || alias_name.empty?
        body[:mappings] = mappings unless mappings.nil?
        body
      end

      def ensure_connectors_index_exists
        mappings = {
          :properties => {
            :api_key_id => { :type => :keyword },
            :configuration => { :type => :object },
            :error => { :type => :text },
            :index_name => { :type => :keyword },
            :last_seen => { :type => :date },
            :last_synced => { :type => :date },
            :scheduling => {
              :properties => {
                :enabled => { :type => :boolean },
                :interval => { :type => :text }
              }
            },
            :service_type => { :type => :keyword },
            :status => { :type => :keyword },
            :sync_error => { :type => :text },
            :sync_now => { :type => :boolean },
            :sync_status => { :type => :keyword }
          }
        }
        ensure_index_exists("#{CONNECTORS_INDEX}-v1", system_index_body(:alias_name => CONNECTORS_INDEX, :mappings => mappings))
      end

      def ensure_job_index_exists
        mappings = {
          :properties => {
            :connector_id => { :type => :keyword },
            :status => { :type => :keyword },
            :error => { :type => :text },
            :worker_hostname => { :type => :keyword },
            :indexed_document_count => { :type => :integer },
            :deleted_document_count => { :type => :integer },
            :created_at => { :type => :date },
            :completed_at => { :type => :date }
          }
        }
        ensure_index_exists("#{JOB_INDEX}-v1", system_index_body(:alias_name => JOB_INDEX, :mappings => mappings))
      end

      def update_connector_fields(connector_id, doc = {})
        return if doc.empty?
        client.update(:index => CONNECTORS_INDEX, :id => connector_id, :body => { :doc => doc })
      end

      def client
        @client ||= Utility::EsClient.new
      end
    end
  end
end
