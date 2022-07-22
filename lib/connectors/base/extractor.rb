#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.
#

require 'faraday'
require 'httpclient'
require 'active_support/core_ext/array/wrap'
require 'active_support/core_ext/numeric/time'
require 'active_support/core_ext/object/deep_dup'
require 'date'

require 'utility'
require 'stubs/connectors/stats' unless defined?(Rails)

module Connectors
  module Base
    class Extractor
      MAX_CONNECTION_ATTEMPTS = 3
      DEFAULT_CURSOR_KEY = 'all'.freeze

      TRANSIENT_SERVER_ERROR_CLASSES = Set.new(
        [
          Faraday::ConnectionFailed,
          Faraday::SSLError,
          Faraday::TimeoutError,
          HTTPClient::ConnectTimeoutError,
          Net::OpenTimeout
        ]
      )

      attr_reader :content_source_id, :config, :features, :original_cursors, :service_type, :completed
      attr_accessor :monitor, :client_proc

      def initialize(content_source_id:,
                     service_type:,
                     config:,
                     features:,
                     client_proc:,
                     authorization_data_proc:,
                     monitor: Utility::Monitor.new(:connector => self))
        @content_source_id = content_source_id
        @service_type = service_type
        @config = config
        @features = features
        @client_proc = client_proc
        @authorization_data_proc = authorization_data_proc
        @original_cursors = config.cursors.deep_dup
        @monitor = monitor
        @completed = false
      end

      def authorization_data!
        @authorization_data = nil
        authorization_data
      end

      def authorization_data
        @authorization_data ||= @authorization_data_proc.call
      end

      def client!
        @client = nil
        client
      end

      def client
        @client ||= client_proc.call
      end

      def retrieve_latest_cursors
        nil
      end

      def with_auth_tokens_and_retry(&block)
        connection_attempts = 0

        begin
          convert_transient_server_errors do
            convert_rate_limit_errors(&block)
          end
        rescue Utility::TokenRefreshFailedError => e
          log_error('Could not refresh token, aborting')
          raise e
        rescue Utility::PublishingFailedError => e
          log_error('Could not publish, aborting')
          raise e.reason
        rescue Utility::EvictionWithNoProgressError
          log_error('Aborting job because it did not make any progress and cannot be evicted')
          raise
        rescue Utility::EvictionError,
               Utility::ThrottlingError,
               Utility::JobDocumentLimitError,
               Utility::MonitoringError,
               Utility::JobInterruptedError,
               Utility::SecretInvalidError,
               Utility::InvalidIndexingConfigurationError => e
          # Don't retry eviction, throttling, document limit, or monitoring errors, let them bubble out
          raise
        rescue StandardError => e
          Utility::ExceptionTracking.augment_exception(e)
          connection_attempts += 1
          if connection_attempts >= MAX_CONNECTION_ATTEMPTS
            log_warn("Failed to connect in with_auth_tokens_and_retry Reason: #{e.class}: #{e.message} {:message_id => #{e.id}}")
            log_warn("Retries: #{connection_attempts}/#{MAX_CONNECTION_ATTEMPTS}, giving up.")
            Utility::ExceptionTracking.log_exception(e)
            raise e
          else
            log_warn("Failed to connect in with_auth_tokens_and_retry. Reason: #{e.class}: #{e.message} {:message_id => #{e.id}}")
            log_warn("Retries: #{connection_attempts}/#{MAX_CONNECTION_ATTEMPTS}, trying again.")
            retry
          end
        end
      end

      def yield_document_changes(modified_since: nil)
        raise NotImplementedError
      end

      def document_changes(modified_since: nil, &block)
        enum = nil
        Connectors::Stats.measure("extractor.#{Connectors::Stats.class_key(self.class)}.documents") do
          with_auth_tokens_and_retry do
            Connectors::Stats.measure("extractor.#{Connectors::Stats.class_key(self.class)}.yield_documents") do
              counter = 0
              enum = Enumerator.new do |yielder|
                yield_document_changes(:modified_since => modified_since) do |action, change, subextractors|
                  yielder.yield action, change, subextractors
                  counter += 1
                  log_info("Extracted #{counter} documents so far") if counter % 100 == 0
                end
              end
              enum.each(&block) if block_given?
            end
          end
        end
        enum
      end

      def yield_single_document_change(identifier: nil, &block)
        log_debug("Extracting single document for #{identifier}") if identifier
        convert_transient_server_errors do
          convert_rate_limit_errors(&block)
        end
        monitor.note_success
      rescue *fatal_exception_classes => e
        Utility::ExceptionTracking.augment_exception(e)
        log_error("Encountered a fall-through error during extraction#{identifying_error_message(identifier)}: #{e.class}: #{e.message} {:message_id => #{e.id}}")
        raise
      rescue StandardError => e
        Utility::ExceptionTracking.augment_exception(e)
        log_warn("Encountered error during extraction#{identifying_error_message(identifier)}: #{e.class}: #{e.message} {:message_id => #{e.id}}")
        monitor.note_error(e, :id => e.id)
      end

      def identifying_error_message(identifier)
        identifier.present? ? " of '#{identifier}'" : ''
      end

      def yield_deleted_ids(_ids)
        raise NotImplementedError
      end

      def deleted_ids(ids, &block)
        enum = nil
        Connectors::Stats.measure("extractor.#{Connectors::Stats.class_key(self.class)}.deleted_ids") do
          with_auth_tokens_and_retry do
            Connectors::Stats.measure("extractor.#{Connectors::Stats.class_key(self.class)}.yield_deleted_ids") do
              counter = 0
              enum = Enumerator.new do |yielder|
                yield_deleted_ids(ids) do |id|
                  yielder.yield id
                  counter += 1
                  log_info("Deleted #{counter} documents so far") if counter % 100 == 0
                end
              end
              enum.each(&block) if block_given?
            end
          end
        end
        enum
      end

      def yield_permissions(source_user_id)
        # no-op for content source without DLP support
      end

      def permissions(source_user_id, &block)
        result = []
        Connectors::Stats.measure("extractor.#{Connectors::Stats.class_key(self.class)}.permissions") do
          with_auth_tokens_and_retry do
            Connectors::Stats.measure("extractor.#{Connectors::Stats.class_key(self.class)}.yield_permissions") do
              yield_permissions(source_user_id) do |permissions|
                log_info("Extracted #{permissions.size} permissions for source user #{source_user_id}")
                result = permissions
                block.call(permissions) if block_given?
              end
            end
          end
        end
        result.each
      end

      Utility::Logger::SUPPORTED_LOG_LEVELS.each do |log_level|
        define_method(:"log_#{log_level}") do |message|
          if message.kind_of?(String)
            message = "ContentSource[#{content_source_id}, #{service_type}]: #{message}"
          end
          Utility::Logger.public_send(log_level, message)
        end
      end

      def convert_transient_server_errors
        yield
      rescue StandardError => e
        raise unless transient_error?(e)

        raise Utility::TransientServerError.new(
          "Transient error #{e.class}: #{e.message}",
          :suspend_until => Connectors.config.fetch('transient_server_error_retry_delay_minutes').minutes.from_now,
          :cursors => config.cursors
        )
      end

      def transient_error?(error)
        TRANSIENT_SERVER_ERROR_CLASSES.any? { |error_class| error.kind_of?(error_class) }
      end

      def evictable?
        false
      end

      def cursors_modified_since_start?
        config.cursors != original_cursors
      end

      def download_args_and_proc(id:, name:, size:, download_args:, &block)
        [id, name, size, download_args, block]
      end

      private

      def convert_rate_limit_errors
        yield # subclasses override this with source-specific handling.
      end

      def fatal_exception_classes
        [
          Utility::TokenRefreshFailedError,
          Utility::EvictionError,
          Utility::ThrottlingError,
          Utility::JobDocumentLimitError,
          Utility::MonitoringError
        ]
      end
    end
  end
end
