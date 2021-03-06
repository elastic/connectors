#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.
#

# frozen_string_literal: true

require 'bson'
require 'utility/logger'
require 'stubs/enterprise_search/exception_tracking'

module Utility
  class ExceptionTracking
    class << self
      def capture_message(message, context = {})
        EnterpriseSearch::ExceptionTracking.capture_message(message, context)
      end

      def capture_exception(exception, context = {})
        EnterpriseSearch::ExceptionTracking.log_exception(exception, :context => context)
      end

      def log_exception(exception, message = nil)
        EnterpriseSearch::ExceptionTracking.log_exception(exception, message, :logger => Utility::Logger.logger)
      end

      def augment_exception(exception)
        unless exception.respond_to?(:id)
          exception.instance_eval do
            def id
              @error_id ||= BSON::ObjectId.new.to_s
            end
          end
        end
      end
    end
  end
end
