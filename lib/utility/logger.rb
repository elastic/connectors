#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.
#

require 'logger'
require 'active_support/core_ext/module'

module Utility
  class Logger
    SUPPORTED_LOG_LEVELS = %i[fatal error warn info debug].freeze

    class << self

      delegate :formatter, :formatter=, :to => :logger

      def level=(log_level)
        logger.level = log_level
      end

      def logger
        @logger ||= ::Logger.new(STDOUT)
      end

      SUPPORTED_LOG_LEVELS.each do |level|
        define_method(level) do |message|
          logger.public_send(level, message)
        end
      end

      def error_with_backtrace(message: nil, exception: nil, prog_name: nil)
        logger.error(prog_name) { message } if message
        logger.error exception.message if exception
        logger.error exception.backtrace.join("\n") if exception
      end

      def new_line
        logger.info("\n")
      end
    end
  end

  def self.with_logging(config, &block)
    # Set UTC as the timezone
    ENV['TZ'] = 'UTC'
    Logger.level = config[:log_level]
    es_config = config[:elasticsearch]
    disable_warnings = if es_config.has_key?(:disable_warnings)
                         es_config[:disable_warnings]
                       else
                         true
                       end

    if disable_warnings
      Logger.info('Disabling warnings')
      Kernel.silence_warnings(&block)
    else
      Logger.info('Enabling warnings')
      Kernel.enable_warnings(&block)
    end
  end
end
