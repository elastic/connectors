#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.
#

# frozen_string_literal: true

require 'core/output_sink'
require 'utility/logger'

module Core::OutputSink
  class ConsoleSink < Core::OutputSink::BaseSink
    def ingest(document)
      print_header 'Got a single document:'
      Utility::Logger.info document
    end

    def flush(size: nil)
      print_header 'Flushing'
    end

    def ingest_multiple(documents)
      print_header 'Got multiple documents:'
      Utility::Logger.info documents
    end

    def delete(id)
      print_header "Deleting single id: #{id}"
      Utility::Logger.info id
    end

    def delete_multiple(ids)
      print_header "Deleting several ids: #{ids}"
      Utility::Logger.info ids
    end

    private

    def print_delim
      Utility::Logger.info '----------------------------------------------------'
    end

    def print_header(header)
      print_delim
      Utility::Logger.info header
      print_delim
    end
  end
end
