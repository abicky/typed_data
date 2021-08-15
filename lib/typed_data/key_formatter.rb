module TypedData
  class KeyFormatter
    class UnknownFormatter < StandardError; end

    UNION_TYPE_KEY_FORMATTERS = {
      bigquery: ->(type) { "#{type}_value" },
      avro: ->(type) { type.split("_").first },
    }

    # @param formatter [Symbol]
    def self.find(formatter)
      UNION_TYPE_KEY_FORMATTERS.fetch(formatter) do
        raise UnknownFormatter, "Unknown formatter: #{formatter}"
      end
    end
  end
end
