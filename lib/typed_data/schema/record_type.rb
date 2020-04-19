# frozen_string_literal: true

module TypedData
  class Schema
    class RecordType < Type
      # @param fields [Array] an array of "fields" in an Avro schema
      def initialize(fields)
        @field_to_type = fields.each_with_object({}) do |field, h|
          h[field["name"] || field[:name]] = Schema.build_type(field["type"] || field[:type])
        end
      end

      def primitive?
        false
      end

      # @param field_name [String, Symbol]
      def find_type(field_name)
        @field_to_type.fetch(field_name.to_s) do
          raise UnknownField, "Unknown field \"#{field_name}\""
        end
      end

      def match?(value)
        value.is_a?(Hash)
      end
    end
  end
end
