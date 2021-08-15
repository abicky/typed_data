# frozen_string_literal: true

module TypedData
  class Schema
    class RecordType < Type
      # @param name [String]
      # @param fields [Array] an array of "fields" in an Avro schema
      def initialize(name, fields)
        @name = name
        @field_to_type = fields.each_with_object({}) do |field, h|
          h[field[:name]] = Schema.build_type(field[:type])
        end
      end

      def accept(visitor, value)
        visitor.visit_record(self, value)
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
        value.is_a?(Hash) && value.all? { |k, v| @field_to_type[k]&.match?(v) }
      end
    end
  end
end
