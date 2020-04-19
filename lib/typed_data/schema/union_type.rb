# frozen_string_literal: true

module TypedData
  class Schema
    class UnionType < Type
      # @param types [Array<String>]
      def initialize(types)
        @types = types.map(&Schema.method(:build_type))
      end

      def to_s
        "union_#{@types.map(&:to_s).join("_")}"
      end

      def coerce(value)
        type = find_match(value)
        if type.is_a?(NullType)
          default_value
        else
          default_value.merge!("#{type}_value" => type.coerce(value).to_s)
        end
      end

      def primitive?
        false
      end

      def find_match(value)
        @types.find { |t| t.match?(value) }
      end

      def match?(value)
        @types.any? { |t| t.match?(value) }
      end

      def default_value
        @types.each_with_object({}) do |t, v|
          next if t.is_a?(NullType)
          v["#{t}_value"] = t.primitive? || t.is_a?(EnumType) ? nil : []
        end
      end
    end
  end
end
