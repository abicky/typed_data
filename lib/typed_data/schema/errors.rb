# frozen_string_literal: true

module TypedData
  class Schema
    class UnknownField < StandardError; end
    class UnsupportedType < StandardError; end
    class InvalidValue < StandardError; end
  end
end
