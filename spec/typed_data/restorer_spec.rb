require "date"
require "json"

require "spec_helper"

RSpec.describe TypedData::Restorer do
  describe "#restore" do
    subject(:restored_data) { restorer.restore(data) }
    let(:restorer) { described_class.new(JSON.parse(schema_file), key_formatter: key_formatter) }
    let(:key_formatter) { :bigquery }

    let(:schema_file) do
      File.read(File.join(__dir__, "..", "avsc", "#{schema_name}.avsc"))
    end

    context "primitive types" do
      let(:schema_name) { "primitive_types" }

      let(:data) do
        {
          "null" => nil,
          "boolean" => false,
          "int" => 2147483647,
          "long" => 2147483648,
          "float" => 1.5,
          "double" => 1.8,
          "bytes" => ["foo"].pack("m0"),
          "string" => "bar",
        }
      end

      it do
        expect(restored_data).to eq({
          "null" => nil,
          "boolean" => false,
          "int" => 2147483647,
          "long" => 2147483648,
          "float" => 1.5,
          "double" => 1.8,
          "bytes" => "foo",
          "string" => "bar",
        })
      end
    end

    context "complex types" do
      context "without union type" do
        let(:schema_name) { "complex_types_without_union" }

        let(:md5) { ["58e53d1324eef6265fdb97b08ed9aadf"].pack("H*") }

        let(:data) do
          {
            "record" => {
              "int_field" => 1,
            },
            "enum" => "red",
            "array" => ["1", "2"],
            "map" => [
              {
                "key" => "key1",
                "value" => "value1",
              },
            ],
            "fixed" => [md5].pack("m0"),
          }
        end

        it do
          expect(restored_data).to eq({
            "record" => {
              "int_field" => 1,
            },
            "enum" => "red",
            "array" => ["1", "2"],
            "map" => {
              "key1" => "value1",
            },
            "fixed" => md5,
          })
        end
      end

      context "with nullable" do
        context "with nullable primitive" do
          let(:schema_name) { "nullable_primitives" }

          let(:data) do
            {
              "nullable_string1" => "null",
              "nullable_string2" => nil,
              "nullable_int1" => 1,
              "nullable_int2" => nil,
            }
          end

          it do
            expect(restored_data).to eq({
              "nullable_string1" => "null",
              "nullable_string2" => nil,
              "nullable_int1" => 1,
              "nullable_int2" => nil,
            })
          end
        end

        context "with nullable record" do
          let(:schema_name) { "nullable_record" }

          let(:data) do
            {
              "nullable_string_record1" => { "string_field" => "1" },
              "nullable_string_record2" => nil,
              "nullable_union_record1" => {
                "union_field" => {
                  "string_value" => "1",
                },
              },
              "nullable_union_record2" => {
                "union_field" => {
                  "int_value" => 2,
                },
              },
              "nullable_union_record3" => nil,
            }
          end

          it do
            expect(restored_data).to eq({
              "nullable_string_record1" => { "string_field" => "1" },
              "nullable_string_record2" => nil,
              "nullable_union_record1" => { "union_field" => "1" },
              "nullable_union_record2" => { "union_field" => 2 },
              "nullable_union_record3" => nil,
            })
          end
        end

        context "with nullable array" do
          let(:schema_name) { "nullable_array" }

          let(:data) do
            {
              "nullable_string_array1" => ["1"],
              "nullable_string_array2" => nil,
              "nullable_union_array1" => [
                {
                  "string_value" => "1",
                },
                {
                  "int_value" => 2,
                },
              ],
              "nullable_union_array2" => nil,
            }
          end

          it do
            expect(restored_data).to eq({
              "nullable_string_array1" => ["1"],
              "nullable_string_array2" => nil,
              "nullable_union_array1" => ["1", 2],
              "nullable_union_array2" => nil,
            })
          end
        end

        context "with nullable map" do
          let(:schema_name) { "nullable_map" }

          let(:data) do
            {
              "nullable_string_map1" => [
                { "key" => "key1", "value" => "1" }
              ],
              "nullable_string_map2" => nil,
              "nullable_union_map1" => [
                {
                  "key" => "key1",
                  "value" => {
                    "string_value" => "1",
                  },
                },
                {
                  "key" => "key2",
                  "value" => {
                    "int_value" => 2,
                  },
                },
              ],
              "nullable_union_map2" => nil,
            }
          end

          it do
            expect(restored_data).to eq({
              "nullable_string_map1" => { "key1" => "1" },
              "nullable_string_map2" => nil,
              "nullable_union_map1" => { "key1" => "1", "key2" => 2 },
              "nullable_union_map2" => nil,
            })
          end
        end
      end

      context "with nested record" do
        let(:schema_name) { "nested_record" }

        let(:data) do
          {
            "nested_record" => {
              "int_field" => 1,
            },
            "nested_record_with_union_type" => {
              "union_field" => {
                "int_value" => 1,
              }
            }
          }
        end

        it do
          expect(restored_data).to eq({
            "nested_record" => {
              "int_field" => 1,
            },
            "nested_record_with_union_type" => {
              "union_field" => 1,
            },
          })
        end
      end

      context "with nested array" do
        let(:schema_name) { "nested_array" }

        let(:data) do
          {
            "nested_array" => [1, 2, 3],
            "nested_array_with_union_type" => [
              {
                "int_value" => 1,
              },
              {
                "string_value" => "2",
              },
              {
                "int_value" => 3,
              },
            ],
          }
        end

        it do
          expect(restored_data).to eq({
            "nested_array" => [1, 2, 3],
            "nested_array_with_union_type" => [1, "2", 3],
          })
        end
      end

      context "with nested map" do
        let(:schema_name) { "nested_map" }

        let(:data) do
          {
            "nested_map" => [
              "key" => "key",
              "value" => [
                {
                  "key" => "key1",
                  "value" => 1,
                },
                {
                  "key" => "key2",
                  "value" => 2,
                },
              ],
            ],
            "nested_map_with_union_type" => [
              "key" => "key",
              "value" => [
                {
                  "key" => "key1",
                  "value" => {
                    "int_value" => 1,
                  },
                },
                {
                  "key" => "key2",
                  "value" => {
                    "string_value" => "2",
                  },
                },
              ],
            ],
          }
        end

        it do
          expect(restored_data).to eq({
            "nested_map" => {
              "key" => {
                "key1" => 1,
                "key2" => 2,
              },
            },
            "nested_map_with_union_type" => {
              "key" => {
                "key1" => 1,
                "key2" => "2",
              },
            },
          })
        end
      end

      context "with records in array" do
        let(:schema_name) { "records_in_array" }

        let(:data) do
          {
            "array_field" => [
              { "int_field" => 1 },
              { "int_field" => 2 },
            ],
          }
        end

        it do
          expect(restored_data).to eq({
            "array_field" => [
              { "int_field" => 1 },
              { "int_field" => 2 },
            ],
          })
        end
      end

      context "with simple union type" do
        let(:schema_name) { "complex_types_with_simple_union_type" }

        context "with the default formatter" do
          let(:data) do
            {
              "simple_union" => {
                "string_value" => "str",
              },
            }
          end

          it do
            expect(restored_data).to eq({
              "simple_union" => "str",
            })
          end
        end

        context "with the avro formatter" do
          let(:key_formatter) { :avro }

          let(:data) do
            {
              "simple_union" => {
                "string" => "str",
              },
            }
          end

          it do
            expect(restored_data).to eq({
              "simple_union" => "str",
            })
          end
        end

        context "with other fields" do
          let(:data) do
            {
              "simple_union" => {
                "boolean_value" => nil,
                "string_value" => "str",
              },
            }
          end

          it do
            expect(restored_data).to eq({
              "simple_union" => "str",
            })
          end
        end
      end

      context "with complex union type" do
        let(:schema_name) { "complex_types_with_complex_union_type" }

        context "with string value" do
          let(:data) do
            {
              "complex_union" => {
                "string_value" => "str"
              },
            }
          end

          it do
            expect(restored_data).to eq({
              "complex_union" => "str",
            })
          end
        end

        context "with record value" do
          let(:data) do
            {
              "complex_union" => {
                "with_int_field_value" => {
                  "int_field" => 1
                },
              },
            }
          end

          it do
            expect(restored_data).to eq({
              "complex_union" => {
                "int_field" => 1,
              }
            })
          end
        end

        context "with array value" do
          let(:data) do
            {
              "complex_union" => {
                "array_string_value" => ["1"],
              },
            }
          end

          it do
            expect(restored_data).to eq({
              "complex_union" => ["1"],
            })
          end
        end

        context "with map value" do
          let(:data) do
            {
              "complex_union" => {
                "map_string_value" => [
                  { "key" => "a", "value" => "1"},
                ],
              },
            }
          end

          it do
            expect(restored_data).to eq({
              "complex_union" => { "a" => "1" },
            })
          end
        end
      end

      context "with union array" do
        let(:schema_name) { "complex_types_with_union_array" }

        let(:data) do
          {
            "union_array" => [
              {
                "color_value" => "red",
              },
              {
                "array_union_long_string_value" => [
                  {
                    "long_value" => 1,
                  },
                  {
                    "string_value" => "2",
                  },
                ],
              },
              {
                "string_value" => uuid,
              },
              {
                "map_union_long_string_null_value" => [
                  {
                    "key" => "a",
                    "value" => {
                      "long_value" => 1,
                    },
                  },
                  {
                    "key" => "b",
                    "value" => {},
                  },
                  {
                    "key" => "c",
                    "value" => {
                      "string_value" => "3",
                    },
                  },
                  {
                    "key" => "d",
                    "value" => {
                      "long_value" => nil,
                      "string_value" => nil,
                    },
                  },
                ],
              },
            ],
          }
        end

        let(:uuid) { "98e73f15-db77-4424-94f8-8c8745920e59" }

        it do
          expect(restored_data).to eq({
            "union_array" => [
              "red",
              [1, "2"],
              uuid,
              {
                "a" => 1,
                "b" => nil,
                "c" => "3",
                "d" => nil,
              },
            ],
          })
        end
      end

      context "with union map" do
        let(:schema_name) { "complex_types_with_union_map" }

        let(:md5) { ["58e53d1324eef6265fdb97b08ed9aadf"].pack("H*") }

        let(:data) do
          {
            "union_map" => [
              {
                "key" => "md5",
                "value" => {
                  "md5_value" => [md5].pack("m0"),
                },
              },
              {
                "key" => "array",
                "value" => {
                  "array_union_long_string_value" => [
                    {
                      "long_value" => 1,
                    },
                    {
                      "string_value" => "2",
                    },
                  ],
                },
              },
              {
                "key" => "map",
                "value" => {
                  "map_union_long_string_null_value" => [
                    {
                      "key" => "a",
                      "value" => {
                        "long_value" => 1,
                      },
                    },
                    {
                      "key" => "b",
                      "value" => {},
                    },
                    {
                      "key" => "c",
                      "value" => {
                        "string_value" => "3",
                      },
                    },
                  ],
                },
              },
            ]
          }
        end

        it do
          expect(restored_data).to eq({
            "union_map" => {
              "md5" => md5,
              "array" => [1, "2"],
              "map" => {
                "a" => 1,
                "b" => nil,
                "c" => "3",
              },
            },
          })
        end
      end
    end

    context "logical types" do
      let(:schema_name) { "logical_types" }

      context "when timestamp data has millisecond precision or microsecond precision" do
        let(:now) { Time.now.utc }
        let(:data) do
          {
            "date" => now.strftime("%F"),
            "time_millis" => now.strftime("%T.%3N"),
            "time_micros" => now.strftime("%T.%6N"),
            "timestamp_millis" => now.strftime("%F %T.%3N"),
            "timestamp_micros" => now.strftime("%F %T.%6N"),
            "date_array" => [now.strftime("%F")],
            "date_map" => [
              { "key" => "today", "value" => now.strftime("%F") },
            ],
            "date_in_date_or_timestamp_millis" => {
              "int_date_value" => now.strftime("%F"),
            },
            "timestamp_in_date_or_timestamp_millis" => {
              "long_timestamp_millis_value" => now.strftime("%F %T.%3N"),
            },
          }
        end

        it do
          expect(restored_data).to eq({
            "date" => (now.to_date - Date.new(1970, 1, 1)).to_i,
            "time_millis" => (now.sec + now.min * 60 + now.hour * 60**2) * 10**3 + now.nsec / 10**6,
            "time_micros" => (now.sec + now.min * 60 + now.hour * 60**2) * 10**6 + now.nsec / 10**3,
            "timestamp_millis" => now.to_i * 10**3 + now.nsec / 10**6,
            "timestamp_micros" => now.to_i * 10**6 + now.nsec / 10**3,
            "date_array" => [(now.to_date - Date.new(1970, 1, 1)).to_i],
            "date_map" => { "today" => (now.to_date - Date.new(1970, 1, 1)).to_i },
            "date_in_date_or_timestamp_millis" => (now.to_date - Date.new(1970, 1, 1)).to_i,
            "timestamp_in_date_or_timestamp_millis" => now.to_i * 10**3 + now.nsec / 10**6,
          })
        end
      end

      context "when timestamp data doesn't have millisecond precision or microsecond precision" do
        let(:now) { Time.now.utc }
        let(:data) do
          {
            "date" => now.strftime("%F"),
            "time_millis" => now.strftime("%T.%3N"),
            "time_micros" => now.strftime("%T.%6N"),
            "timestamp_millis" => now.strftime("%F %T"),
            "timestamp_micros" => now.strftime("%F %T"),
            "date_array" => [now.strftime("%F")],
            "date_map" => [
              { "key" => "today", "value" => now.strftime("%F") },
            ],
            "date_in_date_or_timestamp_millis" => {
              "int_date_value" => now.strftime("%F"),
            },
            "timestamp_in_date_or_timestamp_millis" => {
              "long_timestamp_millis_value" => now.strftime("%F %T.%3N"),
            },
          }
        end

        it do
          expect(restored_data).to eq({
            "date" => (now.to_date - Date.new(1970, 1, 1)).to_i,
            "time_millis" => (now.sec + now.min * 60 + now.hour * 60**2) * 10**3 + now.nsec / 10**6,
            "time_micros" => (now.sec + now.min * 60 + now.hour * 60**2) * 10**6 + now.nsec / 10**3,
            "timestamp_millis" => now.to_i * 10**3,
            "timestamp_micros" => now.to_i * 10**6,
            "date_array" => [(now.to_date - Date.new(1970, 1, 1)).to_i],
            "date_map" => { "today" => (now.to_date - Date.new(1970, 1, 1)).to_i },
            "date_in_date_or_timestamp_millis" => (now.to_date - Date.new(1970, 1, 1)).to_i,
            "timestamp_in_date_or_timestamp_millis" => now.to_i * 10**3 + now.nsec / 10**6,
          })
        end
      end
    end
  end
end
