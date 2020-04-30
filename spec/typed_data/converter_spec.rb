require "date"
require "json"

require "spec_helper"

RSpec.describe TypedData::Converter do
  describe "#convert" do
    subject(:converted_data) { converter.convert(data) }
    let(:converter) { described_class.new(JSON.parse(schema_file)) }

    let(:schema_file) do
      File.read(File.join(__dir__, "..", "avsc", "#{schema_name}.avsc"))
    end

    shared_examples "converter for BigQuery" do
      it "converts the argument into data BigQuery can load", bigquery: true do
        require "tempfile"
        require "avro"
        require "google/cloud/bigquery"

        Tempfile.open(schema_name) do |f|
          schema = Avro::Schema.parse(schema_file)
          writer = Avro::IO::DatumWriter.new(schema)
          dw = Avro::DataFile::Writer.new(f, writer, schema)
          dw << data
          dw.close

          bigquery = Google::Cloud::Bigquery.new
          dataset = bigquery.dataset(ENV["BIGQUERY_DATASET"])
          dataset.load(schema_name, f.path, format: "avro", write: "truncate") do |updater|
            updater.gapi.configuration.load.use_avro_logical_types = true
          end

          Tempfile.open("converted") do |jsonl|
            jsonl.puts(converter.convert(data).compact.to_json)
            dataset.load(schema_name, jsonl, format: "json")
          end

          result = dataset.query("SELECT * FROM #{schema_name}")

          convert_stringio = ->(value) {
            case value
            when Hash
              value.each_with_object({}) do |(k, v), h|
                h[k] = convert_stringio.call(v)
              end
            when Array
              value.map(&convert_stringio)
            when StringIO
              value.string
            else
              value
            end
          }

          expect(result.total).to eq 2
          expect(convert_stringio.call(result[0])).to eq convert_stringio.call(result[1])
        end
      end
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
          "bytes" => "foo",
          "string" => "bar",
        }
      end

      it do
        expect(converted_data).to eq({
          "null" => nil,
          "boolean" => false,
          "int" => 2147483647,
          "long" => 2147483648,
          "float" => 1.5,
          "double" => 1.8,
          "bytes" => ["foo"].pack("m0"),
          "string" => "bar",
        })
      end

      it_behaves_like "converter for BigQuery"
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
            "map" => {
              "key1" => "value1",
            },
            "fixed" => md5,
          }
        end

        it do
          expect(converted_data).to eq({
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
          })
        end

        it_behaves_like "converter for BigQuery"
      end

      context "with nullable" do
        context "with nullable primitive" do
          let(:schema_name) { "nullable_string" }

          let(:data) do
            {
              "nullable_string1" => "null",
              "nullable_string2" => nil,
            }
          end

          it do
            expect(converted_data).to eq({
              "nullable_string1" => "null",
              "nullable_string2" => nil,
            })
          end

          it_behaves_like "converter for BigQuery"
        end

        context "with nullable record" do
          let(:schema_name) { "nullable_record" }

          let(:data) do
            {
              "nullable_string_record1" => { "string_field" => "1" },
              "nullable_string_record2" => nil,
              "nullable_union_record1" => { "union_field" => "1" },
              "nullable_union_record2" => { "union_field" => 2 },
              "nullable_union_record3" => nil,
            }
          end

          it do
            expect(converted_data).to eq({
              "nullable_string_record1" => { "string_field" => "1" },
              "nullable_string_record2" => nil,
              "nullable_union_record1" => {
                "union_field" => {
                  "string_value" => "1",
                },
              },
              "nullable_union_record2" => {
                "union_field" => {
                  "int_value" => "2",
                },
              },
              "nullable_union_record3" => nil,
            })
          end

          it_behaves_like "converter for BigQuery"
        end

        context "with nullable array" do
          let(:schema_name) { "nullable_array" }

          let(:data) do
            {
              "nullable_string_array1" => ["1"],
              "nullable_string_array2" => nil,
              "nullable_union_array1" => ["1", 2],
              "nullable_union_array2" => nil,
            }
          end

          it do
            expect(converted_data).to eq({
              "nullable_string_array1" => ["1"],
              "nullable_string_array2" => nil,
              "nullable_union_array1" => [
                {
                  "string_value" => "1",
                },
                {
                  "int_value" => "2",
                },
              ],
              "nullable_union_array2" => nil,
            })
          end

          it_behaves_like "converter for BigQuery"
        end

        context "with nullable map" do
          let(:schema_name) { "nullable_map" }

          let(:data) do
            {
              "nullable_string_map1" => { "key1" => "1" },
              "nullable_string_map2" => nil,
              "nullable_union_map1" => { "key1" => "1", "key2" => 2 },
              "nullable_union_map2" => nil,
            }
          end

          it do
            expect(converted_data).to eq({
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
                    "int_value" => "2",
                  },
                },
              ],
              "nullable_union_map2" => nil,
            })
          end

          it_behaves_like "converter for BigQuery"
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
              "union_field" => 1,
            },
          }
        end

        it do
          expect(converted_data).to eq({
            "nested_record" => {
              "int_field" => 1,
            },
            "nested_record_with_union_type" => {
              "union_field" => {
                "int_value" => "1",
              }
            }
          })
        end

        it_behaves_like "converter for BigQuery"
      end

      context "with nested array" do
        let(:schema_name) { "nested_array" }

        let(:data) do
          {
            "nested_array" => [
              [1, 2],
            ],
            "nested_array_with_union_type" => [
              [1, "2"],
            ],
          }
        end

        it do
          # BigQuery doesn't seem to support nested array
          expect(converted_data).to eq({
            "nested_array" => [1, 2],
            "nested_array_with_union_type" => [
              {
                "int_value" => "1",
              },
              {
                "string_value" => "2",
              },
            ],
          })
        end

        it_behaves_like "converter for BigQuery"
      end

      context "with nested map" do
        let(:schema_name) { "nested_map" }

        let(:data) do
          {
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
          }
        end

        it do
          expect(converted_data).to eq({
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
                    "int_value" => "1",
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
          })
        end

        it_behaves_like "converter for BigQuery"
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
          expect(converted_data).to eq({
            "array_field" => [
              { "int_field" => 1 },
              { "int_field" => 2 },
            ],
          })
        end

        it_behaves_like "converter for BigQuery"
      end

      context "with simple union type" do
        let(:schema_name) { "complex_types_with_simple_union_type" }

        let(:data) do
          {
            "simple_union" => "str",
          }
        end

        context "without formatter" do
          it do
            expect(converted_data).to eq({
              "simple_union" => {
                "string_value" => "str",
              },
            })
          end
        end

        context "with formatter" do
          before do
            converter.union_type_key_formatter = ->(type) { type.split("_").first }
          end

          it do
            expect(converted_data).to eq({
              "simple_union" => {
                "string" => "str",
              },
            })
          end
        end

        it_behaves_like "converter for BigQuery"
      end

      context "with complex union type" do
        let(:schema_name) { "complex_types_with_complex_union_type" }

        context "with string value" do
          let(:data) do
            {
              "complex_union" => "str",
            }
          end

          it do
            expect(converted_data).to eq({
              "complex_union" => {
                "string_value" => "str"
              },
            })
          end

          it_behaves_like "converter for BigQuery"
        end

        context "with record value" do
          let(:data) do
            {
              "complex_union" => {
                "int_field" => 1,
              }
            }
          end

          it do
            expect(converted_data).to eq({
              "complex_union" => {
                "with_int_field_value" => {
                  "int_field" => 1
                },
              },
            })
          end

          it_behaves_like "converter for BigQuery"
        end

        context "with array value" do
          let(:data) do
            {
              "complex_union" => ["1"],
            }
          end

          it do
            expect(converted_data).to eq({
              "complex_union" => {
                "array_string_value" => ["1"],
              },
            })
          end

          it_behaves_like "converter for BigQuery"
        end

        context "with map value" do
          let(:data) do
            {
              "complex_union" => { "a" => "1" },
            }
          end

          it do
            expect(converted_data).to eq({
              "complex_union" => {
                "map_string_value" => [
                  { "key" => "a", "value" => "1"},
                ],
              },
            })
          end

          it_behaves_like "converter for BigQuery"
        end
      end

      context "with union array" do
        let(:schema_name) { "complex_types_with_union_array" }

        let(:data) do
          {
            "union_array" => [
              "red",
              [1, "2"],
              uuid,
              {
                "a" => 1,
                "b" => nil,
                "c" => "3",
              },
            ],
          }
        end

        let(:uuid) { "98e73f15-db77-4424-94f8-8c8745920e59" }

        it do
          expect(converted_data).to eq({
            "union_array" => [
              {
                "color_value" => "red",
              },
              {
                "array_union_long_string_value" => [
                  {
                    "long_value" => "1",
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
                      "long_value" => "1",
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
            ],
          })
        end

        it_behaves_like "converter for BigQuery"
      end

      context "with union map" do
        let(:schema_name) { "complex_types_with_union_map" }

        let(:md5) { ["58e53d1324eef6265fdb97b08ed9aadf"].pack("H*") }

        let(:data) do
          {
            "union_map" => {
              "md5" => md5,
              "array" => [1, "2"],
              "map" => {
                "a" => 1,
                "b" => nil,
                "c" => "3",
              },
            },
          }
        end

        it do
          expect(converted_data).to eq({
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
                      "long_value" => "1",
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
                        "long_value" => "1",
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
          })
        end

        it_behaves_like "converter for BigQuery"
      end
    end

    context "logical types" do
      let(:schema_name) { "logical_types" }

      let(:now) { Time.now.utc }

      let(:data) do
        {
          "date" => (now.to_date - Date.new(1970, 1, 1)).to_i,
          "time_millis" => (now.sec + now.min * 60 + now.hour * 60**2) * 10**3 + now.nsec / 10**6,
          "time_micros" => (now.sec + now.min * 60 + now.hour * 60**2) * 10**6 + now.nsec / 10**3,
          "timestamp_millis" => now.to_i * 10**3 + now.nsec / 10**6,
          "timestamp_micros" => now.to_i * 10**6 + now.nsec / 10**3,
          "date_array" => [(now.to_date - Date.new(1970, 1, 1)).to_i],
          "date_map" => { "today" => (now.to_date - Date.new(1970, 1, 1)).to_i },
          "date_in_date_or_timestamp_millis" => (now.to_date - Date.new(1970, 1, 1)).to_i,
          "timestamp_in_date_or_timestamp_millis" => now.to_i * 10**3 + now.nsec / 10**6,
        }
      end

      it do
        expect(converted_data).to eq({
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
        })
      end

      it_behaves_like "converter for BigQuery"
    end

    context "with invalid data" do
      context "with unknown record field" do
        let(:schema_name) { "primitive_types" }

        let(:data) do
          {
            "unknown_field" => nil,
          }
        end

        it do
          expect { converted_data }.to raise_error(TypedData::Schema::UnknownField)
        end
      end

      context "with invalid union type value" do
        let(:schema_name) { "nullable_string" }

        let(:data) do
          {
            "nullable_string1" => 1,
          }
        end

        it do
          expect { converted_data }.to raise_error(TypedData::Schema::InvalidValue)
        end
      end

      context "with invalid record type value" do
        let(:schema_name) { "records_in_array" }

        let(:data) do
          {
            "array_field" => [
              { int_field: 1 },
            ],
          }
        end

        it do
          expect { converted_data }.to raise_error(TypedData::Schema::InvalidValue)
        end
      end
    end
  end
end
