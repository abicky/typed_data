require "json"
require "thor"
require "typed_data/converter"
require "typed_data/restorer"

module TypedData
  class CLI < Thor
    def self.exit_on_failure?
      true
    end

    desc "convert [file]", "Convert data in an encoding similar to Avro JSON encoding"
    long_desc <<~DESC
      This command converts data in an encoding similar to Avro JSON encoding.
      You can specify the file in JSON format or JSON Lines format.
      If the file option is ommited, the command read data from stdin.
    DESC
    option :schema, desc: "Path to Avro schema file", required: true
    option :"key-format", desc: "Format for union type key", enum: %w[bigquery avro], default: "bigquery", banner: "FORMAT"
    def convert(file = nil)
      process(TypedData::Converter, :convert, file)
    end

    desc "restore [file]", "Restore converted data"
    long_desc <<~DESC
      This command restores converted data.
      You can specify the file in JSON format or JSON Lines format.
      If the file option is ommited, the command read data from stdin.
    DESC
    option :schema, desc: "Path to Avro schema file", required: true
    option :"key-format", desc: "Format for union type key", enum: %w[bigquery avro], default: "bigquery", banner: "FORMAT"
    def restore(file = nil)
      process(TypedData::Restorer, :restore, file)
    end

    private

    def process(processor_class, method_name, file)
      abort_if_not_exist(options[:schema])
      abort_if_not_exist(file) if file

      schema = JSON.parse(File.read(options[:schema]))
      processor = processor_class.new(schema, key_formatter: options[:"key-format"].to_sym)

      input = file ? File.open(file) : $stdin
      first_line = input.readline.lstrip
      if first_line.start_with?("[")
        first_line << input.read
        JSON.parse(first_line).each do |record|
          puts processor.public_send(method_name, record).to_json
        end
      else
        records = input
        puts processor.public_send(method_name, JSON.parse(first_line)).to_json
        input.each do |line|
          puts processor.public_send(method_name, JSON.parse(line)).to_json
        end
      end
    end

    def abort_if_not_exist(file)
      unless File.exist?(file)
        $stderr.puts("#{file} doesn't exit")
        exit(1)
      end
    end
  end
end
