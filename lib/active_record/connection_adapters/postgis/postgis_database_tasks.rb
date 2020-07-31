# frozen_string_literal: true

module ActiveRecord  # :nodoc:
  module ConnectionAdapters  # :nodoc:
    module PostGIS  # :nodoc:
      class PostGISDatabaseTasks < ::ActiveRecord::Tasks::PostgreSQLDatabaseTasks  # :nodoc:
        def initialize(db_config)
          super
          ensure_installation_configs
        end

        def setup_gis
          if extension_names
            setup_gis_from_extension
          end
          establish_connection(db_config)
        end

        # Override to set the database owner and call setup_gis
        def create(master_established = false)
          establish_master_connection unless master_established
          connection.create_database(db_config.database, configuration_hash.merge(extra_configs))
          setup_gis
        rescue ::ActiveRecord::StatementInvalid => error
          if /database .* already exists/ === error.message
            raise ::ActiveRecord::Tasks::DatabaseAlreadyExists
          else
            raise
          end
        end

        private

        def search_path
          @search_path ||= configuration_hash[:schema_search_path].to_s.strip.split(",").map(&:strip)
        end

        def extension_names
          @extension_names ||= begin
            extensions = configuration_hash[:postgis_extension]
            case extensions
            when ::String
              extensions.split(",")
            when ::Array
              extensions
            else
              ["postgis"]
            end
          end
        end

        def ensure_installation_configs
          configuration_hash[:postgis_extension] = "postgis"
        end

        def setup_gis_from_extension
          extension_names.each do |extname|
            if extname == "postgis_topology"
              unless search_path.include?("topology")
                raise ArgumentError, "'topology' must be in schema_search_path for postgis_topology"
              end
              connection.execute("CREATE EXTENSION IF NOT EXISTS #{extname} SCHEMA topology")
            else
              if (postgis_schema = configuration_hash[:postgis_schema])
                schema_clause = "WITH SCHEMA #{postgis_schema}"
                unless schema_exists?(postgis_schema)
                  connection.execute("CREATE SCHEMA #{postgis_schema}")
                  connection.execute("GRANT ALL ON SCHEMA #{postgis_schema} TO PUBLIC")
                end
              else
                schema_clause = ""
              end

              connection.execute("CREATE EXTENSION IF NOT EXISTS #{extname} #{schema_clause}")
            end
          end
        end

        def schema_exists?(schema_name)
          connection.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '#{schema_name}'"
          ).any?
        end
      end

      ::ActiveRecord::Tasks::DatabaseTasks.register_task(/postgis/, PostGISDatabaseTasks)
    end
  end
end
