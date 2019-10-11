require 'active_record/base'
require 'active_record/connection_adapters/abstract_adapter'
require "database_cleaner/generic/truncation"
require 'database_cleaner/active_record/base'
require 'database_cleaner/active_record/truncation'

module DatabaseCleaner
  module ConnectionAdapters
    module AbstractDeleteAdapter
      def delete_table(table_name)
        raise NotImplementedError
      end
    end

    module GenericDeleteAdapter
      def delete_table(table_name)
        execute("DELETE FROM #{quote_table_name(table_name)};")
      end
    end

    module OracleDeleteAdapter
      def delete_table(table_name)
        execute("DELETE FROM #{quote_table_name(table_name)}")
      end
    end
  end
end

module ActiveRecord
  module ConnectionAdapters
    AbstractAdapter.class_eval { include DatabaseCleaner::ConnectionAdapters::AbstractDeleteAdapter }

    JdbcAdapter.class_eval { include ::DatabaseCleaner::ConnectionAdapters::GenericDeleteAdapter } if defined?(JdbcAdapter)
    AbstractMysqlAdapter.class_eval { include ::DatabaseCleaner::ConnectionAdapters::GenericDeleteAdapter } if defined?(AbstractMysqlAdapter)
    Mysql2Adapter.class_eval { include ::DatabaseCleaner::ConnectionAdapters::GenericDeleteAdapter } if defined?(Mysql2Adapter)
    SQLiteAdapter.class_eval { include ::DatabaseCleaner::ConnectionAdapters::GenericDeleteAdapter } if defined?(SQLiteAdapter)
    SQLite3Adapter.class_eval { include ::DatabaseCleaner::ConnectionAdapters::GenericDeleteAdapter } if defined?(SQLite3Adapter)
    PostgreSQLAdapter.class_eval { include ::DatabaseCleaner::ConnectionAdapters::GenericDeleteAdapter } if defined?(PostgreSQLAdapter)
    IBM_DBAdapter.class_eval { include ::DatabaseCleaner::ConnectionAdapters::GenericDeleteAdapter } if defined?(IBM_DBAdapter)
    SQLServerAdapter.class_eval { include ::DatabaseCleaner::ConnectionAdapters::GenericDeleteAdapter } if defined?(SQLServerAdapter)
    OracleEnhancedAdapter.class_eval { include ::DatabaseCleaner::ConnectionAdapters::OracleDeleteAdapter } if defined?(OracleEnhancedAdapter)
  end
end

module DatabaseCleaner::ActiveRecord
  module SelectiveTruncation
    def tables_to_truncate(connection)
      if information_schema_exists?(connection)
        (@only || tables_with_new_rows(connection)) - @tables_to_exclude
      else
        super
      end
    end

    def tables_with_new_rows(connection)
      @db_name ||= connection.instance_variable_get('@config')[:database]
      stats = table_stats_query(connection, @db_name)
      if stats != ''
        connection.exec_query(stats).inject([]) {|all, stat| all << stat['table_name'] if stat['exact_row_count'] > 0; all }
      else
        []
      end
    end

    def table_stats_query(connection, db_name)
      if @cache_tables && !@table_stats_query.nil?
        return @table_stats_query
      else
        tables = connection.select_values(<<-SQL)
          SELECT table_name
          FROM information_schema.tables
          WHERE table_schema = '#{db_name}'
          AND #{::DatabaseCleaner::ActiveRecord::Base.exclusion_condition('table_name')};
        SQL
        queries = tables.map do |table|
          "SELECT #{connection.quote(table)} AS table_name, COUNT(*) AS exact_row_count FROM #{connection.quote_table_name(table)}"
        end
        @table_stats_query = queries.join(' UNION ')
      end
    end

    def information_schema_exists? connection
      return false unless connection.is_a? ActiveRecord::ConnectionAdapters::Mysql2Adapter
      @information_schema_exists ||=
        begin
          connection.execute("SELECT 1 FROM information_schema.tables")
          true
        rescue
          false
        end
    end
  end

  class Deletion < Truncation
    if defined?(ActiveRecord::ConnectionAdapters::Mysql2Adapter)
      include SelectiveTruncation
    end

    def clean

      connection = connection_class.connection

      if ENV['DB_CLEANER_AZURE'] == 'true'

        records_array = connection.execute %{
WITH RECURSIVE t AS (
    SELECT relnamespace as nsp, oid as tbl, null::regclass as source, 1 as level
    FROM pg_class
    WHERE relkind = 'r'
        AND relnamespace not in ('pg_catalog'::regnamespace, 'information_schema'::regnamespace)
UNION ALL
    SELECT c.connamespace as nsp, c.conrelid as tbl, c.confrelid as source, p.level + 1
    FROM pg_constraint c
    INNER JOIN t p ON (c.confrelid = p.tbl AND c.connamespace = p.nsp)
    WHERE c.contype = 'f'
        AND c.connamespace not in ('pg_catalog'::regnamespace, 'information_schema'::regnamespace)
)
SELECT tbl::regclass
FROM t
GROUP BY nsp, tbl
ORDER BY max(level) DESC;
        }
       
        tables = records_array.column_values(0)  - @tables_to_exclude

        tables.each do |table_name|
          connection.delete_table table_name
        end

      else

        connection.disable_referential_integrity do
          tables_to_truncate(connection).each do |table_name|
            connection.delete_table table_name
          end
        end
      end
    end

  end
end
