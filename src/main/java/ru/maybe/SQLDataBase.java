package ru.maybe;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static ru.maybe.SQLDataBase.SQLQuery.*;

class SQLDataBase {
    private static final Logger logger = (Logger) LoggerFactory.getLogger(H2DataBase.class.getName());
    private static Connection connection;
    private List<Table> tables = new ArrayList<>();

    public static Connection getConnection() {
        return connection;
    }

    public void setConnection(String jdbc) {
        try {
            connection = DriverManager.getConnection(jdbc);
        } catch (SQLException e) {
            logger.error("Error while initializing connection");
        }
    }


    private static Map sqlQueryExec(String sql, String... returnedColumnsHeaders) {
        logger.debug("SQL =>:\t{}", sql);
        Map<String, List<String>> columns = new HashMap<>();
        try (Statement stmt = connection.createStatement()) {
            if (returnedColumnsHeaders.length > 0) {
                logger.info("SQL =>:\tColumns requested: {}", Arrays.stream(returnedColumnsHeaders).collect(Collectors.joining(", ")));
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        for (String c : returnedColumnsHeaders) {
                            List<String> column = columns.getOrDefault(c, new ArrayList<>());
                            String value = rs.getObject(c).toString();
                            column.add(value);
                            columns.put(c, column);
                        }
                    }
                }
                logger.info("SQL <=:\t{}", columns);
            } else {
                stmt.execute(sql);
            }
            connection.commit();
        } catch (SQLException e) {
            logger.error("Error executing the query: " + sql, e);
        }
        return columns;
    }

    public static String buildQuery(SQLQuery command, Table table, String[] values, String... additionalParams) {
        StringBuilder query = new StringBuilder();
        String valuesAsString = Arrays.stream(values).collect(Collectors.joining(", "));
        switch (command) {
            case SHOW_TABLES:
                query.append("SHOW TABLES");
                break;
            case INSERT:
                query.append(String.format("INSERT INTO %s VALUES ( %s )", table.name, valuesAsString));
                break;
            case DROP:
                query.append(String.format("DROP TABLE %s", table.name));
                break;
            case CREATE:
                StringBuilder headers = new StringBuilder();
                for (Table.Structure.Cell val : table.structure.data) {
                    headers.append(val.getName()).append(" ").append(val.getType().getValue()).append(", ");
                }
                query.append(String.format("CREATE TABLE %s ( %s )", table.name, headers.toString().substring(0, headers.toString().length() - 2)));
                break;
            case SELECT:
                query.append(String.format("SELECT %s FROM %s ", valuesAsString, table.name));
                break;
            default:
                query.append("NULL");
        }
        if (additionalParams.length > 0) {
            query.append(' ').append(Arrays.stream(additionalParams).collect(Collectors.joining(",")));
            if (additionalParams.length > 1)
                query.deleteCharAt(query.length() - 1);
        }
        return query.append(';').toString();
    }


    public void createTable(Table table) {
        String query = buildQuery(CREATE, table, new String[0]);
        this.tables.add(table);
        sqlQueryExec(query);
    }

    public List getDbTables() {
        String query = SHOW_TABLES.getCommandFormat();
        return (List) sqlQueryExec(query, "TABLE_NAME").get("TABLE_NAME");
    }

    public void flushTables() {
        logger.debug("Setup tables for the new DataBase");
        for (Table table : this.tables) {
            sqlQueryExec(buildQuery(DROP, table, new String[0]));
            logger.debug("Table {0} dropped.", table);
        }
    }


    enum SQLQuery {
        INSERT("INSERT INTO %s VALUES ( %s );"),
        SELECT("SELECT %s FROM %s %s;"),
        CREATE("CREATE TABLE %s ( %s );"),
        DROP("DROP TABLE %s;"),
        SHOW_TABLES("SHOW TABLES;");
        String commandFormat;

        SQLQuery(String cmdFormat) {
            this.commandFormat = cmdFormat;
        }

        String getCommandFormat() {
            return commandFormat;
        }
    }

    static class Table {
        String name;
        Structure structure;
        Map[][] data;
        List rows = null;
        List columns = null;

        Table(String name, Structure structure) {
            this.name = name;
            this.structure = structure;
        }

        public void drop() {
            buildQuery(DROP, this, new String[0]);
        }

        public void insert(String[] values, String... additionalParams) {
            String query = buildQuery(INSERT, this, values, additionalParams);
            sqlQueryExec(query);
        }

        public Map select(String[] values, String[] returnedHeaders, String... params) {
            String query = buildQuery(SELECT, this, values, params);
            return sqlQueryExec(query, returnedHeaders);
        }


        static class Structure {
            Cell[] data;
            String[] parameters;

            Structure(Cell[] data, String... recordParameters) {
                this.data = data;
                this.parameters = recordParameters;
            }

            Cell[] getData() {
                return this.data;
            }

            enum SQLType {
                INT("INT"),
                DATE("DATE"),
                VARCHAR("VARCHAR(255)");
                String value;

                SQLType(String value) {
                    this.value = value;
                }

                public String getValue() {
                    return value;
                }
            }

            static class Cell {
                String name;
                SQLType type;
                String[] parameters;

                Cell(String name, SQLType type, String... valueParams) {
                    this.name = name;
                    this.type = type;
                    if (valueParams.length > 0)
                        this.parameters = valueParams;
                }

                public String getName() {
                    return this.name;
                }

                public SQLType getType() {
                    return this.type;
                }

            }
        }
    }
}

