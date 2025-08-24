#pragma once
#include <string>
#include <vector>
#include <variant>
#include <unordered_map>
#include <optional>
#include "inmemdb/parser.hpp"

namespace inmemdb {

using Value = std::variant<int64_t, std::string>;

struct ColumnMeta {
    std::string name;
    ColumnType type;
};

struct Row {
    std::vector<Value> values; 
};

struct Table {
    std::string name;
    std::vector<ColumnMeta> columns;
    std::vector<Row> rows;

    std::optional<size_t> find_column(std::string const& col) const {
        for (size_t i = 0; i < columns.size(); ++i) 
            if (columns[i].name == col) return i;
        return std::nullopt;
    }
};

struct QueryResult {
    bool success = true;
    std::string message;
    std::vector<std::string> header;
    std::vector<std::vector<std::string>> rows;
};

class Database {
public:
    Table& create_table(CreateTableStmt const& stmt);
    void insert_row(InsertStmt const& stmt);
    QueryResult select_rows(SelectStmt const& stmt) const;
private:
    std::unordered_map<std::string, Table> tables_;
};

} // namespace inmemdb
