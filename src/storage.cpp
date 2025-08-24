#include "inmemdb/storage.hpp"
#include <stdexcept>
#include <sstream>
#include <variant>
#include <optional>
#include <cstdlib>
#include <cerrno>

namespace inmemdb {

static std::string to_string(Value const& v) {
    if (v.index() == 0) return std::to_string(std::get<0>(v)); 
    return std::get<1>(v); 
}

// Integer parsing
static bool parse_int64(const std::string& raw, int64_t& out) {
    errno = 0;
    char* end = nullptr;
    long long v = std::strtoll(raw.c_str(), &end, 10);
    if (errno != 0 || end == raw.c_str() || *end != '\0') return false;
    out = static_cast<int64_t>(v);
    return true;
}
// Create a new table
Table& Database::create_table(CreateTableStmt const& stmt) {
    if (tables_.find(stmt.table) != tables_.end()) 
        throw std::runtime_error("Table already exists: " + stmt.table);
    Table t; t.name = stmt.table;
    for (auto const& c : stmt.columns) 
        t.columns.push_back({c.name, c.type});
    auto [it, _] = tables_.emplace(stmt.table, std::move(t));
    return it->second;
}

// Insert a row into a table
void Database::insert_row(InsertStmt const& stmt) {
    auto it = tables_.find(stmt.table);
    if (it == tables_.end()) throw std::runtime_error("Unknown table: " + stmt.table);
    Table& tbl = it->second;
    if (tbl.columns.size() != stmt.values.size()) throw std::runtime_error("Column count mismatch in INSERT");

    Row row;
    row.values.reserve(tbl.columns.size());
    for (size_t i = 0; i < tbl.columns.size(); ++i) {
        auto const& meta = tbl.columns[i];
        auto const& raw = stmt.values[i];
        if (meta.type == ColumnType::Int) {
            int64_t v{};
            if (!parse_int64(raw, v)) throw std::runtime_error("Expected integer for column " + meta.name);
            row.values.push_back(v);
        } else { // Text
            row.values.push_back(raw);
        }
    }
    tbl.rows.push_back(std::move(row));
}

// Comparison helper
static int cmp(Value const& a, Value const& b) {
    if (a.index() != b.index()) throw std::runtime_error("Type mismatch in comparison");
    if (a.index() == 0) { // int64_t
        auto av = std::get<0>(a); auto bv = std::get<0>(b);
        if (av < bv) return -1; if (av > bv) return 1; return 0;
    } else { // std::string
        auto const& as = std::get<1>(a); auto const& bs = std::get<1>(b);
        if (as < bs) return -1; if (as > bs) return 1; return 0;
    }
}

QueryResult Database::select_rows(SelectStmt const& stmt) const {
    QueryResult qr;
    auto it = tables_.find(stmt.table);
    if (it == tables_.end()) { qr.success = false; qr.message = "Unknown table"; return qr; }
    Table const& tbl = it->second;

    std::vector<size_t> col_indexes;
    if (stmt.select_all) {
        for (size_t i = 0; i < tbl.columns.size(); ++i) col_indexes.push_back(i);
    } else {
        for (auto const& name : stmt.columns) {
            std::optional<size_t> idx;
            for (size_t i = 0; i < tbl.columns.size(); ++i) {
                if (tbl.columns[i].name == name) { idx = i; break; }
            }
            if (!idx) { qr.success = false; qr.message = "Unknown column: " + name; return qr; }
            col_indexes.push_back(*idx);
        }
    }

  
    if (stmt.select_all) {
        for (auto const& c : tbl.columns) qr.header.push_back(c.name);
    } else {
        qr.header = stmt.columns;
    }

    std::optional<size_t> where_col_idx;
    Value where_value;
    std::string where_op;
    if (stmt.where) {
        // find column index
        std::optional<size_t> idx;
        for (size_t i = 0; i < tbl.columns.size(); ++i) {
            if (tbl.columns[i].name == stmt.where->column) { idx = i; break; }
        }
        if (!idx) { 
            qr.success = false; 
            qr.message = "Unknown column in WHERE: " + stmt.where->column; 
            return qr; 
        }
        where_col_idx = *idx;
        where_op = stmt.where->op;
        // parse value into appropriate type
        auto const& meta = tbl.columns[*idx];
        if (meta.type == ColumnType::Int) {
            int64_t v{}; auto const& raw = stmt.where->value;
            if (!parse_int64(raw, v)) { qr.success = false; qr.message = "Expected integer in WHERE for column " + meta.name; return qr; }
            where_value = v;
        } else {
            where_value = stmt.where->value;
        }
    }

    // filter rows
    for (auto const& row : tbl.rows) {
        bool include = true;
        if (where_col_idx) {
            try {
                int cval = cmp(row.values[*where_col_idx], where_value);
                if (where_op == "=") include = (cval == 0);
                else if (where_op == "!=") include = (cval != 0);
                else if (where_op == "<") include = (cval < 0);
                else if (where_op == "<=") include = (cval <= 0);
                else if (where_op == ">") include = (cval > 0);
                else if (where_op == ">=") include = (cval >= 0);
                else throw std::runtime_error("Unsupported operator");
            } catch (std::exception const& ex) {
                qr.success = false; qr.message = ex.what(); return qr; }
        }
        if (!include) continue;
        std::vector<std::string> outrow;
        outrow.reserve(col_indexes.size());
        for (auto idx : col_indexes) outrow.push_back(to_string(row.values[idx]));
        qr.rows.push_back(std::move(outrow));
    }
    qr.message = std::to_string(qr.rows.size()) + " row(s)";
    return qr;
}

}
