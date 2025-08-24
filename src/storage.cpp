#include "inmemdb/storage.hpp"
#include <stdexcept>
#include <sstream>
#include <variant>
#include <optional>
#include <cstdlib>
#include <cerrno>

namespace inmemdb {

static std::string to_string(Value const& v) {
    if (auto pi = std::get_if<int64_t>(&v)) return std::to_string(*pi);
    return *std::get_if<std::string>(&v);
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
    if (auto ai = std::get_if<int64_t>(&a)) {
        auto bi = std::get_if<int64_t>(&b);
        if (!bi) throw std::runtime_error("Type mismatch in comparison");
        if (*ai < *bi) return -1; if (*ai > *bi) return 1; return 0;
    } else {
        auto const* as = std::get_if<std::string>(&a);
        auto const* bs = std::get_if<std::string>(&b);
        if (!as || !bs) throw std::runtime_error("Type mismatch in comparison");
        if (*as < *bs) return -1; if (*as > *bs) return 1; return 0;
    }
}

// Resolve a possibly qualified column name against up to two tables.
// Returns pair<tableSelector, index> where tableSelector: 0 for left, 1 for right.
static std::pair<int, size_t> resolve_column(
    std::string const& colspec,
    Table const& left,
    Table const* right // nullable when no join
) {
    // qualified form t.col: split on '.'
    auto dot = colspec.find('.');
    if (dot != std::string::npos) {
        std::string tname = colspec.substr(0, dot);
        std::string cname = colspec.substr(dot + 1);
        if (tname == left.name) {
            for (size_t i = 0; i < left.columns.size(); ++i)
                if (left.columns[i].name == cname) return {0, i};
            throw std::runtime_error("Unknown column: " + colspec);
        } else if (right && tname == right->name) {
            for (size_t i = 0; i < right->columns.size(); ++i)
                if (right->columns[i].name == cname) return {1, i};
            throw std::runtime_error("Unknown column: " + colspec);
        } else {
            throw std::runtime_error("Unknown table qualifier: " + tname);
        }
    }
    // unqualified: prefer left, then right if present; error if ambiguous
    std::optional<size_t> lidx;
    for (size_t i = 0; i < left.columns.size(); ++i) if (left.columns[i].name == colspec) { lidx = i; break; }
    std::optional<size_t> ridx;
    if (right) {
        for (size_t i = 0; i < right->columns.size(); ++i) if (right->columns[i].name == colspec) { ridx = i; break; }
    }
    if (lidx && ridx) throw std::runtime_error("Ambiguous column name: " + colspec);
    if (lidx) return {0, *lidx};
    if (ridx) return {1, *ridx};
    throw std::runtime_error("Unknown column: " + colspec);
}

QueryResult Database::select_rows(SelectStmt const& stmt) const {
    QueryResult qr;
    auto itL = tables_.find(stmt.table);
    if (itL == tables_.end()) { qr.success = false; qr.message = "Unknown table"; return qr; }
    Table const& left = itL->second;

    if (!stmt.join) {
        // Single-table path (existing behavior)
        std::vector<size_t> col_indexes;
        if (stmt.select_all) {
            for (size_t i = 0; i < left.columns.size(); ++i) col_indexes.push_back(i);
            for (auto const& c : left.columns) qr.header.push_back(c.name);
        } else {
            for (auto const& name : stmt.columns) {
                auto idx = left.find_column(name);
                if (!idx) { qr.success = false; qr.message = "Unknown column: " + name; return qr; }
                col_indexes.push_back(*idx);
            }
            qr.header = stmt.columns;
        }

        std::optional<size_t> where_col_idx;
        Value where_value;
        std::string where_op;
        if (stmt.where) {
            auto idx = left.find_column(stmt.where->column);
            if (!idx) { qr.success = false; qr.message = "Unknown column in WHERE: " + stmt.where->column; return qr; }
            where_col_idx = *idx;
            where_op = stmt.where->op;
            auto const& meta = left.columns[*idx];
            if (meta.type == ColumnType::Int) {
                int64_t v{}; auto const& raw = stmt.where->value;
                if (!parse_int64(raw, v)) { qr.success = false; qr.message = "Expected integer in WHERE for column " + meta.name; return qr; }
                where_value = v;
            } else { where_value = stmt.where->value; }
        }

        for (auto const& row : left.rows) {
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
                } catch (std::exception const& ex) { qr.success = false; qr.message = ex.what(); return qr; }
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

    // JOIN path
    auto itR = tables_.find(stmt.join->right_table);
    if (itR == tables_.end()) { qr.success = false; qr.message = "Unknown right table in JOIN"; return qr; }
    Table const& right = itR->second;

    // Resolve JOIN columns
    auto [lSel, lIdx] = resolve_column(stmt.join->left_col, left, &right);
    auto [rSel, rIdx] = resolve_column(stmt.join->right_col, left, &right);
    if (!(lSel == 0 && rSel == 1)) { qr.success = false; qr.message = "JOIN condition must be left_col from left table and right_col from right table"; return qr; }

    // Type compatibility check
    auto const& lmeta = left.columns[lIdx];
    auto const& rmeta = right.columns[rIdx];
    if (lmeta.type != rmeta.type) { qr.success = false; qr.message = "Type mismatch in JOIN columns"; return qr; }

    // Build projection plan: for select_all, list left.* then right.* headers as qualified names; otherwise resolve each requested column
    struct Proj { int sel; size_t idx; }; // sel: 0 left, 1 right
    std::vector<Proj> proj;
    if (stmt.select_all) {
        for (size_t i = 0; i < left.columns.size(); ++i) { proj.push_back({0, i}); qr.header.push_back(left.name + "." + left.columns[i].name); }
        for (size_t i = 0; i < right.columns.size(); ++i) { proj.push_back({1, i}); qr.header.push_back(right.name + "." + right.columns[i].name); }
    } else {
        for (auto const& c : stmt.columns) {
            auto [s, idx] = resolve_column(c, left, &right);
            proj.push_back({s, idx});
            qr.header.push_back(c);
        }
    }

    // Prepare WHERE if present
    std::optional<std::pair<int,size_t>> where_sel_idx;
    Value where_value;
    std::string where_op;
    if (stmt.where) {
        auto [s, idx] = resolve_column(stmt.where->column, left, &right);
        where_sel_idx = std::make_pair(s, idx);
        where_op = stmt.where->op;
        auto const& meta = (s==0 ? left.columns[idx] : right.columns[idx]);
        if (meta.type == ColumnType::Int) {
            int64_t v{}; auto const& raw = stmt.where->value;
            if (!parse_int64(raw, v)) { qr.success = false; qr.message = "Expected integer in WHERE for column " + meta.name; return qr; }
            where_value = v;
        } else { where_value = stmt.where->value; }
    }

    // Nested-loop INNER JOIN
    for (auto const& lrow : left.rows) {
        Value const& lv = lrow.values[lIdx];
        for (auto const& rrow : right.rows) {
            Value const& rv = rrow.values[rIdx];
            bool match = false;
            try { match = (cmp(lv, rv) == 0); }
            catch (std::exception const& ex) { qr.success = false; qr.message = ex.what(); return qr; }
            if (!match) continue;

            // WHERE
            if (where_sel_idx) {
                Value const& cv = (where_sel_idx->first==0 ? lrow.values[where_sel_idx->second] : rrow.values[where_sel_idx->second]);
                int cval = cmp(cv, where_value);
                bool include = false;
                if (where_op == "=") include = (cval == 0);
                else if (where_op == "!=") include = (cval != 0);
                else if (where_op == "<") include = (cval < 0);
                else if (where_op == "<=") include = (cval <= 0);
                else if (where_op == ">") include = (cval > 0);
                else if (where_op == ">=") include = (cval >= 0);
                else { qr.success = false; qr.message = "Unsupported operator"; return qr; }
                if (!include) continue;
            }

            // Project
            std::vector<std::string> outrow;
            outrow.reserve(proj.size());
            for (auto const& p : proj) {
                outrow.push_back(to_string(p.sel==0 ? lrow.values[p.idx] : rrow.values[p.idx]));
            }
            qr.rows.push_back(std::move(outrow));
        }
    }

    qr.message = std::to_string(qr.rows.size()) + " row(s)";
    return qr;
}

} // namespace inmemdb
