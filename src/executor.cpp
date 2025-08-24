#include "inmemdb/executor.hpp"
#include <variant>
#include <type_traits>

namespace inmemdb {

QueryResult Executor::execute(Statement const& stmt) {
    if (stmt.index() == 0) { // CreateTableStmt
        auto const& s = std::get<0>(stmt);
        QueryResult qr; qr.header = {}; qr.success = true;
        try { db_.create_table(s); qr.message = "Table created"; }
        catch (std::exception const& ex) { qr.success = false; qr.message = ex.what(); }
        return qr;
    }
    if (stmt.index() == 1) { // InsertStmt
        auto const& s = std::get<1>(stmt);
        QueryResult qr; qr.header = {}; qr.success = true;
        try { db_.insert_row(s); qr.message = "1 row inserted"; }
        catch (std::exception const& ex) { qr.success = false; qr.message = ex.what(); }
        return qr;
    }
    if (stmt.index() == 2) { // SelectStmt
        auto const& s = std::get<2>(stmt);
        return db_.select_rows(s);
    }
    return {};
}

} // namespace inmemdb
