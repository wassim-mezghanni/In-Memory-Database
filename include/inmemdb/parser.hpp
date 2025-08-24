#pragma once
#include <string>
#include <vector>
#include <variant>
#include <optional>
#include "inmemdb/lexer.hpp"

namespace inmemdb {

enum class ColumnType { Int, Text };

struct ColumnDef { 
    std::string name; 
    ColumnType type; 
};

struct CreateTableStmt { 
    std::string table; 
    std::vector<ColumnDef> columns; };

struct InsertStmt { 
    std::string table; 
    std::vector<std::string> values; };

struct WhereCond { 
    std::string column; 
    std::string op;  // = != < > <= >=
    std::string value; 
}; 

struct JoinClause {
    std::string right_table;
    std::string left_col;  // column name on left table
    std::string right_col; // column name on right table
};

struct SelectStmt { 
    std::vector<std::string> columns; // may include qualified names like t.col
    std::string table; // left table
    std::optional<JoinClause> join; // optional INNER JOIN
    std::optional<WhereCond> where; 
    bool select_all = false; 
};

using Statement = std::variant<CreateTableStmt, InsertStmt, SelectStmt>;

class Parser {
public:
    explicit Parser(Lexer lex) : lex_(std::move(lex)) {}
    std::vector<Statement> parse_all();
private:
    Statement parse_statement();
    CreateTableStmt parse_create();
    InsertStmt parse_insert();
    SelectStmt parse_select();

    // helpers
    std::string parse_column_name(); // identifier or qualified identifier

    bool accept(TokenType t);
    void expect(TokenType t, const char* msg);
    Token const& current();
    void advance();

    Lexer lex_;
    Token tok_{TokenType::End, "", 0};
    bool started_ = false;
};

} // namespace inmemdb
