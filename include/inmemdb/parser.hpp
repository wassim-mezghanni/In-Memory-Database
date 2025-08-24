#pragma once
#include <string>
#include <vector>
#include <variant>
#include <optional>
#include <optional>#include "inmemdb/lexer.hpp"

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
    std::string op;  // op only = != < > <= >=
    std::string value; 
}; 

struct SelectStmt { 
    std::vector<std::string> columns; 
    std::string table; 
    std::optional<WhereCond> where; 
    bool select_all = false; // flag for SELECT *
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

    bool accept(TokenType t);
    void expect(TokenType t, const char* msg);
    Token const& current();
    void advance();

    Lexer lex_;
    Token tok_{TokenType::End, "", 0};
    bool started_ = false;
};

}
