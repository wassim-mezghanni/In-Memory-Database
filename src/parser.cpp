#include "inmemdb/parser.hpp"
#include <stdexcept>
#include <cctype>
#include <optional>

namespace inmemdb {

Token const& Parser::current() {
    if (!started_) { tok_ = lex_.next(); started_ = true; }
    return tok_;
}

void Parser::advance() { tok_ = lex_.next(); }

bool Parser::accept(TokenType t) {
    if (current().type == t) { 
        advance(); return true; 
    }
    return false;
}

// Throws if the current token is not of the expected type
void Parser::expect(TokenType t, const char* msg) {
    if (!accept(t)) throw std::runtime_error(msg);
}

std::vector<Statement> Parser::parse_all() {
    std::vector<Statement> out;
    while (true) {
        if (current().type == TokenType::End) break;
        out.push_back(parse_statement());
        // optional semicolon between statements
        if (accept(TokenType::Semicolon)) {
            // consume any extra semicolons
            while (accept(TokenType::Semicolon)) {}
        }
    }
    return out;
}

Statement Parser::parse_statement() {
    switch (current().type) {
        case TokenType::KeywordCreate: return parse_create();
        case TokenType::KeywordInsert: return parse_insert();
        case TokenType::KeywordSelect: return parse_select();
        default: throw std::runtime_error("Expected a statement (CREATE/INSERT/SELECT)");
    }
}

CreateTableStmt Parser::parse_create() {
    expect(TokenType::KeywordCreate, "Expected CREATE");
    expect(TokenType::KeywordTable, "Expected TABLE after CREATE");
    if (current().type != TokenType::Identifier) throw std::runtime_error("Expected table name");
    std::string table = current().text; advance();
    expect(TokenType::LParen, "Expected '('");

    // parse column
    std::vector<ColumnDef> columns;
    bool first = true;
    while (current().type != TokenType::RParen) {
        if (!first) expect(TokenType::Comma, "Expected ',' between column definitions");
        first = false;
        if (current().type != TokenType::Identifier) throw std::runtime_error("Expected column name");
        std::string colname = current().text; advance();
        ColumnType ctype;
        if (accept(TokenType::KeywordInt)) ctype = ColumnType::Int;
        else if (accept(TokenType::KeywordText)) ctype = ColumnType::Text;
        else throw std::runtime_error("Expected column type INT or TEXT");
        columns.push_back({colname, ctype});
    }
    expect(TokenType::RParen, "Expected ')' after column list");
    return CreateTableStmt{table, std::move(columns)};
}

InsertStmt Parser::parse_insert() {
    expect(TokenType::KeywordInsert, "Expected INSERT");
    expect(TokenType::KeywordInto, "Expected INTO after INSERT");
    if (current().type != TokenType::Identifier) throw std::runtime_error("Expected table name after INSERT INTO");
    std::string table = current().text; advance();
    expect(TokenType::KeywordValues, "Expected VALUES");
    expect(TokenType::LParen, "Expected '(' after VALUES");

    std::vector<std::string> values;
    bool first = true;
    while (current().type != TokenType::RParen) {
        if (!first) expect(TokenType::Comma, "Expected ',' between values");
        first = false;
        if (current().type == TokenType::Integer || current().type == TokenType::String || current().type == TokenType::Identifier) {
            values.push_back(current().text);
            advance();
        } else {
            throw std::runtime_error("Expected literal value");
        }
    }
    expect(TokenType::RParen, "Expected ')' after values");
    return InsertStmt{table, std::move(values)};
}

SelectStmt Parser::parse_select() {
    expect(TokenType::KeywordSelect, "Expected SELECT");
    SelectStmt stmt;
    // check for SELECT *
    if (accept(TokenType::Star)) {
        stmt.select_all = true;
    } else {
        bool first = true;
        while (true) {
            if (!first) expect(TokenType::Comma, "Expected ',' between column names");
            first = false;
            if (current().type != TokenType::Identifier) throw std::runtime_error("Expected column name in select list");
            stmt.columns.push_back(current().text);
            advance();
            if (current().type != TokenType::Comma) break;
        }
    }
    // check for FROM
    expect(TokenType::KeywordFrom, "Expected FROM");
    // check for table name
    if (current().type != TokenType::Identifier) throw std::runtime_error("Expected table name after FROM");
    stmt.table = current().text; advance();
    
    // check for optional WHERE clause
    if (accept(TokenType::KeywordWhere)) {
        if (current().type != TokenType::Identifier) throw std::runtime_error("Expected column name in WHERE");
        std::string col = current().text; advance();
        std::string op;
        switch(current().type) {
            case TokenType::Equal: op = "="; break;
            case TokenType::NotEqual: op = "!="; break;
            case TokenType::Less: op = "<"; break;
            case TokenType::LessEqual: op = "<="; break;
            case TokenType::Greater: op = ">"; break;
            case TokenType::GreaterEqual: op = ">="; break;
            default: throw std::runtime_error("Expected comparison operator in WHERE");
        }
        advance();
        if (current().type != TokenType::Integer && current().type != TokenType::String && current().type != TokenType::Identifier)
            throw std::runtime_error("Expected literal value in WHERE");
        std::string value = current().text; advance();
        stmt.where = WhereCond{col, op, value};
    }
    return stmt;
}

} 
