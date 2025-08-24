#pragma once
#include <string>
#include <string_view>
#include <cstdint>

namespace inmemdb {

enum class TokenType {
    End,        // end of input
    Identifier,
    Integer,
    String,
    Comma,
    LParen, // left parenthesis
    RParen, // right parenthesis
    Semicolon,
    Star,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    KeywordCreate,
    KeywordTable,
    KeywordInsert,
    KeywordInto,
    KeywordValues,
    KeywordSelect,
    KeywordFrom,
    KeywordWhere,
    KeywordInt,
    KeywordText,
    KeywordJoin,
    KeywordInner,
    KeywordOn,
    Dot,
};

struct Token {
    TokenType type;
    std::string text; 
    std::size_t pos{}; 
};

inline const char* to_string(TokenType t) {
    switch(t) {
        case TokenType::End: return "End";
        case TokenType::Identifier: return "Identifier";
        case TokenType::Integer: return "Integer";
        case TokenType::String: return "String";
        case TokenType::Comma: return ",";
        case TokenType::LParen: return "(";
        case TokenType::RParen: return ")";
        case TokenType::Semicolon: return ";";
        case TokenType::Star: return "*";
        case TokenType::Equal: return "=";
        case TokenType::NotEqual: return "!=";
        case TokenType::Less: return "<";
        case TokenType::LessEqual: return "<=";
        case TokenType::Greater: return ">";
        case TokenType::GreaterEqual: return ">=";
        case TokenType::KeywordCreate: return "CREATE";
        case TokenType::KeywordTable: return "TABLE";
        case TokenType::KeywordInsert: return "INSERT";
        case TokenType::KeywordInto: return "INTO";
        case TokenType::KeywordValues: return "VALUES";
        case TokenType::KeywordSelect: return "SELECT";
        case TokenType::KeywordFrom: return "FROM";
        case TokenType::KeywordWhere: return "WHERE";
        case TokenType::KeywordInt: return "INT";
        case TokenType::KeywordText: return "TEXT";
        case TokenType::KeywordJoin: return "JOIN";
        case TokenType::KeywordInner: return "INNER";
        case TokenType::KeywordOn: return "ON";
        case TokenType::Dot: return ".";
    }
    return "?";
}

}
