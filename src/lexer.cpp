#include "inmemdb/lexer.hpp"
#include <cctype>
#include <stdexcept>
#include <unordered_map>

namespace inmemdb {

Lexer::Lexer(std::string input) : input_(std::move(input)) {}

// skip whitespace
void Lexer::skip_ws() {
    while (pos_ < input_.size() && std::isspace(static_cast<unsigned char>(input_[pos_]))) ++pos_;
}

// Make identifier or keyword
Token Lexer::make_identifier_or_keyword(std::size_t start) {
    while (pos_ < input_.size() && (std::isalnum(static_cast<unsigned char>(input_[pos_])) || input_[pos_] == '_')) ++pos_;
    std::string text = input_.substr(start, pos_ - start);
    std::string upper;
    upper.reserve(text.size());

    for(char c: text) upper.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
    static const std::unordered_map<std::string, TokenType> keywords = {
        {"CREATE", TokenType::KeywordCreate}, {"TABLE", TokenType::KeywordTable},
        {"INSERT", TokenType::KeywordInsert}, {"INTO", TokenType::KeywordInto},
        {"VALUES", TokenType::KeywordValues}, {"SELECT", TokenType::KeywordSelect},
        {"FROM", TokenType::KeywordFrom}, {"WHERE", TokenType::KeywordWhere},
        {"INT", TokenType::KeywordInt}, {"TEXT", TokenType::KeywordText},
        {"JOIN", TokenType::KeywordJoin}, {"INNER", TokenType::KeywordInner}, {"ON", TokenType::KeywordOn}
    };
    auto it = keywords.find(upper);
    if (it != keywords.end()) return {it->second, upper, start};
    return {TokenType::Identifier, text, start};
}

// Number parsing
Token Lexer::make_number(std::size_t start) {
    while (pos_ < input_.size() && std::isdigit(static_cast<unsigned char>(input_[pos_]))) ++pos_;
    return {TokenType::Integer, input_.substr(start, pos_ - start), start};
}

// String parsing
Token Lexer::make_string(std::size_t start) {
    std::string out;
    while (pos_ < input_.size()) {
        char c = input_[pos_++];
        if (c == '\\') {
            if (pos_ >= input_.size()) throw std::runtime_error("Unterminated escape sequence in string literal");
            char e = input_[pos_++];
            switch(e) {
                case '\\': out.push_back('\\'); break;
                case '\'': out.push_back('\''); break;
                case 'n': out.push_back('\n'); break;
                case 't': out.push_back('\t'); break;
                default: out.push_back(e); break;
            }
        } else if (c == '\'') {
            return {TokenType::String, out, start};
        } else {
            out.push_back(c);
        }
    }
    throw std::runtime_error("Unterminated string literal");
}

// Get the next token
Token Lexer::next() {
    if (has_lookahead_) { has_lookahead_ = false; return lookahead_; }
    skip_ws();
    if (pos_ >= input_.size()) return {TokenType::End, "", pos_};
    std::size_t start = pos_;
    char c = input_[pos_++];

    switch(c) {
        case ',': return {TokenType::Comma, ",", start};
        case '(': return {TokenType::LParen, "(", start};
        case ')': return {TokenType::RParen, ")", start};
        case ';': return {TokenType::Semicolon, ";", start};
        case '*': return {TokenType::Star, "*", start};
        case '=': return {TokenType::Equal, "=", start};
        case '.': return {TokenType::Dot, ".", start};
        case '!':
            if (pos_ < input_.size() && input_[pos_] == '=') { ++pos_; return {TokenType::NotEqual, "!=", start}; }
            break;
        case '<':
            if (pos_ < input_.size() && input_[pos_] == '=') { ++pos_; return {TokenType::LessEqual, "<=", start}; }
            return {TokenType::Less, "<", start};
        case '>':
            if (pos_ < input_.size() && input_[pos_] == '=') { ++pos_; return {TokenType::GreaterEqual, ">=", start}; }
            return {TokenType::Greater, ">", start};
        case '\'':
            return make_string(start);
    }
    // Check for identifier or number
    if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') return make_identifier_or_keyword(start);
    if (std::isdigit(static_cast<unsigned char>(c))) return make_number(start);
    throw std::runtime_error("Unexpected character in input");
}

Token Lexer::peek() {
    if (!has_lookahead_) {
        lookahead_ = next();
        has_lookahead_ = true;
    }
    return lookahead_;
}

}
