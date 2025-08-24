#pragma once
#include <string>
#include <vector>
#include "inmemdb/token.hpp"

namespace inmemdb {

class Lexer {
public:
    explicit Lexer(std::string input);
    const std::string& input() const { return input_; }
    Token next();
    Token peek();
private:
    Token make_identifier_or_keyword(std::size_t start);
    Token make_number(std::size_t start);
    Token make_string(std::size_t start);

    void skip_ws(); // skip whitespace

    std::string input_;
    std::size_t pos_{};
    Token lookahead_{TokenType::End, "", 0}; 
    bool has_lookahead_ = false; // whether lookahead_ is valid
};

} 
