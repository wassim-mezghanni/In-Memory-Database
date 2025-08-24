#pragma once
#include "inmemdb/storage.hpp"
#include <variant>

namespace inmemdb {

class Executor {
public:
    explicit Executor(Database& db) : db_(db) {}
    QueryResult execute(Statement const& stmt);
private:
    Database& db_;
};

}
