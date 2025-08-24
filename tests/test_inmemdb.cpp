#include <iostream>
#include <string>
#include <vector>

#include "inmemdb/lexer.hpp"
#include "inmemdb/parser.hpp"
#include "inmemdb/executor.hpp"
#include "inmemdb/storage.hpp"

using namespace inmemdb;

static int g_failures = 0;
#define EXPECT_TRUE(cond) do { if(!(cond)) { std::cerr << "EXPECT_TRUE failed at " << __FILE__ << ":" << __LINE__ << " : " #cond "\n"; ++g_failures; } } while(0)
#define EXPECT_EQ(a,b) do { auto _a=(a); auto _b=(b); if(!(_a==_b)) { std::cerr << "EXPECT_EQ failed at " << __FILE__ << ":" << __LINE__ << ": " #a " vs " #b << " => '" << _a << "' vs '" << _b << "'\n"; ++g_failures; } } while(0)

struct RunResult { std::vector<QueryResult> results; };

static RunResult run_sql(Database& db, std::string const& sql) {
    Lexer lex(sql);
    Parser parser(std::move(lex));
    auto stmts = parser.parse_all();
    Executor exec(db);
    RunResult rr;
    for (auto const& st : stmts) {
        rr.results.push_back(exec.execute(st));
    }
    return rr;
}

static void test_basic_single_table() {
    Database db;
    auto rr = run_sql(db,
        "CREATE TABLE users(id INT, name TEXT);\n"
        "INSERT INTO users VALUES(1, Alice);\n"
        "INSERT INTO users VALUES(2, Bob);\n"
        "SELECT name FROM users WHERE id = 2;\n"
    );
    EXPECT_EQ(rr.results.size(), 4u);
    auto const& sel = rr.results.back();
    EXPECT_TRUE(sel.success);
    EXPECT_EQ(sel.header.size(), 1u);
    EXPECT_EQ(sel.header[0], std::string("name"));
    EXPECT_EQ(sel.rows.size(), 1u);
    EXPECT_EQ(sel.rows[0].size(), 1u);
    EXPECT_EQ(sel.rows[0][0], std::string("Bob"));
}

static void test_inner_join() {
    Database db;
    auto rr = run_sql(db,
        "CREATE TABLE users(id INT, name TEXT);\n"
        "CREATE TABLE orders(user_id INT, total INT);\n"
        "INSERT INTO users VALUES(1, Alice);\n"
        "INSERT INTO users VALUES(2, Bob);\n"
        "INSERT INTO orders VALUES(1, 100);\n"
        "INSERT INTO orders VALUES(1, 50);\n"
        "INSERT INTO orders VALUES(2, 75);\n"
        "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id WHERE orders.total >= 80;\n"
    );
    EXPECT_EQ(rr.results.size(), 8u);
    auto const& sel = rr.results.back();
    EXPECT_TRUE(sel.success);
    EXPECT_EQ(sel.header.size(), 2u);
    EXPECT_EQ(sel.header[0], std::string("users.name"));
    EXPECT_EQ(sel.header[1], std::string("orders.total"));
    EXPECT_EQ(sel.rows.size(), 1u);
    EXPECT_EQ(sel.rows[0][0], std::string("Alice"));
    EXPECT_EQ(sel.rows[0][1], std::string("100"));
}

int main() {
    test_basic_single_table();
    test_inner_join();
    if (g_failures == 0) {
        std::cout << "All tests passed\n";
        return 0;
    }
    std::cerr << g_failures << " test(s) failed\n";
    return 1;
}
