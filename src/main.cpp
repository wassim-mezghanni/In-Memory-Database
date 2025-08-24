#include <iostream>
#include "inmemdb/lexer.hpp"
#include "inmemdb/parser.hpp"
#include "inmemdb/executor.hpp"

int main() {
    using namespace inmemdb;
    Database db;
    Executor exec(db);

    std::cout << "In-Memory DB CLI. Enter statements; end with semicolon. Ctrl-D to exit.\n";
    std::string buffer;
    std::string line;
    while (std::cout << "> " && std::getline(std::cin, line)) {
        if (line.empty()) continue;
        buffer += line;
        buffer += '\n';
        if (line.find(';') != std::string::npos) { 
            try {
                Lexer lx(buffer);
                Parser parser{std::move(lx)};
                auto stmts = parser.parse_all();
                for (auto const& st : stmts) {
                    auto res = exec.execute(st);
                    if (!res.success) {
                        std::cout << "Error: " << res.message << "\n";
                    } else if (!res.header.empty()) {
                        for (size_t i = 0; i < res.header.size(); ++i) {
                            if (i) std::cout << '\t';
                            std::cout << res.header[i];
                        }
                        std::cout << "\n";
                        for (auto const& row : res.rows) {
                            for (size_t i = 0; i < row.size(); ++i) {
                                if (i) std::cout << '\t';
                                std::cout << row[i];
                            }
                            std::cout << "\n";
                        }
                        std::cout << res.rows.size() << " row(s).\n";
                    } else {
                        std::cout << res.message << "\n";
                    }
                }
            } catch (std::exception const& ex) {
                std::cout << "Parse/Exec Error: " << ex.what() << "\n";
            }
            buffer.clear();
        }
    }
    std::cout << "End.\n";
    return 0;
}
