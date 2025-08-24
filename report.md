# In-Memory Database: Design Report

Overview
- This project implements a small relational engine with a command-line REPL. It parses a tiny SQL subset (CREATE TABLE, INSERT, SELECT with WHERE, and INNER JOIN) and executes queries against in-memory tables.

Architecture
- Lexer: Converts characters into tokens (identifiers, literals, operators, keywords). Keeps parsing simple and robust.
- Parser and AST: Builds typed statements (CreateTableStmt, InsertStmt, SelectStmt) and supporting types (ColumnDef, WhereCond, JoinClause). All statements are carried by a Statement = std::variant<...> to avoid virtual dispatch.
- Executor: Dispatches on the Statement variant and calls the storage layer.
- Storage/Engine: Database manages Table objects (schema + rows). Value is std::variant<int64_t, std::string>. QueryResult holds success/message, headers, and stringified rows for the CLI.

Key Design Choices
- Separation of concerns: lex/parse/execute/store are decoupled and testable in isolation.
- Strong typing with variants: the AST and row Value use std::variant and std::optional to express alternatives and optionals without inheritance or nullable sentinels.
- Errors via exceptions: parse/execute throw on invalid input and are caught at the REPL boundary, keeping the core clean.
- Portability: avoided non-portable std features (e.g., from_chars, unordered_map::contains) to work across libstdc++/libc++ and older toolchains; used strtoll and find instead. Also replaced std::visit-heavy code with std::get/index patterns where useful.
- Simplicity over performance: INNER JOIN uses a nested loop; SELECT * over joins emits qualified headers (table.column) to avoid ambiguity.

C++ Features Utilized
- C++17/20 standard library: std::variant, std::optional, std::unordered_map, std::vector, structured bindings, and exceptions.
- RAII and value semantics: clear ownership of data; no raw resource management needed.
- CMake project model with a reusable static library and two executables (CLI and tests).

Testing and Build
- Unit tests cover single-table selection and INNER JOIN with WHERE, integrated via CTest. The suite can be migrated to Catch2/GoogleTest in Artemis. The code builds with Qt6-provided toolchains and formats cleanly with clang-format.
