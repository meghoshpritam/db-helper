const CONFIG = {
  IGNORED_LINE_STARTS: [
    "--",
    "set ",
    "\\connect ",
    "drop database",
    "create database",
  ],
  CACHE_DIR: ".cache",
  CHUNK_FILE_NAME_PREFIX: "chunk_",
  ERROR_FILE_NAME: "error.log",
  EXECUTION_ORDER_FILE_NAME: "execution_order.json",
  INSERT_STATEMENT_FILE_NAME_PREFIX: "insert_statement_",
  ORDERED_INSERT_STATEMENT_FILE_NAME_PREFIX: "ordered_insert_statement_",
  OTHER_STATEMENT_FILE_NAME_PREFIX: "other_statement_",
  MAX_LINES_PER_FILE: 2000,
  MAX_CONCURRENCY: 1000,
};

module.exports = CONFIG;
