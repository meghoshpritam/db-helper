const CONFIG = {
  IGNORED_LINE_STARTS: [
    "--",
    "/*",
    "set",
    "\\connect ",
    "drop database",
    "create database",
  ],
  CACHE_DIR: ".cache",
  CHUNK_FILE_NAME_PREFIX: "chunk_",
  ERROR_FILE_NAME: "error.log",
};

module.exports = CONFIG;
