const fs = require("node:fs");
const readline = require("node:readline");
const { logTime } = require("./date.helper");
const { formatNumber } = require("./number.helper");
const {
  CACHE_DIR,
  CHUNK_FILE_NAME_PREFIX,
  ERROR_FILE_NAME,
  IGNORED_LINE_STARTS,
  MAX_LINES_PER_FILE,
  OTHER_STATEMENT_FILE_NAME_PREFIX,
  INSERT_STATEMENT_FILE_NAME_PREFIX,
  ORDERED_INSERT_STATEMENT_FILE_NAME_PREFIX,
} = require("./config");

const clearCacheFiles = (fileSTartsWIth) => {
  if (fs.existsSync(CACHE_DIR)) {
    const files = fs.readdirSync(CACHE_DIR);
    for (const file of files) {
      if (file.startsWith(fileSTartsWIth)) {
        fs.unlinkSync(`${CACHE_DIR}/${file}`);
      }
    }
  }
};

const getJsonFile = (filePath) => {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
};

const clearOldCacheFiles = () => {
  for (const prefix of [
    CHUNK_FILE_NAME_PREFIX,
    INSERT_STATEMENT_FILE_NAME_PREFIX,
    ORDERED_INSERT_STATEMENT_FILE_NAME_PREFIX,
    OTHER_STATEMENT_FILE_NAME_PREFIX,
  ]) {
    clearCacheFiles(prefix);
  }

  if (!fs.existsSync(CACHE_DIR)) {
    fs.mkdirSync(CACHE_DIR, { recursive: true });
  }

  if (fs.existsSync(ERROR_FILE_NAME)) {
    fs.unlinkSync(ERROR_FILE_NAME);
  }
};

const prepareCacheFileReading = async ({ filePath, startTime }) => {
  const files = [];
  let combinedLines = [];
  let linesToMerge = [];

  const fileStream = fs.createReadStream(filePath, { encoding: "utf8" });
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Number.POSITIVE_INFINITY,
  });

  clearOldCacheFiles();

  for await (const line of rl) {
    if (
      line.length > 0 &&
      !IGNORED_LINE_STARTS.some((start) => line.toLowerCase().startsWith(start))
    ) {
      linesToMerge.push(line);
      if (
        linesToMerge?.[0]?.toLowerCase()?.startsWith("insert into ")
          ? line.endsWith(");") ||
            linesToMerge.join("\n").match(/\)\s+ON CONFLICT /gm)?.length > 0
          : line.endsWith(";")
      ) {
        combinedLines.push(linesToMerge?.join("\n"));
        linesToMerge = [];
      }

      if (combinedLines.length >= MAX_LINES_PER_FILE) {
        saveCombinedLinesAndAddInFiles({ combinedLines, files });
        console.info(
          `- ${formatNumber(files.length)} files created.${logTime(startTime)}`
        );
        combinedLines = [];
      }
    }
  }
  if (linesToMerge?.length > 0) {
    combinedLines.push(linesToMerge?.join("\n"));
    linesToMerge = [];
  }

  if (combinedLines.length > 0) {
    saveCombinedLinesAndAddInFiles({ combinedLines, files });
  }

  return files;
};

const saveCombinedLinesAndAddInFiles = ({
  combinedLines,
  files,
  prefix = CHUNK_FILE_NAME_PREFIX,
}) => {
  const chunkFileName = `${CACHE_DIR}/${prefix}${files.length + 1}.json`;

  fs.writeFileSync(chunkFileName, JSON.stringify(combinedLines, null, 1));

  files.push({
    filePath: chunkFileName,
    nLines: combinedLines.length,
  });
};

const syncAndSaveOrderedFiles = ({
  tableLines,
  tableOrderFileMap,
  skipLimitCheck = false,
}) => {
  for (const lineTable of Object.keys(tableLines)) {
    if (
      tableLines[lineTable].length >= (skipLimitCheck ? 0 : MAX_LINES_PER_FILE)
    ) {
      if (!tableOrderFileMap[lineTable]) {
        tableOrderFileMap[lineTable] = [];
      }
      saveCombinedLinesAndAddInFiles({
        combinedLines: tableLines[lineTable],
        files: tableOrderFileMap[lineTable],
        prefix: `${ORDERED_INSERT_STATEMENT_FILE_NAME_PREFIX}${lineTable}_`,
      });
      tableLines[lineTable] = [];
    }
  }
};

const orderImports = ({ files, tableOrder, schema }) => {
  const orderedFiles = [];
  const tableOrderFileMap = {};
  const tableLines = {};

  for (const file of files) {
    const lines = getJsonFile(file.filePath);
    for (const line of lines) {
      const tableName = line.match(/insert into\s+([^\s]+)/i)[1];

      if (!tableLines[tableName]) {
        tableLines[tableName] = [];
      }
      tableLines[tableName].push(line);

      syncAndSaveOrderedFiles({ tableLines, tableOrderFileMap });
    }
  }

  syncAndSaveOrderedFiles({
    tableLines,
    tableOrderFileMap,
    skipLimitCheck: true,
  });

  for (const table of tableOrder) {
    const key = schema ? `${schema}.${table}` : table;
    if (tableOrderFileMap[key]) {
      orderedFiles.push(...tableOrderFileMap[key]);
      tableOrderFileMap[key] = [];
    }
  }

  for (const table of Object.keys(tableOrderFileMap)) {
    orderedFiles.push(...tableOrderFileMap[table]);
  }

  return orderedFiles;
};

const organizeImportStatements = ({ files, tableOrder, schema }) => {
  if (tableOrder.length === 0) {
    return files;
  }

  const insertFiles = [];
  const otherFiles = [];
  let insertLines = [];
  let otherLines = [];

  for (const file of files) {
    const lines = getJsonFile(file.filePath);

    for (const line of lines) {
      const lowerCaseLine = line.toLowerCase();
      if (lowerCaseLine.startsWith("insert into ")) {
        insertLines.push(line);
      } else {
        otherLines.push(line);
      }

      if (insertLines.length >= MAX_LINES_PER_FILE) {
        saveCombinedLinesAndAddInFiles({
          combinedLines: insertLines,
          files: insertFiles,
          prefix: INSERT_STATEMENT_FILE_NAME_PREFIX,
        });
        insertLines = [];
      }
      if (otherLines.length >= MAX_LINES_PER_FILE) {
        saveCombinedLinesAndAddInFiles({
          combinedLines: otherLines,
          files: otherFiles,
          prefix: OTHER_STATEMENT_FILE_NAME_PREFIX,
        });
        otherLines = [];
      }
    }
  }

  if (insertLines.length >= MAX_LINES_PER_FILE) {
    saveCombinedLinesAndAddInFiles({
      combinedLines: insertLines,
      files: insertFiles,
      prefix: INSERT_STATEMENT_FILE_NAME_PREFIX,
    });
    insertLines = [];
  }
  if (otherLines.length >= MAX_LINES_PER_FILE) {
    saveCombinedLinesAndAddInFiles({
      combinedLines: otherLines,
      files: otherFiles,
      prefix: OTHER_STATEMENT_FILE_NAME_PREFIX,
    });
    otherLines = [];
  }

  clearCacheFiles(CHUNK_FILE_NAME_PREFIX);

  const organizedInsertFiles = orderImports({
    files: insertFiles,
    tableOrder,
    schema,
  });
  clearCacheFiles(INSERT_STATEMENT_FILE_NAME_PREFIX);

  return [...organizedInsertFiles, ...otherFiles];
};

module.exports = {
  prepareCacheFileReading,
  organizeImportStatements,
  getJsonFile,
};
