const fs = require("node:fs");
const readline = require("node:readline");
const { logTime } = require("./date.helper");
const { formatNumber } = require("./number.helper");
const {
  CACHE_DIR,
  CHUNK_FILE_NAME_PREFIX,
  ERROR_FILE_NAME,
  IGNORED_LINE_STARTS,
} = require("./config");

const clearOldCacheFiles = () => {
  if (fs.existsSync(CACHE_DIR)) {
    const files = fs.readdirSync(CACHE_DIR);
    for (const file of files) {
      if (file.startsWith(CHUNK_FILE_NAME_PREFIX)) {
        fs.unlinkSync(`${CACHE_DIR}/${file}`);
      }
    }
  } else {
    fs.mkdirSync(CACHE_DIR, { recursive: true });
  }

  if (fs.existsSync(ERROR_FILE_NAME)) {
    fs.unlinkSync(ERROR_FILE_NAME);
  }
};

const prepareCacheFileReading = async ({
  filePath,
  maxLines = 2000,
  startTime,
}) => {
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

      if (combinedLines.length >= maxLines) {
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

const saveCombinedLinesAndAddInFiles = ({ combinedLines, files }) => {
  const chunkFileName = `${CACHE_DIR}/${CHUNK_FILE_NAME_PREFIX}${
    files.length + 1
  }.json`;

  fs.writeFileSync(chunkFileName, JSON.stringify(combinedLines, null, 1));

  files.push({
    filePath: chunkFileName,
    nLines: combinedLines.length,
  });
};

module.exports = {
  prepareCacheFileReading,
};
