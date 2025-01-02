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

const prepareCacheFileReading = async ({
  filePath,
  maxLines = 2000,
  startTime,
}) => {
  const fileStream = fs.createReadStream(filePath, { encoding: "utf8" });
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Number.POSITIVE_INFINITY,
  });

  const files = [];

  let combinedLines = [];
  let combinedLine = "";

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

  for await (const line of rl) {
    const trimmedLine = line.trim();
    if (
      trimmedLine.length > 0 &&
      !IGNORED_LINE_STARTS.some((start) =>
        trimmedLine.toLowerCase().startsWith(start)
      )
    ) {
      combinedLine += `${trimmedLine} `;
      if (trimmedLine.endsWith(";")) {
        combinedLines.push(combinedLine);
        combinedLine = "";
      }
    }

    if (combinedLines.length >= maxLines) {
      const chunkFileName = `${CACHE_DIR}/${CHUNK_FILE_NAME_PREFIX}${files.length}.json`;
      fs.writeFileSync(chunkFileName, JSON.stringify(combinedLines));
      files.push({
        filePath: chunkFileName,
        nLines: combinedLines.length,
      });
      console.info(
        `- ${formatNumber(files.length)} files created.${logTime(startTime)}`
      );
      combinedLines = [];
    }
  }

  if (combinedLines.length > 0) {
    const chunkFileName = `${CACHE_DIR}/${CHUNK_FILE_NAME_PREFIX}${files.length}.json`;
    fs.writeFileSync(chunkFileName, JSON.stringify(combinedLines));
    files.push({
      filePath: chunkFileName,
      nLines: combinedLines.length,
    });
  }

  return files;
};

module.exports = {
  prepareCacheFileReading,
};
