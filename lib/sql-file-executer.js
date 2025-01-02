const { humanizeMs } = require("./date.helper");
const fs = require("node:fs");
const dotenv = require("dotenv");
const { Pool } = require("pg");
const readline = require("node:readline");

dotenv.config();

const IGNORED_LINE_STARTS = [
  "--",
  "/*",
  "set",
  "\\connect ",
  "drop database",
  "create database",
];
const CACHE_DIR = ".cache";
const CHUNK_FILE_NAME_PREFIX = "chunk_";
const ERROR_FILE_NAME = "error.log";

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

const formatNumber = (number) => {
  return Number(number).toLocaleString("en-IN");
};

const sqlQuery = async ({ lines, nLines, client, counter, startTime }) => {
  for (const line of lines) {
    const prefix = `${formatNumber(counter.index)}/${formatNumber(
      nLines
    )} | ${Math.floor((counter.index * 100) / nLines)}%${logTime(
      startTime
    )} | Success: ${formatNumber(counter.success)} | Fail: ${formatNumber(
      counter.failed
    )}`;
    try {
      console.info(`- ${prefix}: Executing...`);
      await client.query(line);
      counter.success += 1;
    } catch (err) {
      counter.failed += 1;
      console.error("  ! Error executing line:", err.message);
      fs.appendFileSync(
        ERROR_FILE_NAME,
        `${new Date().toISOString()} | ${prefix}:\n` +
          `${line}\n` +
          `Error: ${err.message}\n\n`
      );
    }
    counter.index += 1;
  }
};

const sqlFileExecuter = async ({ files, client, startTime }) => {
  const counter = {
    index: 1,
    success: 0,
    failed: 0,
  };
  const nLines = files.reduce((acc, file) => acc + file.nLines, 0);
  for (const file of files) {
    const lines = JSON.parse(fs.readFileSync(file.filePath, "utf8"));
    await sqlQuery({
      lines,
      nLines,
      client,
      counter,
      startTime,
    });
  }
};

const logTime = (startTime) => {
  return ` | ${humanizeMs(new Date().getTime() - startTime)}`;
};

const main = async () => {
  const startTime = new Date().getTime();
  console.info(`Connecting to the database...${logTime(startTime)}`);
  const dbConfig = {
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_DATABASE,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
  };

  const pool = new Pool(dbConfig);
  const client = await pool.connect();
  console.info(`Database connection established.${logTime(startTime)}`);
  console.info(`\nReading SQL file...${logTime(startTime)}`);
  const files = await prepareCacheFileReading({
    filePath: process.argv[2],
    startTime,
  });
  console.info(`SQL files read successfully.${logTime(startTime)}`);
  console.info(`\nExecuting SQL file...${logTime(startTime)}`);
  await sqlFileExecuter({
    files,
    client,
    startTime,
  });
};

main().then(() => console.info("Done!"));
