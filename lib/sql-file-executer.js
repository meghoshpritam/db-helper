const { humanizeMs } = require("./date.helper");
const fs = require("node:fs");
const dotenv = require("dotenv");
const { Client } = require("pg");
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

const prepareCacheFileReading = async ({ filePath, maxLines = 1000 }) => {
  const fileStream = fs.createReadStream(filePath, { encoding: "utf8" });
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Number.POSITIVE_INFINITY,
  });

  const files = [];

  let combinedLines = [];
  let combinedLine = "";

  if (!fs.existsSync(CACHE_DIR)) {
    fs.mkdirSync(CACHE_DIR, { recursive: true });
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
      const chunkFileName = `${CACHE_DIR}/chunk_${files.length}.json`;
      fs.writeFileSync(chunkFileName, JSON.stringify(combinedLines));
      files.push({
        filePath: chunkFileName,
        nLines: combinedLines.length,
      });

      combinedLines = [];
    }
  }

  if (combinedLines.length > 0) {
    const chunkFileName = `${CACHE_DIR}/chunk_${files.length}.json`;
    fs.writeFileSync(chunkFileName, JSON.stringify(combinedLines));
    files.push({
      filePath: chunkFileName,
      nLines: combinedLines.length,
    });
  }

  return files;
};

const sqlQuery = async ({ lines, nLines, client, counter }) => {
  for (const line of lines) {
    try {
      console.info(
        `- ${counter.index}/${nLines} | ${Math.floor(
          (index * 100) / nLines
        )}% | ${humanizeMs(new Date().getTime() - startTime)} | S:${
          counter.success
        } - F:${counter.failed}:: Executing...`
      );
      await client.query(line);
      counter.success += 1;
    } catch (err) {
      counter.failed += 1;
      console.error("  ! Error executing line:", err.message);
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
    });
  }
};

const main = async () => {
  const startTime = new Date().getTime();
  console.info("Connecting to the database...");
  const dbConfig = {
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_DATABASE,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
  };

  const client = new Client(dbConfig);
  console.info("Database connection established.");
  console.info("\nReading SQL file...");
  const files = await prepareCacheFileReading({ filePath: process.argv[2] });
  console.info("SQL files read successfully.");
  console.info("\nExecuting SQL file...");
  await sqlFileExecuter({
    files,
    client,
    startTime,
  });
};

main();
