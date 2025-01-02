const { logTime } = require("./date.helper");
const fs = require("node:fs");
const dotenv = require("dotenv");
const { Pool } = require("pg");
const { prepareCacheFileReading } = require("./file.helper");
const { formatNumber } = require("./number.helper");
const { ERROR_FILE_NAME } = require("./config");

dotenv.config();

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
