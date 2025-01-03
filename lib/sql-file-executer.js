const { logTime, humanizeMs } = require("./date.helper");
const fs = require("node:fs");
const dotenv = require("dotenv");
const { Pool } = require("pg");
const {
  prepareCacheFileReading,
  organizeImportStatements,
  getJsonFile,
} = require("./file.helper");
const { formatNumber } = require("./number.helper");
const { ERROR_FILE_NAME, EXECUTION_ORDER_FILE_NAME } = require("./config");
const { getInsertionOrder } = require("./prisma.helper");
const { program } = require("commander");

program.option("--skip-line-until <number>").argument("[string]");
program.option("-s, --schema <string>").argument("[string]");

program.parse();

dotenv.config();

const sqlQuery = async ({
  lines,
  nLines,
  client,
  counter,
  startTime,
  skipLineUntil,
}) => {
  for (const line of lines) {
    if (skipLineUntil > 0 && counter.index < skipLineUntil) {
      counter.index += 1;
      continue;
    }
    const performingPercentage = Math.floor((counter.index * 100) / nLines);
    const timeSpent = new Date().getTime() - startTime;
    const totalEstimatedTime = (timeSpent * 100) / (performingPercentage || 1);
    const eta = totalEstimatedTime - timeSpent;
    const prefix = `${formatNumber(counter.index)}/${formatNumber(
      nLines
    )} | ${performingPercentage}%${logTime(startTime)} | ETA: ${humanizeMs(
      eta
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

const sqlFileExecuter = async ({ files, client, startTime, skipLineUntil }) => {
  const counter = {
    index: 1,
    success: 0,
    failed: 0,
  };
  const nLines = files.reduce((acc, file) => acc + file.nLines, 0);
  for (const file of files) {
    const lines = getJsonFile(file.filePath);
    await sqlQuery({
      lines,
      nLines,
      client,
      counter,
      startTime,
      skipLineUntil,
    });
  }
};

const main = async () => {
  const startTime = new Date().getTime();
  const options = program.opts();
  fs.writeFileSync(ERROR_FILE_NAME, "");
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
  let tableOrder = [];
  if (options?.schema?.length > 0) {
    console.info(`\nSetting insertion order...${logTime(startTime)}`);
    const res = await getInsertionOrder(options.schema);
    tableOrder = res;
    console.info(`Insertion order obtained.${logTime(startTime)}`);
  }

  let orderedFiles = [];
  if (options?.skipLineUntil) {
    orderedFiles = getJsonFile(EXECUTION_ORDER_FILE_NAME);
  } else {
    const files = await prepareCacheFileReading({
      filePath: process.argv[2],
      startTime,
    });

    console.info(`\nOrganizing SQL files...${logTime(startTime)}`);
    orderedFiles = organizeImportStatements({
      files,
      tableOrder,
      schema: process.env.DB_SCHEMA,
    });
    console.info(`SQL files organized.${logTime(startTime)}`);

    console.info(`\nSQL files read successfully.${logTime(startTime)}`);

    fs.writeFileSync(
      EXECUTION_ORDER_FILE_NAME,
      JSON.stringify(orderedFiles, null, 2)
    );
  }

  console.info(`\nExecuting SQL file...${logTime(startTime)}`);
  await sqlFileExecuter({
    files: orderedFiles,
    client,
    startTime,
    skipLineUntil: options?.skipLineUntil || -1,
  });

  console.info(`\nSQL file executed.${logTime(startTime)}`);
};

main()
  .then(() => console.info("Done!"))
  .catch(console.error)
  .finally(() => {
    console.info("Exiting...");
    process.exit(0);
  });
