#!/usr/bin/env node

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
const ora = require("ora");
const { consoleMessageWithOra } = require("./console.helper");
const CONFIG = require("./config");

program.option("--skip-line-until <number>").argument("[string]");
program.option("-s, --schema <string>").argument("[string]");
program.option("--only-execution-order").argument("[boolean]");

program.parse();

dotenv.config();

const sqlQuery = async ({
  lines,
  nLines,
  client,
  counter,
  startTime,
  skipLineUntil,
  spinner,
}) => {
  const getPrefix = () => {
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

    return prefix;
  };

  const singleLineExecutor = async (line) => {
    if (skipLineUntil > 0 && counter.index < skipLineUntil) {
      counter.index += 1;
      return;
    }
    try {
      await client.query(line);
      counter.success += 1;
    } catch (err) {
      counter.failed += 1;
      spinner.fail(`Error ${counter.failed} |executing line: ${err.message}`);
      fs.appendFileSync(
        ERROR_FILE_NAME,
        `${new Date().toISOString()} | ${getPrefix()}:\n` +
          `${line}\n` +
          `Error: ${err.message}\n\n`
      );
    }
    counter.index += 1;
  };

  const maxConcurrency = CONFIG.MAX_CONCURRENCY;
  for (let i = 0; i < lines.length; i += maxConcurrency) {
    await Promise.all(
      lines.slice(i, i + maxConcurrency).map((line) => singleLineExecutor(line))
    );

    consoleMessageWithOra(`- ${getPrefix()}: Executing...`, spinner);
  }
};

const sqlFileExecuter = async ({
  files,
  client,
  startTime,
  skipLineUntil,
  spinner,
}) => {
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
      spinner,
    });
  }
};

const main = async () => {
  const startTime = new Date().getTime();
  const options = program.opts();
  const spinner = ora("Starting...").start();

  fs.writeFileSync(ERROR_FILE_NAME, "");
  consoleMessageWithOra(
    `Connecting to the database...${logTime(startTime)}`,
    spinner
  );
  const dbConfig = {
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_DATABASE,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
  };

  const pool = new Pool(dbConfig);
  const client = await pool.connect();
  spinner.succeed(`Database connection established.${logTime(startTime)}`);

  consoleMessageWithOra(`\nReading SQL file...${logTime(startTime)}`, spinner);
  let tableOrder = [];
  if (options?.schema?.length > 0) {
    consoleMessageWithOra(
      `\nSetting insertion order...${logTime(startTime)}`,
      spinner
    );
    const res = await getInsertionOrder(options.schema);
    spinner.succeed(
      `Found ${res.sortedOrder.length} tables.${logTime(startTime)}`
    );
    if (res?.cyclicNodes?.length > 0) {
      spinner.fail(
        `Cycle detected at: ${res.cyclicNodes.join(
          ", "
        )}. Please fix the schema.`
      );
    }
    tableOrder = res.sortedOrder;
    spinner.succeed(`Insertion order obtained.${logTime(startTime)}`);
  }

  let orderedFiles = [];
  if (options?.skipLineUntil) {
    orderedFiles = getJsonFile(EXECUTION_ORDER_FILE_NAME);
  } else {
    const files = await prepareCacheFileReading({
      filePath: process.argv[2],
      startTime,
      spinner,
    });

    consoleMessageWithOra(
      `\nOrganizing SQL files...${logTime(startTime)}`,
      spinner
    );
    orderedFiles = organizeImportStatements({
      files,
      tableOrder,
      schema: process.env.DB_SCHEMA,
    });
    spinner.succeed(`SQL files organized.${logTime(startTime)}`);

    spinner.succeed(`SQL files read successfully.${logTime(startTime)}`);

    fs.writeFileSync(
      EXECUTION_ORDER_FILE_NAME,
      JSON.stringify(orderedFiles, null, 2)
    );
  }

  if (options?.onlyExecutionOrder) {
    spinner.succeed(`Execution order saved.${logTime(startTime)}`);
    return;
  }

  consoleMessageWithOra(
    `\nExecuting SQL file...${logTime(startTime)}`,
    spinner
  );
  await sqlFileExecuter({
    files: orderedFiles,
    client,
    startTime,
    skipLineUntil: options?.skipLineUntil || -1,
    spinner,
  });

  spinner.succeed(`SQL file executed.${logTime(startTime)}`);
};

main()
  .then(() => console.info("Done!"))
  .catch(console.error)
  .finally(() => {
    console.info("Exiting...");
    process.exit(0);
  });
