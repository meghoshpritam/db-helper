const { humanizeMs } = require("./date.helper");
const fs = require("node:fs");
const dotenv = require("dotenv");
const { Client } = require("pg");

dotenv.config();

const sqlFileExecuter = async ({ filePath, client }) => {
  const startTime = new Date().getTime();
  const fileContent = fs.readFileSync(filePath, "utf8");
  console.log("🚀[sql-file-executer.js:12]: fileContent: ", fileContent);

  const lines = fileContent
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("--"));

  const combinedLines = [];
  let combinedLine = "";
  for (const line of lines) {
    combinedLine += `${line} `;
    if (line.endsWith(";")) {
      combinedLines.push(combinedLine);
      combinedLine = "";
    }
  }

  let index = 1;
  let success = 0;
  let failed = 0;
  for (const line of combinedLines) {
    try {
      console.info(
        `  - ${index}/${combinedLines.length} | ${Math.floor(
          (index * 100) / combinedLines.length
        )}% | ${humanizeMs(
          new Date().getTime() - startTime
        )} | S:${success} - F:${failed}:: Executing...`
      );
      await client.query(line);
      success += 1;
    } catch (err) {
      failed += 1;
      console.error("  ! Error executing line:", err.message);
    }
    index += 1;
  }
};

const main = async () => {
  const dbConfig = {
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_DATABASE,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
  };

  const client = new Client(dbConfig);
  console.log("🚀[sql-file-executer.js:59]: client: ", client);

  console.log(
    "🚀[sql-file-executer.js:63]: process.argv[2]: ",
    process.argv[2]
  );
  await sqlFileExecuter({
    filePath: process.argv[2],
    client,
  });
};

main();
