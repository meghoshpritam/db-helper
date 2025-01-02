const fs = require("node:fs");
const path = require("node:path");
const { Client } = require("pg");
const dotenv = require("dotenv");

dotenv.config();

// Function to execute SQL files
async function executeSqlFiles(directoryPath, dbConfig) {
  const client = new Client(dbConfig);
  console.log("🚀[script.js:11]: client: ", client);

  try {
    // Connect to the database
    await client.connect();
    console.log("Connected to the PostgreSQL database.");

    // Read all SQL files in the directory
    const sqlFiles = fs
      .readdirSync(directoryPath)
      .filter((file) => file.endsWith(".sql"))
      .sort(); // Sort files alphabetically

    if (sqlFiles.length === 0) {
      console.log("No SQL files found in the directory.");
      return;
    }

    let index = 1;
    // Execute each SQL file
    for (const sqlFile of sqlFiles) {
      try {
        const filePath = path.join(directoryPath, sqlFile);
        const prefix = `${index}/${sqlFiles.length} | ${Math.floor(
          (index * 100) / sqlFiles.length
        )}%::`;
        console.log(`\n${prefix} Executing ${sqlFile}...`);

        const sqlScript = fs.readFileSync(filePath, "utf8");

        const lines = sqlScript
          .split("\n")
          .map((line) => line.trim())
          .filter(Boolean);

        let lineIndex = 1;
        for (const line of lines) {
          console.info(
            `  - ${prefix} --> ${lineIndex}/${lines.length} | ${Math.floor(
              (lineIndex * 100) / lines.length
            )}%:: Executing`
          );
          try {
            await client.query(line);
          } catch (err) {
            console.error(
              `Error executing line ${lineIndex} in ${sqlFile}:`,
              err.message
            );
          }
          lineIndex += 1;
        }
        await client.query(sqlScript);
        console.log(`${sqlFile} executed successfully.`);
      } catch (err) {
        console.error(`Error executing ${sqlFile}:`, err.message);
      }
      index += 1;
    }

    console.log("All SQL files executed successfully.");
  } catch (err) {
    console.error("Error during execution:", err.message);
  } finally {
    // Close the database connection
    await client.end();
    console.log("Database connection closed.");
  }
}

// Example usage
(async () => {
  const directoryPath = process.env.SQL_FILES_PATH;
  const dbConfig = {
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_DATABASE,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
  };

  await executeSqlFiles(directoryPath, dbConfig);
})();
