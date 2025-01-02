const { getDMMF } = require("@prisma/sdk");
const fs = require("fs");

// Function to parse the schema and generate insertion order
async function getInsertionOrder(schemaPath) {
  // Step 1: Load the schema file
  const schema = fs.readFileSync(schemaPath, "utf-8");

  // Step 2: Parse the schema using Prisma's DMMF (Data Model Meta Format)
  const dmmf = await getDMMF({ datamodel: schema });

  // Step 3: Build the dependency graph
  const models = dmmf.datamodel.models;
  const graph = {}; // Dependency graph
  const inDegree = {}; // Count of incoming edges for each node

  // Initialize graph and in-degree
  models.forEach((model) => {
    graph[model.name] = [];
    inDegree[model.name] = 0;
  });

  // Add edges based on relationships
  models.forEach((model) => {
    model.fields.forEach((field) => {
      if (field.relationFromFields && field.relationFromFields.length > 0) {
        const referencedModel = field.type; // The related model name
        graph[referencedModel].push(model.name); // Add edge
        inDegree[model.name] += 1; // Increment in-degree for the child
      }
    });
  });

  // Step 4: Perform Topological Sort (Kahn's Algorithm) with cycle handling
  const queue = [];
  const insertionOrder = [];
  const skipped = new Set();

  // Find all nodes with in-degree 0 (no dependencies)
  for (const model in inDegree) {
    if (inDegree[model] === 0) {
      queue.push(model);
    }
  }

  while (queue.length > 0) {
    const current = queue.shift();
    insertionOrder.push(current);

    // Reduce the in-degree of connected nodes
    graph[current].forEach((neighbor) => {
      inDegree[neighbor] -= 1;
      if (inDegree[neighbor] === 0) {
        queue.push(neighbor);
      }
    });
  }

  // Handle remaining models with cycles (those not in the insertion order)
  for (const model of Object.keys(graph)) {
    if (!insertionOrder.includes(model)) {
      skipped.add(model);
    }
  }

  return { insertionOrder, skipped: [...skipped] };
}

module.exports = { getInsertionOrder };
