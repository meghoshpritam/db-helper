const { getDMMF } = require("@prisma/sdk");
const fs = require("node:fs");

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
  for (const model of models) {
    graph[model.name] = [];
    inDegree[model.name] = 0;
  }

  // Add edges based on relationships
  for (const model of models) {
    for (const field of model.fields) {
      if (field.relationFromFields && field.relationFromFields.length > 0) {
        const referencedModel = field.type; // The related model name
        if (referencedModel === model.name) {
          // Self-referential relationship
          console.warn(
            `Self-referential relationship detected in model: ${model.name}`
          );
        } else {
          graph[referencedModel].push(model.name); // Add edge
          inDegree[model.name] += 1; // Increment in-degree for the child
        }
      }
    }
  }

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
    for (const neighbor of graph[current]) {
      inDegree[neighbor] -= 1;
      if (inDegree[neighbor] === 0) {
        queue.push(neighbor);
      }
    }
  }

  // Handle remaining models with cycles (those not in the insertion order)
  for (const model of Object.keys(graph)) {
    if (!insertionOrder.includes(model)) {
      skipped.add(model);
    }
  }

  // Resolve cycles in the skipped list
  const resolvedSkipped = resolveCycles([...skipped], graph);

  // Merge resolved items into the final insertion order
  const finalInsertionOrder = [...insertionOrder, ...resolvedSkipped];

  return { insertionOrder: finalInsertionOrder, skipped: [...skipped] };
}

// Optimized function to resolve cycles in the graph
function resolveCycles(skipped, graph) {
  const resolvedOrder = [];
  const visited = new Set();
  const resolvedSet = new Set();

  function dfs(node, path) {
    if (resolvedSet.has(node)) return; // Already resolved
    if (path.has(node)) {
      // Cycle detected
      console.warn(`Cycle detected involving: ${[...path, node].join(" -> ")}`);
      resolvedOrder.push(node); // Add node to the resolved order anyway
      resolvedSet.add(node);
      return;
    }

    path.add(node);

    for (const neighbor of graph[node]) {
      dfs(neighbor, path);
    }

    path.delete(node);
    resolvedSet.add(node);
    resolvedOrder.push(node);
  }

  for (const model of skipped) {
    if (!visited.has(model)) {
      dfs(model, new Set());
    }
  }

  return [...new Set(resolvedOrder)]; // Remove duplicates and return final order
}

if (require.main === module) {
  const schemaPath = process.argv[2];
  if (!schemaPath) {
    console.error("Usage: node lib/prisma.helper.js <schemaPath>");
    process.exit(1);
  }
  const savePath = process.argv[3];
  getInsertionOrder(schemaPath)
    .then((result) => {
      console.log(result);
      if (savePath) {
        fs.writeFileSync(savePath, JSON.stringify(result, null, 2));
        console.log(`\n\nInsertion order saved to ${savePath}`);
      }
    })
    .catch(console.error)
    .finally(() => {
      process.exit(0);
    });
}

module.exports = { getInsertionOrder };
