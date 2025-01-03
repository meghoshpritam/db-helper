const { getDMMF } = require("@prisma/sdk");
const fs = require("node:fs").promises;

async function getInsertionOrder(schemaPath) {
  const schema = await fs.readFile(schemaPath, "utf-8");
  const dmmf = await getDMMF({ datamodel: schema });

  const models = dmmf.datamodel.models;
  const graph = {};
  models.forEach((model) => {
    graph[model.name] = new Set(); // Use Set to prevent duplicates
  });

  models.forEach((model) => {
    model.fields.forEach((field) => {
      if (field.relationFromFields && field.relationFromFields.length > 0) {
        const target = field.type;
        if (models.some((m) => m.name === target)) {
          graph[target].add(model.name);
        }
      }
    });
  });

  // Convert Set back to Array for processing
  Object.keys(graph).forEach((key) => (graph[key] = Array.from(graph[key])));

  const result = resolveDependencies(graph);
  return result;
}

function resolveDependencies(graph) {
  const sorted = [];
  const visited = new Set();
  const temporary = new Set();
  const cyclicNodes = new Set();

  function visit(node) {
    if (temporary.has(node)) {
      console.error(`Cycle detected at: ${node}`);
      cyclicNodes.add(node);
      return false; // Note the cycle but do not stop the process
    }
    if (!visited.has(node)) {
      temporary.add(node);
      graph[node].forEach((neighbor) => {
        if (!visited.has(neighbor)) {
          visit(neighbor);
        }
      });
      visited.add(node);
      temporary.delete(node);
      sorted.push(node);
    }
    return true;
  }

  Object.keys(graph).forEach((node) => {
    if (!visited.has(node) && !cyclicNodes.has(node)) {
      visit(node);
    }
  });

  // Append cyclic nodes at the end for further review or manual processing
  cyclicNodes.forEach((node) => {
    if (!sorted.includes(node)) {
      sorted.push(node);
    }
  });

  return {
    sortedOrder: sorted.reverse(),
    cyclicNodes: Array.from(cyclicNodes),
  };
}

module.exports = { getInsertionOrder };
