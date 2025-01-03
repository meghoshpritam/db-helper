const { getDMMF } = require("@prisma/sdk");
const fs = require("node:fs").promises;

async function getInsertionOrder(schemaPath) {
  try {
    const schema = await fs.readFile(schemaPath, "utf-8");
    const dmmf = await getDMMF({ datamodel: schema });

    const models = dmmf.datamodel.models;
    const graph = {};
    const inDegree = {};

    for (const model of models) {
      graph[model.name] = [];
      inDegree[model.name] = 0;
    }

    for (const model of models) {
      for (const field of model.fields) {
        if (field.relationFromFields && field.relationFromFields.length > 0) {
          const referencedModel = field.type;
          if (models.some((m) => m.name === referencedModel)) {
            graph[referencedModel].push(model.name);
            inDegree[model.name]++;
          }
        }
      }
    }

    const queue = [];
    const insertionOrder = [];

    for (const model of Object.keys(inDegree)) {
      if (inDegree[model] === 0) queue.push(model);
    }

    while (queue.length) {
      const current = queue.shift();
      insertionOrder.push(current);
      for (const neighbor of graph[current]) {
        inDegree[neighbor]--;
        if (inDegree[neighbor] === 0) queue.push(neighbor);
      }
    }

    const skipped = models
      .map((m) => m.name)
      .filter((m) => !insertionOrder.includes(m));
    const resolvedSkipped = resolveCycles(skipped, graph);
    return [...insertionOrder, ...resolvedSkipped];
  } catch (error) {
    console.error("Failed to generate insertion order:", error);
    throw error;
  }
}

function resolveCycles(skipped, graph) {
  const resolvedOrder = [];
  const path = new Set();

  function dfs(node) {
    if (path.has(node)) {
      console.warn(`Cycle detected involving: ${node}`);
      return;
    }

    path.add(node);
    graph[node].forEach(dfs);
    path.delete(node);
    resolvedOrder.push(node);
  }

  for (const node of skipped) {
    dfs(node);
  }

  return [...new Set(resolvedOrder)];
}

module.exports = { getInsertionOrder };
