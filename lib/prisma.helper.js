const { getDMMF } = require("@prisma/sdk");
const fs = require("node:fs").promises;

async function getInsertionOrder(schemaPath) {
  try {
    const schema = await fs.readFile(schemaPath, "utf-8");
    const dmmf = await getDMMF({ datamodel: schema });

    const models = dmmf.datamodel.models;
    const graph = {};
    const reverseGraph = {};
    const allNodes = new Set();

    // Initialize graphs and nodes set
    models.forEach((model) => {
      graph[model.name] = [];
      reverseGraph[model.name] = [];
      allNodes.add(model.name);
    });

    // Build the graph and reverse graph
    models.forEach((model) => {
      model.fields.forEach((field) => {
        if (field.relationFromFields && field.relationFromFields.length > 0) {
          const referencedModel = field.type;
          if (allNodes.has(referencedModel)) {
            graph[referencedModel].push(model.name);
            reverseGraph[model.name].push(referencedModel);
          }
        }
      });
    });

    // Find all strongly connected components
    const { scc, order } = stronglyConnectedComponents(
      graph,
      reverseGraph,
      Array.from(allNodes)
    );
    const resolvedOrder = resolveSCC(scc, order, graph);

    return resolvedOrder;
  } catch (error) {
    console.error("Failed to generate insertion order:", error);
    throw error;
  }
}

function stronglyConnectedComponents(graph, reverseGraph, nodes) {
  let index = 0;
  const indices = {};
  const lowLink = {};
  const inStack = new Set();
  const stack = [];
  const scc = [];
  const order = [];

  function strongConnect(node) {
    indices[node] = lowLink[node] = index++;
    stack.push(node);
    inStack.add(node);

    graph[node].forEach((neighbor) => {
      if (!(neighbor in indices)) {
        // Successor has not yet been visited; recurse on it
        strongConnect(neighbor);
        lowLink[node] = Math.min(lowLink[node], lowLink[neighbor]);
      } else if (inStack.has(neighbor)) {
        // Successor is in the stack and hence in the current SCC
        lowLink[node] = Math.min(lowLink[node], indices[neighbor]);
      }
    });

    // If node is a root node, pop the stack and generate an SCC
    if (lowLink[node] === indices[node]) {
      let component = [];
      let w;
      do {
        w = stack.pop();
        inStack.delete(w);
        component.push(w);
      } while (w !== node);
      scc.push(component);
    }
  }

  for (const node of nodes) {
    if (!(node in indices)) {
      strongConnect(node);
    }
  }

  // Order nodes considering SCCs
  nodes.forEach((node) => {
    if (scc.every((comp) => !comp.includes(node))) {
      order.push(node);
    }
  });
  scc.forEach((comp) => {
    if (comp.length > 1) order.push(comp); // Treat entire component as a single unit
  });

  return { scc, order };
}

function resolveSCC(scc, order, graph) {
  const resolvedOrder = [];
  const resolved = new Set();

  order.forEach((element) => {
    if (Array.isArray(element)) {
      // Resolve within the SCC
      element.forEach((node) => {
        if (!resolved.has(node)) {
          dfsResolve(node, graph, resolved, resolvedOrder);
        }
      });
    } else {
      // Single node, resolve normally
      dfsResolve(element, graph, resolved, resolvedOrder);
    }
  });

  return resolvedOrder;
}

function dfsResolve(node, graph, resolved, resolvedOrder) {
  if (resolved.has(node)) return;
  resolved.add(node);
  graph[node].forEach((neighbor) => {
    if (!resolved.has(neighbor)) {
      dfsResolve(neighbor, graph, resolved, resolvedOrder);
    }
  });
  resolvedOrder.push(node);
}

module.exports = { getInsertionOrder };
