// NodePropagation.js
// Utility functions for column propagation between connected nodes

/**
 * Given the current nodes + edges, compute what columns
 * each node receives from its upstream node.
 */
export function computeNodeColumns(nodes, edges) {
  // Build adjacency: nodeId -> list of source nodeIds
  const incoming = {};
  edges.forEach(e => {
    if (!incoming[e.target]) incoming[e.target] = [];
    incoming[e.target].push(e.source);
  });

  // Topological order
  const order  = topoSort(nodes, edges);
  const result = {}; // nodeId -> string[]

  for (const nodeId of order) {
    const node      = nodes.find(n => n.id === nodeId);
    if (!node) continue;
    const type      = node.data?.type;
    const config    = node.data?.config || {};
    const sources   = incoming[nodeId] || [];

    // Get columns from first connected source
    const sourceNode   = nodes.find(n => n.id === sources[0]);
    const sourceCols   = sourceNode ? (result[sources[0]] || []) : [];

    // Input dataset: columns come from the dataset itself
    if (type === "input_dataset") {
      result[nodeId] = node.data?.config?.dataset?.columns || [];
      continue;
    }

    // Output dataset: pass through
    if (type === "output_dataset") {
      result[nodeId] = sourceCols;
      continue;
    }

    // No upstream connected yet
    if (sourceCols.length === 0) {
      result[nodeId] = [];
      continue;
    }

    // Apply transform logic to compute output columns
    result[nodeId] = applyColumnTransform(type, config, sourceCols);
  }

  return result;
}

/**
 * Simulate what columns come out of a transform node.
 */
export function applyColumnTransform(type, config, inputCols) {
  if (!inputCols || inputCols.length === 0) return [];

  switch (type) {
    case "select_col": {
      const selected = (config?.columns || []).filter(c => inputCols.includes(c));
      return selected.length > 0 ? selected : inputCols;
    }

    case "drop_col": {
      const drop = new Set(config?.columns || []);
      return inputCols.filter(c => !drop.has(c));
    }

    case "rename_col": {
      const renames = config?.renames || {};
      return inputCols.map(c => renames[c] || c);
    }

    case "add_const": {
      const name = config?.name;
      return name ? [...inputCols, name] : inputCols;
    }

    case "val_mapper": {
      const newCol = config?.newColName;
      return newCol ? [...inputCols, newCol] : inputCols;
    }

    case "group_agg": {
      const groupCols = (config?.groupCols || []).filter(c => inputCols.includes(c));
      const aggAliases = (config?.aggCols || []).map(a => a.alias).filter(Boolean);
      return [...groupCols, ...aggAliases];
    }

    case "join_data": {
      // Join adds columns from both — simplified: just pass through
      return inputCols;
    }

    case "change_type":
    case "fill_null":
    case "filter_rows":
    case "order_table":
    case "set_val":
    case "pyspark":
    default:
      return inputCols;
  }
}

/**
 * Simple topological sort of nodes based on edges.
 */
function topoSort(nodes, edges) {
  const inDegree = {};
  const graph    = {};

  nodes.forEach(n => { inDegree[n.id] = 0; graph[n.id] = []; });
  edges.forEach(e => {
    if (graph[e.source]) graph[e.source].push(e.target);
    if (inDegree[e.target] !== undefined) inDegree[e.target]++;
  });

  const queue  = nodes.filter(n => inDegree[n.id] === 0).map(n => n.id);
  const result = [];

  while (queue.length > 0) {
    const cur = queue.shift();
    result.push(cur);
    (graph[cur] || []).forEach(next => {
      inDegree[next]--;
      if (inDegree[next] === 0) queue.push(next);
    });
  }

  return result;
}

/**
 * Get the upstream columns for a specific node.
 */
export function getUpstreamColumns(nodeId, nodes, edges) {
  const colMap = computeNodeColumns(nodes, edges);
  // Find direct source of this node
  const sourceEdge = edges.find(e => e.target === nodeId);
  if (!sourceEdge) return [];
  return colMap[sourceEdge.source] || [];
}
