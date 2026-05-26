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

  // Helper untuk extract nama kolom
  const getColName = (c) => typeof c === 'object' ? c.name : c;
  const normalizeCols = (cols) => cols.map(getColName);
  const normalized = normalizeCols(inputCols);

  switch (type) {
    case "select_col": {
      const selected = (config?.columns || []).filter(c => normalized.includes(getColName(c)));
      return selected.length > 0 ? selected : normalized;
    }

    case "drop_col": {
      const drop = new Set((config?.columns || []).map(c => getColName(c)));
      return normalized.filter(c => !drop.has(c));
    }

    case "rename_col": {
      const renames = config?.renames || {};
      return normalized.map(c => {
        // Cari apakah ada rename untuk kolom ini
        const rename = renames.find(r => r.old_name === c);
        return rename ? rename.new_name : c;
      });
    }

    case "add_const": {
      const name = config?.name || config?.columnName;
      return name ? [...normalized, name] : normalized;
    }

    case "set_val": {
      // Set value tidak menambah kolom baru, hanya mengubah value
      return normalized;
    }

    case "val_mapper": {
      const newCol = config?.newColName || config?.output_column;
      return newCol ? [...normalized, newCol] : normalized;
    }

    case "group_agg": {
      const groupCols = (config?.groupCols || []).filter(c => normalized.includes(getColName(c)));
      const aggAliases = (config?.aggCols || []).map(a => a.alias).filter(Boolean);
      return [...groupCols, ...aggAliases];
    }

        // ── Calculator Node ────────────────────────────────────────────────────
    // Adds 1 new column (or replaces if same name exists)
    case "calc": {
      const newCol = config?.newColName?.trim();
      if (!newCol) return inputCols;
      if (inputCols.includes(newCol)) return inputCols; // replace → same cols
      return [...inputCols, newCol];
    }
 
    // ── Advance Calculator Node ────────────────────────────────────────────
    // Adds N new columns (one per calculation row)
    case "adv_calculator": {
      const calculations = config?.calculations || [];
      let cols = [...inputCols];
      for (const calc of calculations) {
        const newCol = calc.newColName?.trim();
        if (newCol && !cols.includes(newCol)) {
          cols = [...cols, newCol];
        }
      }
      return cols;
    }
 
    // ── Combine Columns Node ───────────────────────────────────────────────
    // Optionally removes source cols, adds 1 new combined col
    case "combine_cols": {
      const newCol         = config?.newColName?.trim();
      const removeOriginal = config?.removeOriginal || false;
      const selectedCols   = new Set(config?.selectedCols || []);
      let cols = removeOriginal
        ? inputCols.filter(c => !selectedCols.has(c))
        : [...inputCols];
      if (newCol && !cols.includes(newCol)) {
        cols = [...cols, newCol];
      }
      return cols;
    }

    case "join_data": {
      // Join adds columns dari kedua tabel (simplifikasi: pass through saja)
      return normalized;
    }

    case "change_type":
    case "fill_null":
    case "filter_rows":
    case "order_table":
    case "pyspark":
    default:
      return normalized;
  }
}


/**
 * Simple topological sort of nodes based on edges.
 */
export function topoSort(nodes, edges) {
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

/**
 * Validasi apakah koneksi antar node valid berdasarkan jenis node
 * @param {string} sourceType - Tipe node sumber
 * @param {string} targetType - Tipe node tujuan
 * @returns {boolean} - True jika koneksi valid
 */
export function isValidNodeConnection(sourceType, targetType) {
  // Definisikan semua tipe utility node
  const utilityTypes = ["select_col", "rename_col", "drop_col", "add_const", "set_val", "val_mapper", "change_type", "fill_null", "filter_rows", "order_table", "group_agg", "join_data", "pyspark"];

  // Output dataset tidak boleh mengeluarkan koneksi
  if (sourceType === "output_dataset") {
    return false;
  }

  // Input dataset dapat terhubung ke utility dan output dataset
  if (sourceType === "input_dataset") {
    return utilityTypes.includes(targetType) || targetType === "output_dataset";
  }

  // Utility nodes dapat terhubung ke utility atau output dataset
  if (utilityTypes.includes(sourceType)) {
    return utilityTypes.includes(targetType) || targetType === "output_dataset";
  }

  return false;
}

/**
 * Cek apakah kolom dari upstream sudah siap untuk digunakan di utility node
 * @param {Array} upstreamColumns - Kolom dari node upstream
 * @returns {boolean} - True jika ada kolom yang tersedia
 */
export function isColumnAvailable(upstreamColumns) {
  return Array.isArray(upstreamColumns) && upstreamColumns.length > 0;
}

/**
 * Ambil nama kolom dari data kolom (bisa string atau object)
 * @param {string|object} column - Data kolom
 * @returns {string} - Nama kolom
 */
export function getColumnName(column) {
  if (typeof column === "string") return column;
  if (typeof column === "object" && column.name) return column.name;
  return String(column);
}