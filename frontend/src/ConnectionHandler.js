/**
 * ConnectionHandler.js
 * Menangani logika koneksi antara node-node dalam workflow editor
 * Memastikan validasi koneksi dan propagasi kolom yang benar
 */

import { isValidNodeConnection, isColumnAvailable, getColumnName } from "./NodePropagation";

/**
 * Validasi apakah sebuah koneksi dapat dibuat antara dua node
 * @param {Object} source - Node sumber
 * @param {Object} target - Node tujuan  
 * @param {Array} edges - Daftar edge yang sudah ada
 * @returns {Object} - { valid: boolean, message: string }
 */
export function validateConnection(source, target, edges = []) {
  const sourceId = source.id;
  const targetId = target.id;
  const sourceType = source.data?.type;
  const targetType = target.data?.type;

  // 1. Cek self-loop
  if (sourceId === targetId) {
    return { 
      valid: false, 
      message: "Tidak dapat menghubungkan node ke dirinya sendiri" 
    };
  }

  // 2. Cek apakah sudah ada koneksi dari source ke target
  const existingEdge = edges.find(e => e.source === sourceId && e.target === targetId);
  if (existingEdge) {
    return { 
      valid: false, 
      message: "Koneksi antara node ini sudah ada" 
    };
  }

  // 3. Validasi tipe node
  if (!isValidNodeConnection(sourceType, targetType)) {
    return { 
      valid: false, 
      message: getConnectionErrorMessage(sourceType, targetType)
    };
  }

  // 4. Jika source bukan input_dataset, cek apakah sudah dikonfigurasi
  if (sourceType !== "input_dataset" && sourceType !== "output_dataset") {
    const hasConfig = source.data?.config && Object.keys(source.data.config).length > 0;
    const hasColumns = source.data?.columns && source.data.columns.length > 0;
    
    if (!hasConfig && !hasColumns) {
      return { 
        valid: false, 
        message: `Node "${source.data?.label}" belum dikonfigurasi. Silakan konfigurasikan terlebih dahulu.`
      };
    }
  }

  return { valid: true, message: "" };
}

/**
 * Dapatkan pesan error yang sesuai untuk kombinasi tipe node
 */
function getConnectionErrorMessage(sourceType, targetType) {
  if (sourceType === "output_dataset") {
    return "Output Dataset node tidak dapat mengeluarkan koneksi";
  }

  if (sourceType === "input_dataset") {
    return "Input Dataset hanya dapat terhubung ke Utility atau Output Dataset node";
  }

  const utilityTypes = ["select_col", "rename_col", "drop_col", "add_const", "set_val", "val_mapper", "change_type", "fill_null", "filter_rows", "order_table", "group_agg", "join_data", "pyspark"];
  if (utilityTypes.includes(sourceType)) {
    return "Utility node hanya dapat terhubung ke Utility atau Output Dataset node";
  }

  return "Koneksi tidak diizinkan antara tipe node ini";
}

/**
 * Propagate kolom dari source node ke target node
 * @param {Object} sourceNode - Node sumber
 * @param {Object} targetNode - Node tujuan (akan dimodifikasi)
 * @returns {Object} - Target node yang sudah diupdate dengan kolom dari source
 */
export function propagateColumns(sourceNode, targetNode) {
  const sourceType = sourceNode.data?.type;
  const targetType = targetNode.data?.type;

  // Ambil kolom dari source node
  let sourceColumns = [];

  if (sourceType === "input_dataset") {
    // Input dataset: ambil dari config.dataset.columns
    sourceColumns = sourceNode.data?.config?.dataset?.columns || [];
  } else {
    // Utility node: ambil dari outputColumns atau columns
    sourceColumns = sourceNode.data?.outputColumns || sourceNode.data?.columns || [];
  }

  // Jika tidak ada kolom, kembalikan target node tanpa perubahan
  if (!sourceColumns || sourceColumns.length === 0) {
    return {
      ...targetNode,
      data: {
        ...targetNode.data,
        columns: [],
        upstreamColumns: [],
      }
    };
  }

  // Update target node dengan kolom dari source
  return {
    ...targetNode,
    data: {
      ...targetNode.data,
      columns: sourceColumns,
      upstreamColumns: sourceColumns,
      isConnected: true,
    }
  };
}

/**
 * Validasi apakah utility node sudah siap untuk dikonfigurasi
 * @param {Object} node - Node yang akan dikonfigurasi
 * @returns {Object} - { ready: boolean, message: string }
 */
export function isNodeReadyForConfig(node) {
  const nodeType = node.data?.type;

  // Input dataset selalu siap
  if (nodeType === "input_dataset" || nodeType === "output_dataset") {
    return { ready: true, message: "" };
  }

  // Utility node harus punya kolom dari upstream
  const hasColumns = node.data?.columns && node.data.columns.length > 0;
  const hasUpstreamColumns = node.data?.upstreamColumns && node.data.upstreamColumns.length > 0;

  if (!hasColumns && !hasUpstreamColumns) {
    return { 
      ready: false, 
      message: `Hubungkan "${node.data?.label}" ke Input Dataset atau node sebelumnya terlebih dahulu` 
    };
  }

  return { ready: true, message: "" };
}

/**
 * Format kolom untuk ditampilkan di UI
 * @param {Array} columns - Daftar kolom
 * @returns {Array} - Kolom yang sudah di-format
 */
export function formatColumnsForDisplay(columns) {
  if (!Array.isArray(columns)) return [];
  
  return columns.map(col => {
    if (typeof col === "string") return col;
    if (typeof col === "object" && col.name) return col.name;
    return String(col);
  });
}

/**
 * Cek apakah ada circular dependency jika kita membuat edge baru
 * @param {string} sourceId - ID node sumber
 * @param {string} targetId - ID node tujuan
 * @param {Array} nodes - Daftar semua node
 * @param {Array} edges - Daftar semua edge (belum termasuk edge baru)
 * @returns {boolean} - True jika akan membuat circular dependency
 */
export function wouldCreateCircularDependency(sourceId, targetId, nodes, edges) {
  // BFS dari target untuk lihat apakah bisa reach ke source
  const graph = {};
  nodes.forEach(n => graph[n.id] = []);
  edges.forEach(e => graph[e.source].push(e.target));

  // Tambahkan edge baru yang akan dibuat
  if (!graph[sourceId]) graph[sourceId] = [];
  graph[sourceId].push(targetId);

  // BFS dari targetId untuk check apakah bisa reach sourceId
  const visited = new Set();
  const queue = [targetId];
  
  while (queue.length > 0) {
    const current = queue.shift();
    if (current === sourceId) return true; // Found cycle
    if (visited.has(current)) continue;
    
    visited.add(current);
    (graph[current] || []).forEach(next => queue.push(next));
  }

  return false; // No cycle found
}
