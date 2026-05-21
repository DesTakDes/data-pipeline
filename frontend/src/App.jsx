import { useState, useCallback, useEffect, useMemo, useRef } from "react";
import { UtilityConfigModal } from "./UtilityConfigs";
import {
  ReactFlow, Background, Controls, MiniMap, addEdge,
  useNodesState, useEdgesState, Panel, Handle, Position,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import axios from "axios";
import { computeNodeColumns, getUpstreamColumns } from "./NodePropagation";

const api = axios.create({ baseURL: "/api" });

const API = {
  getDatasets:    ()      => api.get("/datasets").catch(() => ({ data: [] })),
  deleteDataset:  (id)    => api.delete(`/datasets/${id}`),
  previewDataset: (id, n=50) => api.get(`/datasets/${id}/preview?limit=${n}`),
  uploadDataset:  (file, name) => {
    const f = new FormData(); f.append("file", file);
    if (name) f.append("name", name);
    return api.post("/datasets/upload", f);
  },
  connectDB:      (p) => api.post("/datasets/connect-db", p),
  previewTransform:(payload) => api.post("/preview/transform", payload),
  getWorkflows:   ()  => { try { return JSON.parse(localStorage.getItem("etl_workflows_v2") || "[]"); } catch { return []; } },
  saveWorkflow:   (w) => { const ws = API.getWorkflows().filter(x => x.id !== w.id); ws.push(w); localStorage.setItem("etl_workflows_v2", JSON.stringify(ws)); return w; },
  deleteWorkflow: (id)=> { const ws = API.getWorkflows().filter(x => x.id !== id); localStorage.setItem("etl_workflows_v2", JSON.stringify(ws)); },
  getWarehouseTables: () => api.get("/warehouse/tables").catch(() => ({ data: [] })),
  runPipeline:    (payload) => api.post("/pipelines/run", payload),
  getPipelineRuns:()       => api.get("/pipelines/runs").catch(() => ({ data: [] })),
  previewRun:     (id)     => api.get(`/pipelines/runs/${id}/preview`),
  getDagStatus:   (id)     => api.get(`/pipelines/runs/${id}/dag-status`),
  downloadRun:    (id, fmt)=> `/api/pipelines/runs/${id}/download?format=${fmt}`,
};

const C = {
  navy:"#0B1E3D", navyMid:"#122850", navyLight:"#1A3A6B",
  blue:"#1D6FEB", blueMid:"#3B82F6", blueLight:"#93C5FD",
  blueTint:"#EFF6FF", blueTint2:"#DBEAFE",
  gold:"#F59E0B", goldTint:"#FFFBEB",
  spark:"#E25822", sparkTint:"#FFF0EC",
  white:"#FFFFFF", off:"#F8FAFC",
  g50:"#F8FAFC", g100:"#F1F5F9", g200:"#E2E8F0",
  g300:"#CBD5E1", g400:"#94A3B8", g500:"#64748B", g600:"#475569", g700:"#334155",
  green:"#16A34A", greenTint:"#DCFCE7",
  red:"#DC2626", redTint:"#FEE2E2",
  orange:"#EA580C",
};

const Ic = {
  DB:       () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="18" height="18"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>,
  Chart:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="18" height="18"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M8 17l3-4 3 3 3-5"/></svg>,
  Flow:     () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="18" height="18"><rect x="3" y="3" width="6" height="6" rx="1"/><rect x="15" y="3" width="6" height="6" rx="1"/><rect x="9" y="15" width="6" height="6" rx="1"/><path d="M6 9v3a3 3 0 003 3h6a3 3 0 003-3V9"/></svg>,
  Upload:   () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>,
  Plus:     () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" width="15" height="15"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>,
  Search:   () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="15" height="15"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>,
  Trash:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6M14 11v6M9 6V4h6v2"/></svg>,
  Copy:     () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="13" height="13"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>,
  Eye:      () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="13" height="13"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>,
  Edit:     () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="13" height="13"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>,
  Play:     () => <svg viewBox="0 0 24 24" fill="currentColor" width="13" height="13"><polygon points="5 3 19 12 5 21 5 3"/></svg>,
  X:        () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>,
  Refresh:  () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 11-2.12-9.36L23 10"/></svg>,
  Download: () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="13" height="13"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>,
  Save:     () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><path d="M19 21H5a2 2 0 01-2-2V5a2 2 0 012-2h11l5 5v11a2 2 0 01-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>,
  Back:     () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16"><line x1="19" y1="12" x2="5" y2="12"/><polyline points="12 19 5 12 12 5"/></svg>,
  Link:     () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>,
  Spark:    () => <svg viewBox="0 0 24 24" fill="currentColor" width="14" height="14"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>,
  Menu:     () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="18" height="18"><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="18" x2="21" y2="18"/></svg>,
  ArrowRight: () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>,
  Branch:   () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><circle cx="6" cy="3" r="2"/><circle cx="6" cy="21" r="2"/><circle cx="18" cy="9" r="2"/><path d="M6 5v14M6 5c3.3 0 10.5 2 10.5 4"/></svg>,
};

// Node palette
const NODE_PALETTE = {
  datasets: {
    label: "Datasets", color: C.blue,
    items: [
      { label: "Input Dataset",  type: "input_dataset" },
      { label: "Output Dataset", type: "output_dataset" },
    ],
  },
  utility: {
    label: "Utility", color: C.gold,
    items: [
      { label: "Select Column",        type: "select_col" },
      { label: "Rename Columns",       type: "rename_col" },
      { label: "Drop Columns",         type: "drop_col" },
      { label: "Add Constant",         type: "add_const" },
      { label: "Set Column Value",     type: "set_val" },
      { label: "Value Mapper",         type: "val_mapper" },
      { label: "Change Data Type",     type: "change_type" },
      { label: "Fill NULL",            type: "fill_null" },
      { label: "Filter Rows",          type: "filter_rows" },
      { label: "Order Table",          type: "order_table" },
      { label: "Group By & Aggregate", type: "group_agg" },
      { label: "Join Data",            type: "join_data" },
      { label: "PySpark Node",         type: "pyspark" },
    ],
  },
};

const NODE_BG = {
  input_dataset: C.blue, output_dataset: C.green,
  filter_rows: C.blueMid, fill_null: C.blueMid,
  group_agg: C.navyLight, join_data: C.blue,
  pyspark: C.spark, drop_col: C.red, default: C.gold,
};
const nodeBg = (type) => NODE_BG[type] || NODE_BG.default;

// ── ETL Node ──────────────────────────────────────────────────────────────────
function ETLNode({ id, data }) {
  const bg      = nodeBg(data.type);
  const isInput  = data.type === "input_dataset";
  const isOutput = data.type === "output_dataset";
  const isUtil   = !isInput && !isOutput;
  const upCols   = data.upstreamColumns || [];
  const outCols  = data.outputColumns   || [];
  const connected= data.isConnected || false;
  const configured = data.config && (
    isInput ? !!data.config.dataset :
    isOutput ? !!data.config.outputName :
    Object.keys(data.config).length > 0
  );

  return (
    <div style={{
      background: C.white, border: `2px solid ${bg}`, borderRadius: 10,
      minWidth: 200, maxWidth: 260, boxShadow: `0 4px 16px ${bg}22`,
      fontFamily: "'DM Sans',sans-serif", overflow: "hidden",
    }}>
      {!isInput  && <Handle type="target" position={Position.Left}  style={{ background: bg, width: 10, height: 10, border: `2px solid ${C.white}` }} />}
      {!isOutput && <Handle type="source" position={Position.Right} style={{ background: bg, width: 10, height: 10, border: `2px solid ${C.white}` }} />}

      {/* Header */}
      <div style={{ background: bg, padding: "6px 8px", display: "flex", alignItems: "center", gap: 4 }}>
        <span style={{ color: C.white, fontSize: 11, fontWeight: 700, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {isInput ? "📥 " : isOutput ? "📤 " : data.type === "pyspark" ? "⚡ " : ""}{data.label}
        </span>
        <button onClick={() => data.onPreview(id)} title="Preview" style={{ background: "rgba(255,255,255,0.18)", border: "none", borderRadius: 4, width: 20, height: 20, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", color: C.white, flexShrink: 0 }}><Ic.Eye /></button>
        <button onClick={() => data.onDuplicate(id)} title="Duplicate" style={{ background: "rgba(255,255,255,0.18)", border: "none", borderRadius: 4, width: 20, height: 20, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", color: C.white, flexShrink: 0 }}><Ic.Copy /></button>
        <button onClick={() => data.onDelete(id)} title="Delete" style={{ background: "rgba(255,255,255,0.18)", border: "none", borderRadius: 4, width: 20, height: 20, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", color: C.white, flexShrink: 0 }}><Ic.Trash /></button>
      </div>

      {/* Body */}
      <div style={{ padding: "8px 10px" }}>
        {isInput && (
          data.config?.dataset ? (
            <div>
              <div style={{ background: C.blueTint, borderRadius: 6, padding: "5px 8px", marginBottom: 6 }}>
                <div style={{ fontSize: 10, fontWeight: 700, color: C.blue, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{data.config.dataset.name}</div>
                <div style={{ fontSize: 9, color: C.g400 }}>{data.config.dataset.row_count?.toLocaleString() || "?"} rows · {outCols.length} cols</div>
                {data.config.dataset.is_large && <div style={{ fontSize: 9, color: C.spark, fontWeight: 700 }}>⚡ Large file → Parquet</div>}
              </div>
              <button onClick={() => data.onConfigure(id)} style={{ width: "100%", padding: "3px 0", borderRadius: 5, border: `1px solid ${C.blue}44`, background: "none", color: C.blue, fontSize: 10, cursor: "pointer" }}>✎ Change Dataset</button>
            </div>
          ) : (
            <button onClick={() => data.onConfigure(id)} style={{ width: "100%", padding: "6px 0", borderRadius: 6, border: `1px dashed ${C.blue}`, background: C.blueTint, color: C.blue, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>+ Select Dataset</button>
          )
        )}

        {isOutput && (
          data.config?.outputName ? (
            <div>
              <div style={{ background: C.greenTint, borderRadius: 6, padding: "5px 8px", marginBottom: 4 }}>
                <div style={{ fontSize: 10, fontWeight: 700, color: C.green }}>warehouse.{data.config.outputName}</div>
                <div style={{ fontSize: 9, color: C.g400 }}>{upCols.length} cols · Task: {data.config.taskId || "task_1"}</div>
              </div>
              <div style={{ display: "flex", gap: 3, marginBottom: 4 }}>
                <button onClick={() => data.onConfigure(id)} style={{ flex: 1, padding: "3px 0", borderRadius: 5, border: `1px solid ${C.green}44`, background: "none", color: C.green, fontSize: 9, fontWeight: 700, cursor: "pointer" }}>✎ Edit</button>
                <button onClick={() => data.onSaveAs && data.onSaveAs(id, "csv")} style={{ flex: 1, padding: "3px 0", borderRadius: 5, border: `1px solid ${C.blue}44`, background: C.blueTint, color: C.blue, fontSize: 9, fontWeight: 700, cursor: "pointer" }}>⬇ CSV</button>
                <button onClick={() => data.onSaveAs && data.onSaveAs(id, "parquet")} style={{ flex: 1, padding: "3px 0", borderRadius: 5, border: `1px solid ${C.spark}44`, background: C.sparkTint, color: C.spark, fontSize: 9, fontWeight: 700, cursor: "pointer" }}>⬇ PKT</button>
              </div>
            </div>
          ) : (
            <button onClick={() => data.onConfigure(id)} style={{ width: "100%", padding: "6px 0", borderRadius: 6, border: `1px dashed ${C.green}`, background: C.greenTint, color: C.green, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>+ Configure Output</button>
          )
        )}

        {isUtil && (
          !connected ? (
            <div style={{ fontSize: 10, color: C.g400, textAlign: "center", padding: "4px 0", fontStyle: "italic" }}>← Connect to upstream node</div>
          ) : !configured ? (
            <button onClick={() => data.onConfigure(id)} style={{ width: "100%", padding: "5px 0", borderRadius: 6, border: `1px dashed ${C.gold}`, background: C.goldTint, color: C.gold, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>+ Configure ({upCols.length} cols)</button>
          ) : (
            <div>
              <div style={{ background: C.greenTint, borderRadius: 5, padding: "3px 7px", marginBottom: 4, fontSize: 10, color: C.green, fontWeight: 600, display: "flex", justifyContent: "space-between" }}>
                <span>✓ Configured</span>
                <button onClick={() => data.onConfigure(id)} style={{ background: "none", border: "none", cursor: "pointer", color: C.green, fontSize: 10, padding: 0 }}>✎ Edit</button>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: C.g400, marginBottom: 3 }}>
                <span>In: {upCols.length}</span><span>→</span><span>Out: {outCols.length}</span>
              </div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 2 }}>
                {outCols.slice(0, 5).map(c => (
                  <span key={c} style={{ fontSize: 8, padding: "1px 4px", borderRadius: 3, background: C.blueTint2, color: C.blue, fontWeight: 600, fontFamily: "monospace" }}>{c}</span>
                ))}
                {outCols.length > 5 && <span style={{ fontSize: 8, color: C.g400 }}>+{outCols.length - 5}</span>}
              </div>
            </div>
          )
        )}
      </div>
    </div>
  );
}

const nodeTypes = { etlNode: ETLNode };

// ── Preview Panel ─────────────────────────────────────────────────────────────
function NodePreviewPanel({ node, datasets, edges, nodes, onClose }) {
  const [loading, setLoading] = useState(false);
  const [data, setData]       = useState(null);
  const [error, setError]     = useState(null);

  const isInput  = node?.data?.type === "input_dataset";
  const isOutput = node?.data?.type === "output_dataset";
  const dsId = node?.data?.config?.dataset?.id;

  useEffect(() => {
    if (!node) return;
    if (isInput && dsId) {
      setLoading(true);
      API.previewDataset(dsId, 50)
        .then(r => setData(r.data))
        .catch(() => setError("Could not load preview"))
        .finally(() => setLoading(false));
    } else if (!isInput && !isOutput) {
      // Find source dataset
      const colMap = computeNodeColumns(nodes, edges);
      const sourceEdge = edges.find(e => e.target === node.id);
      if (!sourceEdge) {
        setData({ columns: [], rows: [], info: "Connect to upstream node first" });
        return;
      }
      // Trace back to input dataset
      const inputNode = nodes.find(n => n.data?.type === "input_dataset" && n.data?.config?.dataset?.id);
      if (!inputNode) {
        setData({ columns: colMap[node.id] || [], rows: [], info: "No input dataset connected" });
        return;
      }
      // Build transform chain up to this node
      const chain = buildTransformChain(node.id, nodes, edges);
      setLoading(true);
      API.previewTransform({
        dataset_id: inputNode.data.config.dataset.id,
        transforms: chain,
        limit: 50,
      })
        .then(r => setData(r.data))
        .catch(e => setData({ columns: colMap[node.id] || [], rows: [], info: "Preview needs connected pipeline" }))
        .finally(() => setLoading(false));
    } else {
      setData({ columns: [], rows: [] });
    }
  }, [node?.id]);

  const downloadCSV = () => {
    if (!data?.rows?.length) return;
    const h = data.columns.join(",");
    const b = data.rows.map(r => data.columns.map(c => {
      const v = r[c]; if (v == null) return "";
      const s = String(v);
      return s.includes(",") || s.includes('"') ? `"${s.replace(/"/g,'""')}"` : s;
    }).join(",")).join("\n");
    const blob = new Blob([h + "\n" + b], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a"); a.href = url; a.download = `${node.data.label}_preview.csv`; a.click();
    URL.revokeObjectURL(url);
  };

  if (!node) return null;
  const bg = nodeBg(node.data?.type);

  return (
    <div style={{ position: "absolute", right: 0, top: 0, bottom: 0, width: 480, background: C.white, borderLeft: `1px solid ${C.g200}`, display: "flex", flexDirection: "column", zIndex: 50, boxShadow: "-4px 0 24px rgba(0,0,0,0.10)", fontFamily: "'DM Sans',sans-serif" }}>
      <div style={{ background: `linear-gradient(90deg,${C.navy},${bg}33)`, padding: "12px 16px", display: "flex", justifyContent: "space-between", alignItems: "center", flexShrink: 0 }}>
        <div>
          <div style={{ color: C.white, fontWeight: 800, fontSize: 14 }}>{node.data?.label}</div>
          <div style={{ color: "rgba(255,255,255,0.6)", fontSize: 10 }}>{data?.columns?.length || 0} cols · {data?.rows?.length || 0} rows preview</div>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          {data?.rows?.length > 0 && <button onClick={downloadCSV} style={{ display: "flex", alignItems: "center", gap: 4, padding: "5px 10px", borderRadius: 6, background: "rgba(255,255,255,0.15)", border: "1px solid rgba(255,255,255,0.2)", color: C.white, fontSize: 11, fontWeight: 700, cursor: "pointer" }}><Ic.Download /> CSV</button>}
          <button onClick={onClose} style={{ background: "rgba(255,255,255,0.1)", border: "none", borderRadius: 6, padding: "4px 8px", cursor: "pointer", color: C.white, fontSize: 16 }}>×</button>
        </div>
      </div>

      {/* Column chips */}
      {data?.columns?.length > 0 && (
        <div style={{ padding: "8px 14px", borderBottom: `1px solid ${C.g200}`, background: C.g50, flexShrink: 0 }}>
          <div style={{ fontSize: 10, fontWeight: 700, color: C.g400, textTransform: "uppercase", letterSpacing: 1, marginBottom: 5 }}>Columns ({data.columns.length})</div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 4, maxHeight: 72, overflowY: "auto" }}>
            {data.columns.map(c => <span key={c} style={{ fontSize: 10, padding: "2px 7px", borderRadius: 4, background: C.blueTint2, color: C.blue, fontWeight: 600, fontFamily: "monospace" }}>{c}</span>)}
          </div>
        </div>
      )}

      <div style={{ flex: 1, overflow: "auto" }}>
        {loading && <div style={{ padding: 40, textAlign: "center", color: C.g400 }}>Loading preview…</div>}
        {error && <div style={{ padding: 20 }}><div style={{ background: C.redTint, borderRadius: 8, padding: 12, fontSize: 12, color: C.red }}>{error}</div></div>}
        {data?.info && !loading && <div style={{ padding: 20, textAlign: "center", color: C.g400, fontSize: 12 }}>{data.info}</div>}
        {data?.rows?.length > 0 && !loading && (
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead><tr style={{ background: C.g50, position: "sticky", top: 0 }}>
              <th style={{ padding: "5px 8px", color: C.g400, fontWeight: 600, borderBottom: `1px solid ${C.g200}`, fontSize: 9 }}>#</th>
              {data.columns.map(c => <th key={c} style={{ padding: "5px 8px", textAlign: "left", fontWeight: 700, color: C.g600, borderBottom: `2px solid ${C.g200}`, whiteSpace: "nowrap", fontFamily: "monospace" }}>{c}</th>)}
            </tr></thead>
            <tbody>
              {data.rows.map((row, i) => (
                <tr key={i} style={{ background: i % 2 === 0 ? C.white : C.g50 }}>
                  <td style={{ padding: "4px 8px", color: C.g300, fontSize: 9, textAlign: "center", borderBottom: `1px solid ${C.g100}` }}>{i+1}</td>
                  {data.columns.map(c => (
                    <td key={c} style={{ padding: "4px 8px", borderBottom: `1px solid ${C.g100}`, color: row[c] == null ? C.g300 : C.g700, fontStyle: row[c] == null ? "italic" : "normal", whiteSpace: "nowrap", maxWidth: 160, overflow: "hidden", textOverflow: "ellipsis" }}>
                      {row[c] == null ? "null" : String(row[c])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {!loading && !error && (!data || data.rows?.length === 0) && !data?.info && (
          <div style={{ padding: 40, textAlign: "center", color: C.g400 }}>
            <div style={{ fontSize: 28, marginBottom: 8 }}>📭</div>
            <div>No data or node not configured</div>
          </div>
        )}
      </div>
    </div>
  );
}

// Build transform chain from root to a given node
function buildTransformChain(targetNodeId, nodes, edges) {
  // Trace path from input to target
  const path = [];
  let current = targetNodeId;
  while (current) {
    const node = nodes.find(n => n.id === current);
    if (!node) break;
    if (node.data.type !== "input_dataset") {
      path.unshift({ type: node.data.type, config: node.data.config || {} });
    }
    const sourceEdge = edges.find(e => e.target === current);
    current = sourceEdge?.source;
    if (node.data.type === "input_dataset") break;
  }
  return path;
}

// ── Toast ─────────────────────────────────────────────────────────────────────
function Toast({ toasts }) {
  return (
    <div style={{ position: "fixed", bottom: 24, right: 24, zIndex: 9999, display: "flex", flexDirection: "column", gap: 8, pointerEvents: "none" }}>
      {toasts.map(t => (
        <div key={t.id} style={{ padding: "10px 18px", borderRadius: 10, fontSize: 13, fontWeight: 600, background: t.type === "success" ? C.green : t.type === "error" ? C.red : C.blue, color: C.white, boxShadow: "0 4px 16px rgba(0,0,0,0.18)" }}>{t.msg}</div>
      ))}
    </div>
  );
}

function Modal({ title, onClose, children, width = 480 }) {
  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(11,30,61,.6)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000 }} onClick={onClose}>
      <div style={{ background: C.white, borderRadius: 16, width, maxWidth: "92vw", maxHeight: "90vh", boxShadow: "0 24px 64px rgba(0,0,0,.22)", overflow: "hidden", display: "flex", flexDirection: "column" }} onClick={e => e.stopPropagation()}>
        <div style={{ background: `linear-gradient(90deg,${C.navy},${C.navyLight})`, padding: "16px 20px", display: "flex", justifyContent: "space-between", alignItems: "center", flexShrink: 0 }}>
          <span style={{ color: C.white, fontWeight: 800, fontSize: 15 }}>{title}</span>
          <button onClick={onClose} style={{ background: "rgba(255,255,255,.1)", border: "none", borderRadius: 6, padding: "4px 8px", cursor: "pointer", color: C.white }}><Ic.X /></button>
        </div>
        <div style={{ padding: 22, overflowY: "auto" }}>{children}</div>
      </div>
    </div>
  );
}

// ── Workflow Editor ────────────────────────────────────────────────────────────
function WorkflowEditor({ workflow, datasets, onSave, onBack, toast }) {
  const [nodes, setNodes, onNodesChange] = useNodesState(workflow.nodes || []);
  const [edges, setEdges, onEdgesChange] = useEdgesState(workflow.edges || []);
  const [wfTab, setWfTab]       = useState("datasets");
  const [counter, setCounter]   = useState(100);
  const [running, setRunning]   = useState(false);
  const [dagStatus, setDagStatus] = useState(null);
  const [taskStates, setTaskStates] = useState({});
  const [configModal, setConfigModal] = useState(null);
  const [utilNode, setUtilNode] = useState(null);
  const [previewNode, setPreviewNode] = useState(null);
  const [outputSidebarNodeId, setOutputSidebarNodeId] = useState(null);
  const [inputConfig, setInputConfig] = useState({ datasetId: "" });
  const [outputConfig, setOutputConfig] = useState({ outputName: "", description: "", taskId: "" });
  const pollRef = useRef(null);

  // Column propagation
  const columnMap = useMemo(() => computeNodeColumns(nodes, edges), [nodes, edges]);

  useEffect(() => {
  setNodes(ns => {
    let changed = false;
    const nextNodes = ns.map(n => {
      const sourceEdge = edges.find(e => e.target === n.id);
      const upstreamCols = sourceEdge ? (columnMap[sourceEdge.source] || []) : [];
      const outputCols   = columnMap[n.id] || [];
      const isConnected  = edges.some(e => e.target === n.id);
      
      const isUpstreamChanged = JSON.stringify(n.data.upstreamColumns) !== JSON.stringify(upstreamCols);
      const isOutputChanged = JSON.stringify(n.data.outputColumns) !== JSON.stringify(outputCols);
      const isConnectionChanged = n.data.isConnected !== isConnected;

      if (isUpstreamChanged || isOutputChanged || isConnectionChanged) {
        changed = true;
        return { 
          ...n, 
          data: { 
            ...n.data, 
            upstreamColumns: upstreamCols, 
            outputColumns: outputCols, 
            isConnected 
          } 
        };
      }
      return n;
    });

    // JIKA ada perubahan nyata, kembalikan array baru.
    // JIKA TIDAK, kembalikan referensi array lama (ns) untuk menghentikan loop.
    return changed ? nextNodes : ns;
  });
}, [columnMap, edges, setNodes]);

  const buildCallbacks = useCallback(() => ({
    onDelete:    (nid) => setNodes(ns => ns.filter(n => n.id !== nid)),
    onDuplicate: (nid) => setNodes(ns => {
      const o = ns.find(n => n.id === nid); if (!o) return ns;
      return [...ns, { ...o, id: `n${Date.now()}`, position: { x: o.position.x + 30, y: o.position.y + 30 } }];
    }),
    onConfigure: (nid) => setNodes(ns => {
      const n = ns.find(x => x.id === nid); if (!n) return ns;
      const isIO = ["input_dataset","output_dataset"].includes(n.data.type);
      if (isIO) {
        if (n.data.type === "output_dataset") {
          // Pre-fill existing config for re-edit
          setOutputConfig({
            outputName: n.data.config?.outputName || "",
            description: n.data.config?.description || "",
            taskId: n.data.config?.taskId || `task_${Date.now().toString().slice(-4)}`,
          });
          setOutputSidebarNodeId(nid);
        } else {
          setInputConfig({ datasetId: "" });
          setConfigModal({ nodeId: nid, type: n.data.type });
        }
      } else {
        setUtilNode(n);
      }
      return ns;
    }),
    onPreview: (nid) => setNodes(ns => {
      const n = ns.find(x => x.id === nid);
      if (n) setPreviewNode(n);
      return ns;
    }),
    onSaveAs: async (nid, format) => {
      // Find the run_id for this output node by output name
      setNodes(ns => {
        const n = ns.find(x => x.id === nid);
        if (!n?.data?.config?.outputName) {
          toast("Configure output first, then run pipeline before saving", "error");
          return ns;
        }
        const outName = n.data.config.outputName;
        // Trigger download directly from warehouse
        const url = `/api/warehouse/${outName}/download?format=${format}`;
        const a = document.createElement("a");
        a.href = url;
        a.download = `${outName}.${format === "parquet" ? "parquet" : "csv"}`;
        a.click();
        return ns;
      });
    },
  }), [setNodes, toast]);

  // Rebuild callbacks on load
  useEffect(() => {
    const cbs = buildCallbacks();
    setNodes(ns => ns.map(n => ({ ...n, data: { ...n.data, ...cbs } })));
  }, []);

  const addNode = useCallback((item) => {
    const id  = `n${counter}`;
    const cbs = buildCallbacks();
    setCounter(c => c + 1);
    setNodes(ns => [...ns, {
      id, type: "etlNode",
      position: { x: 80 + (counter % 4) * 230, y: 60 + Math.floor(counter / 4) * 180 },
      data: { label: item.label, type: item.type, config: null, columns: [], upstreamColumns: [], outputColumns: [], isConnected: false, ...cbs },
    }]);
  }, [counter, buildCallbacks]);

  const onConnect = useCallback(
    p => setEdges(es => addEdge({ ...p, animated: true, style: { stroke: C.blue, strokeWidth: 2 } }, es)),
    [setEdges]
  );

  const applyInputConfig = async () => {
    const ds = datasets.find(d => d.id === parseInt(inputConfig.datasetId));
    if (!ds) return;
    let dsWithCols = ds;
    if (!ds.columns || ds.columns.length === 0) {
      try { const r = await API.previewDataset(ds.id, 1); dsWithCols = { ...ds, columns: r.data.columns || [] }; } catch {}
    }
    const cbs = buildCallbacks();
    setNodes(ns => ns.map(n => n.id === configModal.nodeId ? {
      ...n, data: { ...n.data, config: { dataset: dsWithCols }, columns: dsWithCols.columns || [], outputColumns: dsWithCols.columns || [], ...cbs }
    } : n));
    setConfigModal(null);
    setInputConfig({ datasetId: "" });
    toast(`Dataset "${ds.name}" assigned`, "success");
  };

  const applyOutputConfig = () => {
    if (!outputConfig.outputName.trim()) return toast("Output name required", "error");
    const nodeId = outputSidebarNodeId;
    if (!nodeId) return;
    const cbs = buildCallbacks();
    setNodes(ns => ns.map(n => n.id === nodeId ? {
      ...n, data: { ...n.data, config: { ...outputConfig }, ...cbs }
    } : n));
    setOutputSidebarNodeId(null);
    toast("Output configured", "success");
  };

  const handleSave = useCallback(() => {
    const serialized = {
      ...workflow,
      nodes: nodes.map(n => ({
        id: n.id, type: n.type, position: n.position,
        data: { label: n.data.label, type: n.data.type, config: n.data.config, columns: n.data.columns },
      })),
      edges: edges.map(e => ({ id: e.id, source: e.source, target: e.target })),
      updatedAt: new Date().toISOString(),
    };
    onSave(serialized);
    toast("Workflow saved!", "success");
  }, [nodes, edges, workflow, onSave, toast]);

  // Build multi-branch tasks from output nodes
  const buildTasks = useCallback(() => {
    const outputNodes = nodes.filter(n => n.data.type === "output_dataset" && n.data.config?.outputName);
    const inputNode   = nodes.find(n => n.data.type === "input_dataset" && n.data.config?.dataset);
    if (!inputNode || !outputNodes.length) return [];

    return outputNodes.map((outNode, idx) => {
      const taskId    = outNode.data.config?.taskId || `task_${idx + 1}`;
      const transforms = buildTransformChain(outNode.id, nodes, edges).filter(t => t.type !== "output_dataset");
      // Find dependencies (other output nodes that feed into this one via intermediate nodes)
      const depends_on = [];
      return { task_id: taskId, output_name: outNode.data.config.outputName, transforms, depends_on };
    });
  }, [nodes, edges]);

  const handleRun = async () => {
    const inputNodes  = nodes.filter(n => n.data.type === "input_dataset");
    const outputNodes = nodes.filter(n => n.data.type === "output_dataset");
    if (!inputNodes.length)  return toast("Add an Input Dataset node first", "error");
    if (!outputNodes.length) return toast("Add an Output Dataset node first", "error");
    if (!inputNodes[0].data.config?.dataset)  return toast("Configure Input Dataset first", "error");
    const unconfigOut = outputNodes.find(n => !n.data.config?.outputName);
    if (unconfigOut) return toast("All Output Dataset nodes must be configured", "error");

    // Check all nodes are connected
    const utilNodes = nodes.filter(n => !["input_dataset","output_dataset"].includes(n.data.type));
    const disconnected = utilNodes.find(n => !edges.some(e => e.target === n.id));
    if (disconnected) return toast(`Node "${disconnected.data.label}" is not connected`, "error");

    const input = inputNodes[0].data.config.dataset;
    const tasks = buildTasks();
    const inputTable = input.table_name ? `staging.${input.table_name}` : `staging.${input.name.replace(/\.[^.]+$/, '').toLowerCase().replace(/\s+/g,'_')}`;

    setRunning(true);
    try {
      const r = await API.runPipeline({
        workflow_id:   workflow.id,
        workflow_name: workflow.name,
        input_table:   inputTable,
        tasks,
        description:   workflow.description || "",
      });
      const { run_id, dag_id } = r.data;
      toast(`Spark Pipeline triggered! DAG: ${dag_id}`, "success");
      setDagStatus({ dag_id, state: "queued", run_id });
      handleSave();

      pollRef.current = setInterval(async () => {
        try {
          const s = await API.getDagStatus(run_id);
          const { state, tasks: tStates } = s.data;
          setDagStatus(s.data);
          setTaskStates(tStates || {});
          if (["success","failed"].includes(state)) {
            clearInterval(pollRef.current);
            setRunning(false);
            toast(state === "success" ? "Pipeline completed! ✓" : "Pipeline failed", state === "success" ? "success" : "error");
            if (state === "success") handleSave();
          }
        } catch {}
      }, 6000);

    } catch (e) {
      setRunning(false);
      toast(e.response?.data?.detail || "Failed to trigger pipeline", "error");
    }
  };

  useEffect(() => () => { if (pollRef.current) clearInterval(pollRef.current); }, []);

  const STATUS_BG     = { success: C.greenTint, running: C.blueTint, failed: C.redTint, queued: C.goldTint, none: C.g50 };
  const STATUS_BORDER = { success: C.green, running: C.blue, failed: C.red, queued: C.gold, none: C.g300 };

  useEffect(() => {
    if (!Object.keys(taskStates).length) return;
    setNodes(ns => ns.map(n => {
      const state = taskStates[n.id] || "none";
      if (!STATUS_BG[state]) return n;
      return { ...n, style: { ...n.style, borderColor: STATUS_BORDER[state] } };
    }));
  }, [taskStates]);

  const utilNodeUpstreamCols = useMemo(() => {
    if (!utilNode) return [];
    return getUpstreamColumns(utilNode.id, nodes, edges);
  }, [utilNode, nodes, edges]);

  const outputNodeCount = nodes.filter(n => n.data.type === "output_dataset").length;

  return (
    <div style={{ display: "flex", flex: 1, overflow: "hidden", flexDirection: "column", fontFamily: "'DM Sans',sans-serif" }}>
      {/* Top bar */}
      <div style={{ background: C.white, borderBottom: `1px solid ${C.g200}`, padding: "0 20px", height: 52, display: "flex", alignItems: "center", justifyContent: "space-between", flexShrink: 0 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <button onClick={onBack} style={{ display: "flex", alignItems: "center", gap: 5, padding: "6px 12px", borderRadius: 7, border: `1px solid ${C.g200}`, background: C.g50, cursor: "pointer", fontSize: 12, fontWeight: 600, color: C.g600 }}>
            <Ic.Back /> Back
          </button>
          <div>
            <span style={{ fontWeight: 800, fontSize: 14, color: C.navy }}>{workflow.name}</span>
            <span style={{ fontSize: 11, color: C.g400, marginLeft: 8 }}>{nodes.length} nodes · {edges.length} edges</span>
            {outputNodeCount > 1 && <span style={{ fontSize: 11, color: C.spark, marginLeft: 8, fontWeight: 700 }}>⑂ {outputNodeCount} outputs</span>}
          </div>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {dagStatus && (
            <div style={{ padding: "4px 10px", borderRadius: 20, fontSize: 11, fontWeight: 700, background: STATUS_BG[dagStatus.state] || C.g100, color: STATUS_BORDER[dagStatus.state] || C.g500 }}>
              ⚡ DAG: {dagStatus.state?.toUpperCase()}
            </div>
          )}
          <button onClick={handleSave} style={{ display: "flex", alignItems: "center", gap: 5, padding: "7px 14px", borderRadius: 7, border: `1px solid ${C.g200}`, background: C.white, cursor: "pointer", fontSize: 12, fontWeight: 600, color: C.g600 }}>
            <Ic.Save /> Save
          </button>
          <button onClick={handleRun} disabled={running} style={{ display: "flex", alignItems: "center", gap: 5, padding: "7px 16px", borderRadius: 7, background: running ? C.g300 : `linear-gradient(135deg,${C.spark},${C.blue})`, border: "none", cursor: running ? "not-allowed" : "pointer", fontSize: 12, fontWeight: 700, color: C.white, boxShadow: running ? "none" : `0 2px 12px ${C.blue}55` }}>
            <Ic.Spark /> {running ? "Running…" : "Run Spark Pipeline"}
          </button>
        </div>
      </div>

      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        {/* Left panel */}
        <div style={{ width: 215, background: C.white, borderRight: `1px solid ${C.g200}`, display: "flex", flexDirection: "column", flexShrink: 0 }}>
          <div style={{ display: "flex", borderBottom: `1px solid ${C.g200}` }}>
            {Object.entries(NODE_PALETTE).map(([k, v]) => (
              <button key={k} onClick={() => setWfTab(k)} style={{ flex: 1, padding: "9px 0", fontSize: 11, fontWeight: 700, border: "none", cursor: "pointer", borderBottom: wfTab === k ? `2px solid ${v.color}` : "2px solid transparent", background: wfTab === k ? C.g50 : C.white, color: wfTab === k ? v.color : C.g400 }}>
                {v.label}
              </button>
            ))}
          </div>
          <div style={{ flex: 1, overflow: "auto", padding: "6px 0" }}>
            {NODE_PALETTE[wfTab].items.map(item => (
              <button key={item.type} onClick={() => addNode(item)}
                onMouseEnter={e => e.currentTarget.style.background = C.g50}
                onMouseLeave={e => e.currentTarget.style.background = "none"}
                style={{ width: "100%", display: "flex", alignItems: "center", gap: 8, padding: "8px 14px", background: "none", border: "none", cursor: "pointer", textAlign: "left" }}>
                <span style={{ width: 7, height: 7, borderRadius: 2, background: nodeBg(item.type), flexShrink: 0 }} />
                <span style={{ fontSize: 12, fontWeight: 500, color: C.g700 }}>{item.label}</span>
                <Ic.Plus />
              </button>
            ))}
          </div>
          {/* Spark info */}
          <div style={{ padding: "10px 12px", borderTop: `1px solid ${C.g200}`, background: C.sparkTint }}>
            <div style={{ fontSize: 10, fontWeight: 700, color: C.spark, marginBottom: 4, display: "flex", alignItems: "center", gap: 4 }}><Ic.Spark /> Spark Features</div>
            <div style={{ fontSize: 9, color: C.g500, lineHeight: 1.6 }}>
              Auto-size resources • Dynamic allocation • Parquet output • Multi-branch tasks
            </div>
          </div>
          <div style={{ padding: 10, borderTop: `1px solid ${C.g200}` }}>
            <button onClick={() => { setNodes([]); setEdges([]); setDagStatus(null); setTaskStates({}); }}
              style={{ width: "100%", padding: "6px 0", background: C.g50, border: `1px solid ${C.g200}`, borderRadius: 7, color: C.g500, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>
              Clear Canvas
            </button>
          </div>
        </div>

        {/* Canvas */}
        <div style={{ flex: 1, position: "relative", overflow: "hidden" }}>
          {nodes.length === 0 && (
            <div style={{ position: "absolute", inset: 0, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", pointerEvents: "none", zIndex: 5 }}>
              <div style={{ fontSize: 40, marginBottom: 12 }}>⚡</div>
              <div style={{ fontWeight: 800, color: C.g400, marginBottom: 4, fontSize: 16 }}>Canvas is empty</div>
              <div style={{ fontSize: 12, color: C.g300 }}>Add nodes from the left panel</div>
              <div style={{ fontSize: 11, color: C.g300, marginTop: 4 }}>Multiple Output nodes = Multi-branch pipeline</div>
            </div>
          )}
          <ReactFlow
            nodes={nodes} edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            nodeTypes={nodeTypes}
            fitView
            style={{ background: C.g50 }}
          >
            <Background color={C.g200} gap={20} />
            <Controls style={{ background: C.white, border: `1px solid ${C.g200}` }} />
            <MiniMap style={{ background: C.white, border: `1px solid ${C.g200}`, borderRadius: 8 }} nodeColor={n => nodeBg(n.data?.type)} />
            <Panel position="top-right">
              <div style={{ background: C.white, border: `1px solid ${C.g200}`, borderRadius: 8, padding: "4px 10px", fontSize: 11, color: C.g500, display: "flex", alignItems: "center", gap: 8 }}>
                <Ic.Spark />
                <span style={{ color: C.spark, fontWeight: 700 }}>DAG: {workflow.name}</span>
                <span style={{ color: C.g400 }}>{nodes.length}N · {edges.length}E · {outputNodeCount}Out</span>
              </div>
            </Panel>
          </ReactFlow>

          {/* Output Config Sidebar (right) */}
          {outputSidebarNodeId && (() => {
            const outNode = nodes.find(n => n.id === outputSidebarNodeId);
            const sourceEdge = edges.find(e => e.target === outputSidebarNodeId);
            const upCols = sourceEdge ? (columnMap[sourceEdge.source] || []) : [];
            const isConfigured = outNode?.data?.config?.outputName;
            return (
              <div style={{
                position: "absolute", right: 0, top: 0, bottom: 0, width: 320,
                background: C.white, borderLeft: `2px solid ${C.green}`,
                display: "flex", flexDirection: "column", zIndex: 50,
                boxShadow: "-6px 0 32px rgba(0,0,0,0.12)",
                fontFamily: "'DM Sans',sans-serif",
              }}>
                {/* Header */}
                <div style={{ background: `linear-gradient(135deg,${C.green},${C.navyLight})`, padding: "14px 16px", display: "flex", justifyContent: "space-between", alignItems: "center", flexShrink: 0 }}>
                  <div>
                    <div style={{ color: C.white, fontWeight: 800, fontSize: 14 }}>📤 Configure Output</div>
                    <div style={{ color: "rgba(255,255,255,0.65)", fontSize: 10, marginTop: 1 }}>
                      1 Output Node = 1 DAG Task
                    </div>
                  </div>
                  <button onClick={() => setOutputSidebarNodeId(null)} style={{ background: "rgba(255,255,255,0.15)", border: "none", borderRadius: 6, padding: "5px 9px", cursor: "pointer", color: C.white, fontSize: 16, fontWeight: 700 }}>×</button>
                </div>

                {/* DAG info banner */}
                <div style={{ background: C.sparkTint, borderBottom: `1px solid ${C.spark}22`, padding: "8px 16px" }}>
                  <div style={{ fontSize: 10, color: C.spark, fontWeight: 700 }}>⚡ DAG Name</div>
                  <div style={{ fontSize: 12, fontFamily: "monospace", color: C.navy, fontWeight: 700, marginTop: 2 }}>{
                    workflow.name.toLowerCase().replace(/[^a-z0-9_]/g,"_").replace(/_+/g,"_").replace(/^_|_$/g,"")
                  }</div>
                  <div style={{ fontSize: 9, color: C.g400, marginTop: 1 }}>Each output node = one task in this DAG</div>
                </div>

                {/* Form body */}
                <div style={{ flex: 1, overflow: "auto", padding: "16px" }}>
                  <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>

                    {/* Task ID */}
                    <div>
                      <div style={{ fontSize: 11, fontWeight: 700, color: C.g600, marginBottom: 5 }}>
                        Task ID <span style={{ fontSize: 9, color: C.g400, fontWeight: 400 }}>(unique identifier for this task)</span>
                      </div>
                      <input
                        value={outputConfig.taskId}
                        onChange={e => setOutputConfig(o => ({ ...o, taskId: e.target.value.toLowerCase().replace(/[^a-z0-9_]/g,"_") }))}
                        placeholder="e.g. clean_sales_data"
                        style={{ width: "100%", padding: "9px 11px", borderRadius: 8, border: `1.5px solid ${outputConfig.taskId ? C.blue : C.g200}`, fontSize: 12, boxSizing: "border-box", outline: "none", color: C.navy, fontFamily: "monospace" }}
                      />
                    </div>

                    {/* Output Table Name */}
                    <div>
                      <div style={{ fontSize: 11, fontWeight: 700, color: C.g600, marginBottom: 5 }}>
                        Output Table Name <span style={{ color: C.red }}>*</span>
                      </div>
                      <input
                        value={outputConfig.outputName}
                        onChange={e => setOutputConfig(o => ({ ...o, outputName: e.target.value.toLowerCase().replace(/\s+/g,"_").replace(/[^a-z0-9_]/g,"") }))}
                        placeholder="e.g. sales_clean_2026"
                        style={{ width: "100%", padding: "9px 11px", borderRadius: 8, border: `1.5px solid ${outputConfig.outputName ? C.green : C.g200}`, fontSize: 12, boxSizing: "border-box", outline: "none", color: C.navy, fontFamily: "monospace" }}
                      />
                      <div style={{ fontSize: 10, color: C.g400, marginTop: 3 }}>
                        → <span style={{ fontFamily: "monospace", color: C.green, fontWeight: 700 }}>warehouse.{outputConfig.outputName || "your_table"}</span>
                      </div>
                    </div>

                    {/* Description */}
                    <div>
                      <div style={{ fontSize: 11, fontWeight: 700, color: C.g600, marginBottom: 5 }}>Description</div>
                      <textarea
                        value={outputConfig.description}
                        onChange={e => setOutputConfig(o => ({ ...o, description: e.target.value }))}
                        placeholder="Describe what this output contains..."
                        rows={3}
                        style={{ width: "100%", padding: "9px 11px", borderRadius: 8, border: `1px solid ${C.g200}`, fontSize: 12, boxSizing: "border-box", outline: "none", color: C.g700, resize: "none" }}
                      />
                    </div>

                    {/* Upstream columns preview */}
                    {upCols.length > 0 && (
                      <div style={{ background: C.greenTint, borderRadius: 8, padding: 10 }}>
                        <div style={{ fontSize: 10, fontWeight: 700, color: C.green, marginBottom: 6 }}>
                          {upCols.length} columns will be saved to warehouse:
                        </div>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: 3, maxHeight: 80, overflowY: "auto" }}>
                          {upCols.map(c => (
                            <span key={c} style={{ fontSize: 9, padding: "2px 6px", borderRadius: 3, background: C.green+"22", color: C.green, fontWeight: 600, fontFamily: "monospace" }}>{c}</span>
                          ))}
                        </div>
                      </div>
                    )}
                    {!sourceEdge && (
                      <div style={{ background: C.goldTint, borderRadius: 8, padding: 10, fontSize: 11, color: C.gold }}>
                        ⚠ Connect this node to an upstream node first
                      </div>
                    )}

                    {/* Save As section — shown if already configured */}
                    {isConfigured && (
                      <div style={{ borderTop: `1px solid ${C.g200}`, paddingTop: 14 }}>
                        <div style={{ fontSize: 11, fontWeight: 700, color: C.g600, marginBottom: 8 }}>
                          ⬇ Save As (after pipeline runs)
                        </div>
                        <div style={{ display: "flex", gap: 8 }}>
                          <a
                            href={`/api/warehouse/${outNode.data.config.outputName}/download?format=csv`}
                            download
                            style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: `1.5px solid ${C.blue}`, background: C.blueTint, color: C.blue, fontWeight: 700, fontSize: 12, textDecoration: "none", display: "flex", alignItems: "center", justifyContent: "center", gap: 5 }}
                          >
                            <Ic.Download /> CSV
                          </a>
                          <a
                            href={`/api/warehouse/${outNode.data.config.outputName}/download?format=parquet`}
                            download
                            style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: `1.5px solid ${C.spark}`, background: C.sparkTint, color: C.spark, fontWeight: 700, fontSize: 12, textDecoration: "none", display: "flex", alignItems: "center", justifyContent: "center", gap: 5 }}
                          >
                            <Ic.Spark /> Parquet
                          </a>
                        </div>
                        <div style={{ fontSize: 9, color: C.g400, marginTop: 5, textAlign: "center" }}>
                          File will download from warehouse after pipeline completes
                        </div>
                      </div>
                    )}
                  </div>
                </div>

                {/* Footer buttons */}
                <div style={{ padding: "14px 16px", borderTop: `1px solid ${C.g200}`, background: C.g50, flexShrink: 0 }}>
                  <div style={{ display: "flex", gap: 8 }}>
                    <button
                      onClick={() => setOutputSidebarNodeId(null)}
                      style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: `1px solid ${C.g200}`, background: C.white, color: C.g600, fontSize: 12, fontWeight: 600, cursor: "pointer" }}
                    >
                      Cancel
                    </button>
                    <button
                      onClick={applyOutputConfig}
                      disabled={!outputConfig.outputName}
                      style={{ flex: 2, padding: "9px 0", borderRadius: 8, background: !outputConfig.outputName ? C.g300 : `linear-gradient(90deg,${C.green},#15803d)`, border: "none", color: C.white, fontSize: 12, fontWeight: 700, cursor: !outputConfig.outputName ? "not-allowed" : "pointer" }}
                    >
                      {isConfigured ? "✓ Update Config" : "Save Output Config"}
                    </button>
                  </div>
                </div>
              </div>
            );
          })()}

          {/* Preview panel */}
          {previewNode && !outputSidebarNodeId && (
            <NodePreviewPanel
              node={previewNode}
              datasets={datasets}
              edges={edges}
              nodes={nodes}
              onClose={() => setPreviewNode(null)}
            />
          )}
        </div>
      </div>

      {/* Input Dataset Config Modal */}
      {configModal?.type === "input_dataset" && (
        <Modal title="Configure Input Dataset" onClose={() => setConfigModal(null)}>
          <div style={{ marginBottom: 14 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: C.g600, marginBottom: 8 }}>Select Dataset</div>
            {datasets.length === 0 ? (
              <div style={{ background: C.redTint, borderRadius: 8, padding: 12, fontSize: 12, color: C.red }}>No datasets available. Upload one first.</div>
            ) : (
              <select value={inputConfig.datasetId} onChange={e => setInputConfig({ datasetId: e.target.value })}
                style={{ width: "100%", padding: "10px 12px", borderRadius: 8, border: `1px solid ${C.g200}`, fontSize: 13, color: C.g700, outline: "none" }}>
                <option value="">— Choose a dataset —</option>
                {datasets.map(d => <option key={d.id} value={d.id}>{d.name} ({d.row_count?.toLocaleString() || "?"} rows · {d.type}{d.is_large ? " · ⚡ Parquet" : ""})</option>)}
              </select>
            )}
          </div>
          {inputConfig.datasetId && (() => {
            const ds = datasets.find(d => d.id === parseInt(inputConfig.datasetId));
            return ds ? (
              <div style={{ background: C.blueTint, borderRadius: 8, padding: 12, marginBottom: 14 }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: C.blue, marginBottom: 4 }}>{ds.name}</div>
                <div style={{ display: "flex", gap: 12, fontSize: 11, color: C.blue, opacity: 0.8, marginBottom: ds.is_large ? 6 : 0 }}>
                  <span>{ds.row_count?.toLocaleString()} rows</span><span>{ds.file_size}</span><span>{ds.type}</span>
                </div>
                {ds.is_large && <div style={{ fontSize: 11, color: C.spark, fontWeight: 700 }}>⚡ Large dataset — will use Parquet + Spark</div>}
                {ds.columns?.length > 0 && (
                  <div style={{ marginTop: 8, display: "flex", flexWrap: "wrap", gap: 4 }}>
                    {ds.columns.map(c => <span key={c} style={{ fontSize: 10, padding: "2px 6px", borderRadius: 4, background: C.blue+"22", color: C.blue, fontWeight: 600, fontFamily: "monospace" }}>{c}</span>)}
                  </div>
                )}
              </div>
            ) : null;
          })()}
          <div style={{ display: "flex", gap: 8 }}>
            <button onClick={() => setConfigModal(null)} style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: `1px solid ${C.g200}`, background: C.white, color: C.g600, fontSize: 13, fontWeight: 600, cursor: "pointer" }}>Cancel</button>
            <button onClick={applyInputConfig} disabled={!inputConfig.datasetId} style={{ flex: 1, padding: "9px 0", borderRadius: 8, background: !inputConfig.datasetId ? C.g300 : C.blue, border: "none", color: C.white, fontSize: 13, fontWeight: 700, cursor: !inputConfig.datasetId ? "not-allowed" : "pointer" }}>Apply Dataset</button>
          </div>
        </Modal>
      )}

      {/* Utility Config Modal */}
      {utilNode && (
        <UtilityConfigModal
          node={utilNode}
          columns={utilNodeUpstreamCols}
          allNodes={nodes}
          onSave={(nodeId, cfg) => {
            const cbs = buildCallbacks();
            setNodes(ns => ns.map(n => n.id === nodeId ? { ...n, data: { ...n.data, config: cfg, ...cbs } } : n));
            setUtilNode(null);
            toast("Node configured!", "success");
          }}
          onClose={() => setUtilNode(null)}
        />
      )}
    </div>
  );
}

// ── Main App ──────────────────────────────────────────────────────────────────
export default function ETLPipelineApp() {
  const [page, setPage]         = useState("workflow");
  const [collapsed, setCollapsed] = useState(false);
  const [toasts, setToasts]     = useState([]);
  const [airflowOk, setAirflowOk] = useState(false);

  const [datasets, setDatasets]     = useState([]);
  const [dsLoading, setDsLoading]   = useState(false);
  const [dsTab, setDsTab]           = useState("files");
  const [searchQ, setSearchQ]       = useState("");
  const [filterType, setFilterType] = useState("all");
  const [selectedDS, setSelectedDS] = useState(null);
  const [preview, setPreview]       = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [showAddModal, setShowAddModal] = useState(false);
  const [addTab, setAddTab]         = useState("csv");
  const [uploadFile, setUploadFile] = useState(null);
  const [uploadName, setUploadName] = useState("");
  const [uploading, setUploading]   = useState(false);
  const [dbForm, setDbForm]         = useState({ host: "postgres", port: "5432", database: "airflow", username: "airflow", password: "airflow" });
  const [dbConnecting, setDbConnecting] = useState(false);

  const [pipelineRuns, setPipelineRuns] = useState([]);
  const [selectedRun, setSelectedRun]   = useState(null);
  const [runPreview, setRunPreview]     = useState(null);
  const [runPreviewLoading, setRunPreviewLoading] = useState(false);

  const [workflows, setWorkflows]       = useState([]);
  const [wfSearch, setWfSearch]         = useState("");
  const [wfFilter, setWfFilter]         = useState("all");
  const [activeWorkflow, setActiveWorkflow] = useState(null);
  const [showNewWfModal, setShowNewWfModal] = useState(false);
  const [newWfForm, setNewWfForm]       = useState({ name: "", description: "" });

  const [warehouseTables, setWarehouseTables] = useState([]);

  const toast = useCallback((msg, type = "info") => {
    const id = Date.now();
    setToasts(t => [...t, { id, msg, type }]);
    setTimeout(() => setToasts(t => t.filter(x => x.id !== id)), 3500);
  }, []);

  useEffect(() => {
    axios.get("/airflow-api/health").then(() => setAirflowOk(true)).catch(() => setAirflowOk(false));
    loadDatasets();
    loadWarehouse();
    setWorkflows(API.getWorkflows());
  }, []);

  const loadDatasets = async () => {
    setDsLoading(true);
    try {
      const r = await API.getDatasets();
      const data = Array.isArray(r.data) ? r.data : [];
      const enriched = await Promise.all(data.map(async ds => {
        if (ds.status === "deployed" && ds.table_name) {
          try { const p = await API.previewDataset(ds.id, 1); return { ...ds, columns: p.data.columns || [] }; }
          catch { return ds; }
        }
        return ds;
      }));
      setDatasets(enriched);
    } catch { toast("Backend not reachable", "error"); setDatasets([]); }
    finally { setDsLoading(false); }
  };

  const loadWarehouse = async () => {
    try {
      const [tablesRes, runsRes] = await Promise.all([API.getWarehouseTables(), API.getPipelineRuns()]);
      setWarehouseTables(Array.isArray(tablesRes.data) ? tablesRes.data : []);
      setPipelineRuns(Array.isArray(runsRes.data) ? runsRes.data : []);
    } catch {}
  };

  const loadPreview = async (ds) => {
    setSelectedDS(ds); setPreview(null); setPreviewLoading(true);
    try { const r = await API.previewDataset(ds.id, 50); setPreview(r.data); }
    catch { toast("Could not load preview", "error"); }
    finally { setPreviewLoading(false); }
  };

  const handleUpload = async () => {
    if (!uploadFile) return toast("Select a file first", "error");
    setUploading(true);
    try {
      const r = await API.uploadDataset(uploadFile, uploadName || uploadFile.name);
      const isLarge = r.data.is_large;
      toast(isLarge ? "Large dataset uploaded → converted to Parquet!" : "Dataset uploaded!", "success");
      setShowAddModal(false); setUploadFile(null); setUploadName("");
      await loadDatasets(); await loadWarehouse();
    } catch (e) { toast(e.response?.data?.detail || "Upload failed", "error"); }
    finally { setUploading(false); }
  };

  const handleConnectDB = async () => {
    setDbConnecting(true);
    try { await API.connectDB(dbForm); toast("Database connected!", "success"); setShowAddModal(false); await loadDatasets(); }
    catch (e) { toast(e.response?.data?.detail || "Connection failed", "error"); }
    finally { setDbConnecting(false); }
  };

  const handleDeleteDS = async (id, e) => {
    e.stopPropagation();
    try { await API.deleteDataset(id); toast("Dataset removed", "success"); if (selectedDS?.id === id) { setSelectedDS(null); setPreview(null); } await loadDatasets(); }
    catch { toast("Delete failed", "error"); }
  };

  const createWorkflow = () => {
    if (!newWfForm.name.trim()) return toast("Workflow name required", "error");
    const wf = {
      id: `wf_${Date.now()}`, name: newWfForm.name, description: newWfForm.description,
      nodes: [], edges: [], status: "draft",
      createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
    };
    API.saveWorkflow(wf);
    setWorkflows(API.getWorkflows());
    setNewWfForm({ name: "", description: "" });
    setShowNewWfModal(false);
    setActiveWorkflow(wf);
    toast("Workflow created!", "success");
  };

  const saveWorkflow = (wf) => {
    API.saveWorkflow(wf);
    setWorkflows(API.getWorkflows());
    setActiveWorkflow(wf);
  };

  const deleteWorkflow = (id) => {
    API.deleteWorkflow(id);
    setWorkflows(API.getWorkflows());
    toast("Workflow deleted", "success");
  };

  const filteredDS = datasets.filter(d => {
    const ms = d.name.toLowerCase().includes(searchQ.toLowerCase());
    const mt = filterType === "all" || d.type?.toLowerCase() === filterType;
    return ms && mt;
  });

  const filteredWF = workflows.filter(w => {
    const ms = w.name.toLowerCase().includes(wfSearch.toLowerCase());
    const mt = wfFilter === "all" || w.status === wfFilter;
    return ms && mt;
  });

  const STATUS_COLOR = { deployed: C.green, connected: C.blue, pending: C.gold, failed: C.red };
  const TYPE_COLOR   = { CSV: C.blue, EXCEL: C.gold, POSTGRESQL: C.navyLight, MYSQL: C.navyMid };

  const nav = [
    { id: "datasource",    label: "Data Source",    Icon: Ic.DB },
    { id: "workflow",      label: "Workflow",        Icon: Ic.Flow },
    { id: "visualization", label: "Visualization",   Icon: Ic.Chart },
  ];

  if (activeWorkflow && page === "workflow") {
    return (
      <div style={{ display: "flex", height: "100vh", fontFamily: "'DM Sans','Segoe UI',sans-serif", overflow: "hidden" }}>
        <style>{`*{box-sizing:border-box;margin:0;padding:0}`}</style>
        <WorkflowEditor workflow={activeWorkflow} datasets={datasets} onSave={saveWorkflow} onBack={() => setActiveWorkflow(null)} toast={toast} />
        <Toast toasts={toasts} />
      </div>
    );
  }

  return (
    <div style={{ display: "flex", height: "100vh", fontFamily: "'DM Sans','Segoe UI',sans-serif", background: C.off, overflow: "hidden" }}>
      <style>{`*{box-sizing:border-box;margin:0;padding:0} .hrow:hover{background:${C.blueTint}!important}`}</style>

      {/* SIDEBAR */}
      <aside style={{ width: collapsed ? 58 : 210, flexShrink: 0, background: `linear-gradient(180deg,${C.navy},${C.navyMid})`, display: "flex", flexDirection: "column", transition: "width .22s", boxShadow: "3px 0 20px rgba(0,0,0,0.22)", zIndex: 20 }}>
        <div style={{ padding: collapsed ? "18px 0" : "18px 16px", borderBottom: `1px solid ${C.navyLight}`, display: "flex", alignItems: "center", justifyContent: collapsed ? "center" : "space-between", minHeight: 60 }}>
          {!collapsed && <div><span style={{ color: C.white, fontWeight: 800, fontSize: 17 }}>ETL<span style={{ color: C.gold }}>Flow</span></span><div style={{ color: C.blueLight, fontSize: 9, marginTop: 1, letterSpacing: 1 }}>SPARK PIPELINE STUDIO</div></div>}
          <button onClick={() => setCollapsed(v => !v)} style={{ background: "none", border: "none", cursor: "pointer", color: C.blueLight, display: "flex", padding: 4, borderRadius: 5 }}><Ic.Menu /></button>
        </div>
        <nav style={{ flex: 1, padding: "10px 0" }}>
          {nav.map(({ id, label, Icon }) => {
            const active = page === id;
            return (
              <button key={id} onClick={() => setPage(id)} style={{ width: "100%", display: "flex", alignItems: "center", gap: 10, padding: collapsed ? "12px 0" : "11px 16px", justifyContent: collapsed ? "center" : "flex-start", background: active ? `linear-gradient(90deg,rgba(29,111,235,.28),transparent)` : "none", border: "none", borderLeft: active ? `3px solid ${C.gold}` : "3px solid transparent", cursor: "pointer", color: active ? C.white : C.blueLight }}>
                <span style={{ padding: 7, borderRadius: 8, background: active ? C.blue : "rgba(255,255,255,.06)", display: "flex" }}><Icon /></span>
                {!collapsed && <span style={{ fontSize: 13, fontWeight: active ? 700 : 500 }}>{label}</span>}
              </button>
            );
          })}
        </nav>
        <div style={{ padding: collapsed ? "14px 0" : "14px 16px", borderTop: `1px solid ${C.navyLight}`, display: "flex", justifyContent: collapsed ? "center" : "flex-start" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6, background: airflowOk ? "rgba(22,163,74,.15)" : "rgba(220,38,38,.15)", borderRadius: 20, padding: "4px 10px 4px 6px" }}>
            <span style={{ width: 7, height: 7, borderRadius: "50%", background: airflowOk ? C.green : C.red }} />
            {!collapsed && <span style={{ fontSize: 10, fontWeight: 700, color: airflowOk ? C.green : C.red }}>{airflowOk ? "AIRFLOW OK" : "AIRFLOW DOWN"}</span>}
          </div>
        </div>
      </aside>

      {/* MAIN */}
      <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
        <header style={{ background: C.white, borderBottom: `1px solid ${C.g200}`, padding: "0 26px", height: 54, display: "flex", alignItems: "center", justifyContent: "space-between", flexShrink: 0, boxShadow: "0 1px 3px rgba(0,0,0,.05)" }}>
          <div>
            <div style={{ fontWeight: 800, fontSize: 15, color: C.navy }}>{nav.find(n => n.id === page)?.label}</div>
            <div style={{ fontSize: 10, color: C.g400 }}>
              {page === "datasource" && "Upload & manage datasets · Files >10GB auto-convert to Parquet"}
              {page === "workflow"   && "Build multi-branch Spark ETL pipelines · 1 Workflow = 1 DAG · Multiple outputs supported"}
              {page === "visualization" && "Explore deployed data"}
            </div>
          </div>
          {page === "workflow" && (
            <button onClick={() => setShowNewWfModal(true)} style={{ display: "flex", alignItems: "center", gap: 6, padding: "7px 16px", borderRadius: 8, background: `linear-gradient(135deg,${C.spark},${C.blue})`, color: C.white, border: "none", fontWeight: 700, fontSize: 13, cursor: "pointer", boxShadow: `0 2px 8px ${C.blue}44` }}>
              <Ic.Plus /> New Workflow
            </button>
          )}
        </header>

        {/* ═══ DATA SOURCE PAGE ═══ */}
        {page === "datasource" && (
          <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
            <div style={{ width: 190, background: C.white, borderRight: `1px solid ${C.g200}`, padding: "14px 0", flexShrink: 0 }}>
              <div style={{ padding: "0 14px 8px", fontSize: 10, fontWeight: 700, color: C.g400, letterSpacing: 1.2, textTransform: "uppercase" }}>Input Sources</div>
              {[{ id: "files", label: "Files", subs: ["CSV", "Excel"] }, { id: "rdbms", label: "RDBMS", subs: ["PostgreSQL", "MySQL"] }].map(({ id, label, subs }) => (
                <div key={id}>
                  <button onClick={() => setDsTab(id)} style={{ width: "100%", display: "flex", alignItems: "center", gap: 8, padding: "9px 14px", background: dsTab === id ? C.blueTint : "none", border: "none", borderLeft: dsTab === id ? `3px solid ${C.blue}` : "3px solid transparent", cursor: "pointer", color: dsTab === id ? C.blue : C.g600 }}>
                    {id === "files" ? <Ic.Upload /> : <Ic.DB />}
                    <span style={{ fontSize: 13, fontWeight: dsTab === id ? 700 : 500 }}>{label}</span>
                  </button>
                  {subs.map(s => <div key={s} style={{ padding: "5px 14px 5px 38px", fontSize: 11, color: C.g400 }}>{s}</div>)}
                </div>
              ))}
            </div>

            <div style={{ flex: 1, padding: 22, overflow: "auto" }}>
              <div style={{ display: "flex", gap: 10, marginBottom: 18, flexWrap: "wrap" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 7, background: C.white, border: `1px solid ${C.g200}`, borderRadius: 8, padding: "7px 12px", flex: 1, minWidth: 200, maxWidth: 300 }}>
                  <Ic.Search />
                  <input value={searchQ} onChange={e => setSearchQ(e.target.value)} placeholder="Search datasets..." style={{ border: "none", outline: "none", fontSize: 13, color: C.g700, background: "transparent", width: "100%" }} />
                </div>
                <div style={{ display: "flex", gap: 5 }}>
                  {["all","csv","excel","postgresql"].map(t => (
                    <button key={t} onClick={() => setFilterType(t)} style={{ padding: "6px 10px", borderRadius: 6, fontSize: 11, fontWeight: 600, cursor: "pointer", border: filterType === t ? `1px solid ${C.blue}` : `1px solid ${C.g200}`, background: filterType === t ? C.blue : C.white, color: filterType === t ? C.white : C.g600 }}>{t === "all" ? "All" : t.toUpperCase()}</button>
                  ))}
                </div>
                <button onClick={loadDatasets} style={{ padding: "7px 10px", borderRadius: 7, border: `1px solid ${C.g200}`, background: C.white, cursor: "pointer", color: C.g500, display: "flex", alignItems: "center" }}><Ic.Refresh /></button>
                <button onClick={() => { setShowAddModal(true); setAddTab(dsTab === "files" ? "csv" : "postgres"); }} style={{ display: "flex", alignItems: "center", gap: 6, padding: "7px 14px", borderRadius: 8, background: C.blue, color: C.white, border: "none", fontWeight: 700, fontSize: 13, cursor: "pointer", boxShadow: `0 2px 8px ${C.blue}44` }}>
                  <Ic.Plus /> Add Source
                </button>
              </div>

              {dsLoading && <div style={{ textAlign: "center", color: C.g400, padding: 40 }}>Loading datasets…</div>}

              {!dsLoading && (
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(260px,1fr))", gap: 14 }}>
                  {filteredDS.map(ds => (
                    <div key={ds.id} onClick={() => loadPreview(ds)} style={{ background: C.white, borderRadius: 12, border: selectedDS?.id === ds.id ? `2px solid ${C.blue}` : `1px solid ${C.g200}`, padding: 16, cursor: "pointer", transition: "all .14s", boxShadow: selectedDS?.id === ds.id ? `0 0 0 3px ${C.blue}22` : "0 1px 4px rgba(0,0,0,.05)" }}>
                      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
                        <span style={{ padding: "3px 9px", borderRadius: 5, fontSize: 10, fontWeight: 700, background: `${TYPE_COLOR[ds.type] || C.g400}18`, color: TYPE_COLOR[ds.type] || C.g400 }}>{ds.type}</span>
                        <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
                          {ds.is_large && <span style={{ padding: "3px 6px", borderRadius: 5, fontSize: 9, fontWeight: 700, background: C.sparkTint, color: C.spark }}>⚡ Parquet</span>}
                          <span style={{ padding: "3px 9px", borderRadius: 20, fontSize: 10, fontWeight: 700, background: `${STATUS_COLOR[ds.status] || C.g400}18`, color: STATUS_COLOR[ds.status] || C.g400 }}>{ds.status}</span>
                        </div>
                      </div>
                      <div style={{ fontWeight: 700, fontSize: 14, color: C.navy, marginBottom: 3, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{ds.name}</div>
                      <div style={{ display: "flex", gap: 10, fontSize: 11, color: C.g400, marginBottom: 10 }}>
                        {ds.row_count && <span>{ds.row_count.toLocaleString()} rows</span>}
                        {ds.col_count && <span>{ds.col_count} cols</span>}
                        {ds.file_size && <span>{ds.file_size}</span>}
                      </div>
                      <div style={{ display: "flex", gap: 6 }}>
                        <button onClick={e => { e.stopPropagation(); loadPreview(ds); }} style={{ flex: 1, padding: "5px 0", borderRadius: 6, border: `1px solid ${C.g200}`, background: C.g50, color: C.g600, fontSize: 11, fontWeight: 600, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 4 }}><Ic.Eye /> Preview</button>
                        <a href={`/api/datasets/${ds.id}/download?format=csv`} download onClick={e => e.stopPropagation()} style={{ padding: "5px 8px", borderRadius: 6, border: `1px solid ${C.blue}33`, background: C.blueTint, color: C.blue, display: "flex", alignItems: "center" }} title="Download CSV"><Ic.Download /></a>
                        {ds.is_large && <a href={`/api/datasets/${ds.id}/download?format=parquet`} download onClick={e => e.stopPropagation()} style={{ padding: "5px 8px", borderRadius: 6, border: `1px solid ${C.spark}33`, background: C.sparkTint, color: C.spark, display: "flex", alignItems: "center" }} title="Download Parquet"><Ic.Spark /></a>}
                        <button onClick={e => handleDeleteDS(ds.id, e)} style={{ padding: "5px 8px", borderRadius: 6, border: `1px solid ${C.redTint}`, background: C.redTint, color: C.red, cursor: "pointer", display: "flex", alignItems: "center" }}><Ic.Trash /></button>
                      </div>
                    </div>
                  ))}
                  {filteredDS.length === 0 && !dsLoading && (
                    <div style={{ gridColumn: "1/-1", textAlign: "center", color: C.g400, padding: 48 }}>
                      <div style={{ fontSize: 28, marginBottom: 8 }}>📂</div>
                      No datasets found. Click "Add Source" to upload one.
                    </div>
                  )}
                </div>
              )}

              {selectedDS && (
                <div style={{ marginTop: 20, background: C.white, borderRadius: 12, border: `1px solid ${C.g200}`, overflow: "hidden", boxShadow: "0 2px 12px rgba(0,0,0,.06)" }}>
                  <div style={{ padding: "12px 18px", background: C.navy, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <span style={{ color: C.white, fontWeight: 700, fontSize: 13 }}>📋 {selectedDS.name}</span>
                    <div style={{ display: "flex", gap: 8 }}>
                      <a href={`/api/datasets/${selectedDS.id}/download?format=csv`} download style={{ padding: "4px 10px", borderRadius: 5, background: "rgba(255,255,255,0.15)", border: "1px solid rgba(255,255,255,0.2)", color: C.white, fontSize: 11, fontWeight: 700, textDecoration: "none", display: "flex", alignItems: "center", gap: 4 }}><Ic.Download /> CSV</a>
                      {selectedDS.is_large && <a href={`/api/datasets/${selectedDS.id}/download?format=parquet`} download style={{ padding: "4px 10px", borderRadius: 5, background: "rgba(226,88,34,0.3)", border: "1px solid rgba(226,88,34,0.4)", color: C.white, fontSize: 11, fontWeight: 700, textDecoration: "none", display: "flex", alignItems: "center", gap: 4 }}><Ic.Spark /> Parquet</a>}
                      <button onClick={() => { setSelectedDS(null); setPreview(null); }} style={{ background: "rgba(255,255,255,.1)", border: "none", borderRadius: 5, padding: "3px 8px", cursor: "pointer", color: C.white }}><Ic.X /></button>
                    </div>
                  </div>
                  {previewLoading && <div style={{ padding: 32, textAlign: "center", color: C.g400 }}>Loading preview…</div>}
                  {preview && !previewLoading && (
                    <div style={{ overflowX: "auto", maxHeight: 340 }}>
                      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                        <thead><tr style={{ background: C.g50 }}>
                          {preview.columns.map(c => <th key={c} style={{ padding: "8px 12px", textAlign: "left", fontWeight: 700, color: C.g600, borderBottom: `1px solid ${C.g200}`, whiteSpace: "nowrap" }}>{c}</th>)}
                        </tr></thead>
                        <tbody>
                          {preview.rows.slice(0,50).map((row, i) => (
                            <tr key={i} className="hrow" style={{ background: i%2===0?C.white:C.g50 }}>
                              {preview.columns.map(c => <td key={c} style={{ padding: "6px 12px", borderBottom: `1px solid ${C.g100}`, color: C.g700, whiteSpace: "nowrap", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis" }}>{row[c]==null?<span style={{color:C.g300}}>null</span>:String(row[c])}</td>)}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        )}

        {/* ═══ WORKFLOW PAGE ═══ */}
        {page === "workflow" && (
          <div style={{ flex: 1, padding: 24, overflow: "auto" }}>
            <div style={{ display: "flex", gap: 10, marginBottom: 20, flexWrap: "wrap" }}>
              <div style={{ display: "flex", alignItems: "center", gap: 7, background: C.white, border: `1px solid ${C.g200}`, borderRadius: 8, padding: "7px 12px", flex: 1, minWidth: 200, maxWidth: 320 }}>
                <Ic.Search />
                <input value={wfSearch} onChange={e => setWfSearch(e.target.value)} placeholder="Search workflows..." style={{ border: "none", outline: "none", fontSize: 13, color: C.g700, background: "transparent", width: "100%" }} />
              </div>
              <div style={{ display: "flex", gap: 5 }}>
                {["all","draft","running","success","failed"].map(s => (
                  <button key={s} onClick={() => setWfFilter(s)} style={{ padding: "6px 10px", borderRadius: 6, fontSize: 11, fontWeight: 600, cursor: "pointer", border: wfFilter === s ? `1px solid ${C.blue}` : `1px solid ${C.g200}`, background: wfFilter === s ? C.blue : C.white, color: wfFilter === s ? C.white : C.g600 }}>{s.charAt(0).toUpperCase()+s.slice(1)}</button>
                ))}
              </div>
            </div>

            {filteredWF.length === 0 ? (
              <div style={{ textAlign: "center", padding: 60, color: C.g400 }}>
                <div style={{ fontSize: 40, marginBottom: 12 }}>⚡</div>
                <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 4, color: C.navy }}>No workflows yet</div>
                <div style={{ fontSize: 12, marginBottom: 6 }}>Create your first Spark ETL pipeline</div>
                <div style={{ fontSize: 11, color: C.g300, marginBottom: 20 }}>1 Workflow = 1 DAG · Multiple Output nodes = Multi-branch pipeline</div>
                <button onClick={() => setShowNewWfModal(true)} style={{ padding: "10px 24px", borderRadius: 8, background: `linear-gradient(135deg,${C.spark},${C.blue})`, border: "none", color: C.white, fontWeight: 700, fontSize: 13, cursor: "pointer" }}>+ Create Workflow</button>
              </div>
            ) : (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(300px,1fr))", gap: 16 }}>
                {filteredWF.map(wf => {
                  const wfSC = { draft: C.g400, running: C.blue, success: C.green, failed: C.red }[wf.status] || C.g400;
                  const wfSB = { draft: C.g100, running: C.blueTint, success: C.greenTint, failed: C.redTint }[wf.status] || C.g100;
                  const outputCount = (wf.nodes || []).filter(n => n.data?.type === "output_dataset").length;
                  return (
                    <div key={wf.id} style={{ background: C.white, borderRadius: 14, border: `1px solid ${C.g200}`, overflow: "hidden", boxShadow: "0 2px 10px rgba(0,0,0,.05)" }}>
                      <div style={{ height: 4, background: `linear-gradient(90deg,${C.spark},${C.blue},${C.gold})` }} />
                      <div style={{ padding: 16 }}>
                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 8 }}>
                          <div style={{ fontWeight: 800, fontSize: 14, color: C.navy, flex: 1, marginRight: 8 }}>{wf.name}</div>
                          <span style={{ padding: "3px 9px", borderRadius: 20, fontSize: 10, fontWeight: 700, background: wfSB, color: wfSC, flexShrink: 0 }}>{wf.status}</span>
                        </div>
                        {wf.description && <div style={{ fontSize: 12, color: C.g500, marginBottom: 8, lineHeight: 1.5 }}>{wf.description}</div>}
                        <div style={{ display: "flex", gap: 10, fontSize: 11, color: C.g400, marginBottom: 12 }}>
                          <span>{(wf.nodes || []).length} nodes</span>
                          {outputCount > 0 && <span style={{ color: outputCount > 1 ? C.spark : C.g400 }}>⑂ {outputCount} output{outputCount > 1 ? "s" : ""}</span>}
                          <span>{wf.updatedAt?.slice(0,10)}</span>
                        </div>
                        <div style={{ display: "flex", gap: 8 }}>
                          <button onClick={() => setActiveWorkflow(wf)} style={{ flex: 1, padding: "7px 0", borderRadius: 8, background: `linear-gradient(135deg,${C.spark},${C.blue})`, border: "none", color: C.white, fontWeight: 700, fontSize: 12, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 5 }}>
                            <Ic.Edit /> Open Editor
                          </button>
                          <button onClick={() => deleteWorkflow(wf.id)} style={{ padding: "7px 10px", borderRadius: 8, border: `1px solid ${C.redTint}`, background: C.redTint, color: C.red, cursor: "pointer", display: "flex", alignItems: "center" }}><Ic.Trash /></button>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        )}

        {/* ═══ VISUALIZATION PAGE ═══ */}
        {page === "visualization" && (
          <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
            <div style={{ width: 290, background: C.white, borderRight: `1px solid ${C.g200}`, display: "flex", flexDirection: "column", flexShrink: 0 }}>
              <div style={{ padding: "14px 16px", borderBottom: `1px solid ${C.g200}`, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <div style={{ fontWeight: 700, fontSize: 13, color: C.navy }}>Pipeline Outputs</div>
                <button onClick={loadWarehouse} style={{ background: "none", border: "none", cursor: "pointer", color: C.g400, display: "flex" }}><Ic.Refresh /></button>
              </div>
              <div style={{ flex: 1, overflow: "auto" }}>
                {pipelineRuns.length === 0 ? (
                  <div style={{ padding: 24, textAlign: "center", color: C.g400 }}>
                    <div style={{ fontSize: 24, marginBottom: 8 }}>📭</div>
                    <div style={{ fontSize: 12 }}>No pipeline runs yet</div>
                  </div>
                ) : pipelineRuns.map(run => {
                  const active = selectedRun?.id === run.id;
                  const tname  = run.output_table?.replace('warehouse.', '') || '';
                  const statusC = { success: C.green, failed: C.red, running: C.blue, pending: C.gold }[run.status] || C.g400;
                  return (
                    <div key={run.id} onClick={async () => {
                      setSelectedRun(run); setRunPreview(null); setRunPreviewLoading(true);
                      try { const r = await API.previewRun(run.id); setRunPreview(r.data); } catch {}
                      finally { setRunPreviewLoading(false); }
                    }} style={{ padding: "12px 16px", cursor: "pointer", background: active ? C.blueTint : C.white, borderLeft: active ? `3px solid ${C.blue}` : "3px solid transparent", borderBottom: `1px solid ${C.g100}` }}>
                      <div style={{ fontWeight: 700, fontSize: 13, color: active ? C.blue : C.navy, marginBottom: 2, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{tname}</div>
                      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10 }}>
                        <span style={{ color: C.g400 }}>{run.row_count?.toLocaleString() || "?"} rows</span>
                        <span style={{ color: statusC, fontWeight: 700 }}>{run.status}</span>
                      </div>
                      {run.task_id && <div style={{ fontSize: 9, color: C.g400, marginTop: 1 }}>{run.task_id}</div>}
                      <div style={{ fontSize: 9, color: C.g300, marginTop: 1 }}>{run.ran_at?.slice(0,16).replace('T',' ')}</div>
                    </div>
                  );
                })}
              </div>
            </div>

            <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
              {!selectedRun ? (
                <div style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", color: C.g400 }}>
                  <div style={{ fontSize: 36, marginBottom: 12 }}>📊</div>
                  <div style={{ fontWeight: 700, fontSize: 15 }}>Select a pipeline output</div>
                </div>
              ) : (
                <>
                  <div style={{ padding: "14px 22px", borderBottom: `1px solid ${C.g200}`, background: C.white, display: "flex", justifyContent: "space-between", alignItems: "center", flexShrink: 0 }}>
                    <div>
                      <div style={{ fontWeight: 800, fontSize: 15, color: C.navy }}>{selectedRun.output_table}</div>
                      <div style={{ fontSize: 11, color: C.g400 }}>{selectedRun.row_count?.toLocaleString()} rows · {runPreview?.columns?.length || 0} cols · {selectedRun.ran_at?.slice(0,16).replace("T"," ")}</div>
                    </div>
                    <div style={{ display: "flex", gap: 8 }}>
                      {selectedRun.status === "success" && (
                        <>
                          <a href={API.downloadRun(selectedRun.id, "csv")} download style={{ display: "flex", alignItems: "center", gap: 5, padding: "7px 12px", borderRadius: 7, background: C.blueTint, border: `1px solid ${C.blue}33`, color: C.blue, fontWeight: 700, fontSize: 12, textDecoration: "none" }}><Ic.Download /> CSV</a>
                          <a href={API.downloadRun(selectedRun.id, "parquet")} download style={{ display: "flex", alignItems: "center", gap: 5, padding: "7px 12px", borderRadius: 7, background: C.sparkTint, border: `1px solid ${C.spark}33`, color: C.spark, fontWeight: 700, fontSize: 12, textDecoration: "none" }}><Ic.Spark /> Parquet</a>
                        </>
                      )}
                      <a href="http://localhost:3000" target="_blank" rel="noopener noreferrer" style={{ display: "flex", alignItems: "center", gap: 5, padding: "7px 14px", borderRadius: 7, background: `linear-gradient(90deg,${C.blue},${C.blueMid})`, color: C.white, fontWeight: 700, fontSize: 12, textDecoration: "none" }}>
                        <Ic.Link /> Metabase
                      </a>
                    </div>
                  </div>

                  {runPreview && <div style={{ padding: "8px 22px", borderBottom: `1px solid ${C.g100}`, background: C.g50, display: "flex", flexWrap: "wrap", gap: 4, flexShrink: 0 }}>
                    {runPreview.columns.map(c => <span key={c} style={{ fontSize: 10, padding: "2px 7px", borderRadius: 4, background: C.blueTint2, color: C.blue, fontWeight: 600, fontFamily: "monospace" }}>{c}</span>)}
                  </div>}

                  <div style={{ flex: 1, overflow: "auto" }}>
                    {runPreviewLoading && <div style={{ padding: 40, textAlign: "center", color: C.g400 }}>Loading…</div>}
                    {runPreview && !runPreviewLoading && (
                      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                        <thead><tr style={{ background: C.g50, position: "sticky", top: 0, zIndex: 1 }}>
                          {runPreview.columns.map(c => <th key={c} style={{ padding: "8px 14px", textAlign: "left", fontWeight: 700, color: C.g600, borderBottom: `2px solid ${C.g200}`, whiteSpace: "nowrap" }}>{c}</th>)}
                        </tr></thead>
                        <tbody>
                          {runPreview.rows.map((row, i) => (
                            <tr key={i} className="hrow" style={{ background: i%2===0?C.white:C.g50 }}>
                              {runPreview.columns.map(c => <td key={c} style={{ padding: "6px 14px", borderBottom: `1px solid ${C.g100}`, color: C.g700, whiteSpace: "nowrap", maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis" }}>
                                {row[c]==null?<span style={{color:C.g300,fontStyle:"italic"}}>null</span>:String(row[c])}
                              </td>)}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </div>
                  <div style={{ padding: "8px 22px", borderTop: `1px solid ${C.g200}`, background: C.g50, fontSize: 11, color: C.g400, flexShrink: 0, display: "flex", justifyContent: "space-between" }}>
                    <span>Preview: {runPreview?.rows?.length || 0} rows shown</span>
                    <span>Table: <code style={{ fontFamily: "monospace", color: C.blue }}>{selectedRun.output_table}</code></span>
                  </div>
                </>
              )}
            </div>
          </div>
        )}
      </div>

      {/* ADD SOURCE MODAL */}
      {showAddModal && (
        <Modal title="Add Data Source" onClose={() => setShowAddModal(false)}>
          <div style={{ display: "flex", borderBottom: `1px solid ${C.g200}`, marginBottom: 20 }}>
            {[{id:"csv",l:"CSV"},{id:"excel",l:"Excel"},{id:"postgres",l:"PostgreSQL"},{id:"mysql",l:"MySQL"}].map(({id,l}) => (
              <button key={id} onClick={() => setAddTab(id)} style={{ flex: 1, padding: "10px 0", fontSize: 11, fontWeight: 700, border: "none", cursor: "pointer", borderBottom: addTab===id?`2px solid ${C.blue}`:"2px solid transparent", background: "none", color: addTab===id?C.blue:C.g400 }}>{l}</button>
            ))}
          </div>
          {(addTab==="csv"||addTab==="excel") && (
            <div>
              <label style={{ display: "block", border: `2px dashed ${uploadFile?C.blue:C.g300}`, borderRadius: 10, padding: "24px 20px", textAlign: "center", background: uploadFile?C.blueTint:C.g50, cursor: "pointer", marginBottom: 12 }}>
                <input type="file" accept={addTab==="csv"?".csv":".xlsx,.xls"} onChange={e=>{setUploadFile(e.target.files[0]);setUploadName(e.target.files[0]?.name||"");}} style={{display:"none"}}/>
                <div style={{fontSize:26,marginBottom:6}}>{uploadFile?"✅":"📁"}</div>
                <div style={{fontWeight:700,color:uploadFile?C.blue:C.g600,fontSize:13}}>{uploadFile?uploadFile.name:`Drop your ${addTab.toUpperCase()} file here`}</div>
                <div style={{fontSize:11,color:C.g400,marginTop:3}}>{uploadFile?`${(uploadFile.size/1024).toFixed(1)} KB`:"or click to browse"}</div>
              </label>
              <div style={{ background: C.sparkTint, borderRadius: 7, padding: "8px 12px", fontSize: 11, color: C.spark, marginBottom: 12 }}>
                ⚡ Files larger than 10GB will be automatically converted to Parquet format
              </div>
              <input value={uploadName} onChange={e=>setUploadName(e.target.value)} placeholder="Dataset name (optional)" style={{width:"100%",padding:"9px 12px",borderRadius:8,border:`1px solid ${C.g200}`,fontSize:13,boxSizing:"border-box",outline:"none",color:C.g700}}/>
            </div>
          )}
          {(addTab==="postgres"||addTab==="mysql") && (
            <div style={{display:"flex",flexDirection:"column",gap:10}}>
              {[{k:"host",l:"Host",ph:addTab==="postgres"?"postgres":"localhost"},{k:"port",l:"Port",ph:addTab==="postgres"?"5432":"3306"},{k:"database",l:"Database",ph:"airflow"},{k:"username",l:"Username",ph:addTab==="postgres"?"airflow":"root"},{k:"password",l:"Password",ph:"••••••••",t:"password"}].map(({k,l,ph,t})=>(
                <div key={k}>
                  <div style={{fontSize:11,fontWeight:700,color:C.g600,marginBottom:4}}>{l}</div>
                  <input type={t||"text"} value={dbForm[k]} onChange={e=>setDbForm(f=>({...f,[k]:e.target.value}))} placeholder={ph} style={{width:"100%",padding:"8px 11px",borderRadius:7,border:`1px solid ${C.g200}`,fontSize:13,boxSizing:"border-box",outline:"none",color:C.g700}}/>
                </div>
              ))}
            </div>
          )}
          <div style={{display:"flex",gap:8,marginTop:18}}>
            <button onClick={()=>setShowAddModal(false)} style={{flex:1,padding:"9px 0",borderRadius:8,border:`1px solid ${C.g200}`,background:C.white,color:C.g600,fontSize:13,fontWeight:600,cursor:"pointer"}}>Cancel</button>
            <button onClick={addTab==="csv"||addTab==="excel"?handleUpload:handleConnectDB} disabled={uploading||dbConnecting} style={{flex:1,padding:"9px 0",borderRadius:8,background:uploading||dbConnecting?C.g300:`linear-gradient(90deg,${C.blue},${C.blueMid})`,border:"none",color:C.white,fontSize:13,fontWeight:700,cursor:uploading||dbConnecting?"not-allowed":"pointer"}}>
              {uploading||dbConnecting?"Please wait…":addTab==="csv"||addTab==="excel"?"Upload & Deploy":"Test & Connect"}
            </button>
          </div>
        </Modal>
      )}

      {/* NEW WORKFLOW MODAL */}
      {showNewWfModal && (
        <Modal title="Create New Workflow" onClose={() => setShowNewWfModal(false)}>
          <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
            <div>
              <div style={{ fontSize: 12, fontWeight: 700, color: C.g600, marginBottom: 6 }}>Workflow Name * <span style={{ fontSize: 10, color: C.g400, fontWeight: 400 }}>(= Pipeline / DAG name)</span></div>
              <input value={newWfForm.name} onChange={e => setNewWfForm(f => ({ ...f, name: e.target.value }))} placeholder="e.g. Sales ETL Pipeline" style={{ width: "100%", padding: "10px 12px", borderRadius: 8, border: `1.5px solid ${newWfForm.name ? C.blue : C.g200}`, fontSize: 13, boxSizing: "border-box", outline: "none", color: C.g700 }} />
            </div>
            <div>
              <div style={{ fontSize: 12, fontWeight: 700, color: C.g600, marginBottom: 6 }}>Description</div>
              <textarea value={newWfForm.description} onChange={e => setNewWfForm(f => ({ ...f, description: e.target.value }))} placeholder="Describe this pipeline..." rows={2} style={{ width: "100%", padding: "10px 12px", borderRadius: 8, border: `1px solid ${C.g200}`, fontSize: 13, boxSizing: "border-box", outline: "none", color: C.g700, resize: "none" }} />
            </div>
            <div style={{ background: C.sparkTint, borderRadius: 8, padding: 12, fontSize: 11, color: C.spark, lineHeight: 1.6 }}>
              <div style={{ fontWeight: 700, marginBottom: 4 }}>⚡ Spark Features Enabled</div>
              Auto resource sizing · Dynamic allocation · Parquet output · Multi-branch tasks (add multiple Output Dataset nodes)
            </div>
          </div>
          <div style={{ display: "flex", gap: 8, marginTop: 20 }}>
            <button onClick={() => setShowNewWfModal(false)} style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: `1px solid ${C.g200}`, background: C.white, color: C.g600, fontSize: 13, fontWeight: 600, cursor: "pointer" }}>Cancel</button>
            <button onClick={createWorkflow} disabled={!newWfForm.name.trim()} style={{ flex: 1, padding: "9px 0", borderRadius: 8, background: !newWfForm.name.trim() ? C.g300 : `linear-gradient(135deg,${C.spark},${C.blue})`, border: "none", color: C.white, fontSize: 13, fontWeight: 700, cursor: !newWfForm.name.trim() ? "not-allowed" : "pointer" }}>
              Create & Open Editor
            </button>
          </div>
        </Modal>
      )}

      <Toast toasts={toasts} />
    </div>
  );
}