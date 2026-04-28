import { useState, useCallback, useEffect, useMemo } from "react";
import { UtilityConfigModal } from "./UtilityConfigs";
import {
  ReactFlow, Background, Controls, MiniMap, addEdge,
  useNodesState, useEdgesState, Panel, Handle, Position,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import axios from "axios";

// ─── API ──────────────────────────────────────────────────────────────────────
const api = axios.create({ baseURL: "/api" });
const airflow = axios.create({
  baseURL: "/airflow-api/api/v1",
  headers: { Authorization: "Basic " + btoa("admin:admin123") },
});

const API = {
  // Datasets
  getDatasets:    ()      => api.get("/datasets").catch(() => ({ data: [] })),
  deleteDataset:  (id)    => api.delete(`/datasets/${id}`),
  previewDataset: (id, n=5) => api.get(`/datasets/${id}/preview?limit=${n}`),
  uploadDataset:  (file, name) => {
    const f = new FormData(); f.append("file", file);
    if (name) f.append("name", name);
    return api.post("/datasets/upload", f);
  },
  connectDB: (p) => api.post("/datasets/connect-db", p),
  // Workflows (stored in localStorage for now, backend optional)
  getWorkflows:    ()  => { try { return JSON.parse(localStorage.getItem("etl_workflows") || "[]"); } catch { return []; } },
  saveWorkflow:    (w) => { const ws = API.getWorkflows().filter(x => x.id !== w.id); ws.push(w); localStorage.setItem("etl_workflows", JSON.stringify(ws)); return w; },
  deleteWorkflow:  (id) => { const ws = API.getWorkflows().filter(x => x.id !== id); localStorage.setItem("etl_workflows", JSON.stringify(ws)); },
  // Airflow
  getAirflowStatus: () => axios.get("/airflow-api/health").then(() => ({ data: { connected: true } })).catch(() => ({ data: { connected: false } })),
  getDagRuns:      (d)      => airflow.get(`/dags/${d}/dagRuns?limit=5&order_by=-execution_date`),
  triggerDag:      (d, cfg) => airflow.post(`/dags/${d}/dagRuns`, { conf: cfg || {} }),
  getTaskInstances:(d, r)   => airflow.get(`/dags/${d}/dagRuns/${r}/taskInstances`),
  getWarehouseTables: ()    => api.get("/warehouse/tables").catch(() => ({ data: [] })),
  runNewPipeline:    (payload) => api.post("/pipelines/run", payload),
  getPipelineRuns:   ()        => api.get("/pipelines/runs").catch(() => ({ data: [] })),
  previewPipelineRun:(id)      => api.get(`/pipelines/runs/${id}/preview`),
  getDagStatus:      (id)      => api.get(`/pipelines/runs/${id}/dag-status`),
  updatePipelineRun: (id, data)=> api.patch(`/pipelines/runs/${id}`, data),
};

// ─── Colors ───────────────────────────────────────────────────────────────────
const C = {
  navy:"#0B1E3D", navyMid:"#122850", navyLight:"#1A3A6B",
  blue:"#1D6FEB", blueMid:"#3B82F6", blueLight:"#93C5FD",
  blueTint:"#EFF6FF", blueTint2:"#DBEAFE",
  gold:"#F59E0B", goldTint:"#FFFBEB",
  white:"#FFFFFF", off:"#F8FAFC",
  g50:"#F8FAFC", g100:"#F1F5F9", g200:"#E2E8F0",
  g300:"#CBD5E1", g400:"#94A3B8", g500:"#64748B", g600:"#475569", g700:"#334155",
  green:"#16A34A", greenTint:"#DCFCE7",
  red:"#DC2626", redTint:"#FEE2E2",
  orange:"#EA580C",
};

// ─── Icons ────────────────────────────────────────────────────────────────────
const Ic = {
  DB:      () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="18" height="18"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>,
  Chart:   () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="18" height="18"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M8 17l3-4 3 3 3-5"/></svg>,
  Flow:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="18" height="18"><rect x="3" y="3" width="6" height="6" rx="1"/><rect x="15" y="3" width="6" height="6" rx="1"/><rect x="9" y="15" width="6" height="6" rx="1"/><path d="M6 9v3a3 3 0 003 3h6a3 3 0 003-3V9"/></svg>,
  Upload:  () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>,
  Plus:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" width="15" height="15"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>,
  Search:  () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="15" height="15"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>,
  Trash:   () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6M14 11v6M9 6V4h6v2"/></svg>,
  Copy:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>,
  Link:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>,
  Play:    () => <svg viewBox="0 0 24 24" fill="currentColor" width="13" height="13"><polygon points="5 3 19 12 5 21 5 3"/></svg>,
  X:       () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>,
  Eye:     () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>,
  Refresh: () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 11-2.12-9.36L23 10"/></svg>,
  Edit:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>,
  Warn:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>,
  Menu:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="18" height="18"><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="18" x2="21" y2="18"/></svg>,
  Table:   () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M3 9h18M3 15h18M9 3v18"/></svg>,
  ArrowRight: () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><line x1="5" y1="12" x2="19" y2="12"/><polyline points="12 5 19 12 12 19"/></svg>,
  Save:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><path d="M19 21H5a2 2 0 01-2-2V5a2 2 0 012-2h11l5 5v11a2 2 0 01-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>,
  Back:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16"><line x1="19" y1="12" x2="5" y2="12"/><polyline points="12 19 5 12 12 5"/></svg>,
  InData:  () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>,
  OutData: () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>,
};

// ─── Node Palette ─────────────────────────────────────────────────────────────
const NODE_PALETTE = {
  datasets: {
    label: "Datasets", color: C.blue,
    items: [
      { label: "Input Dataset",  type: "input_dataset",  desc: "Source data" },
      { label: "Output Dataset", type: "output_dataset", desc: "Save result" },
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

const NODE_STYLE = {
  input_dataset:  { bg: C.blue,      text: "Source" },
  output_dataset: { bg: C.green,     text: "Output" },
  filter_rows:    { bg: C.blueMid,   text: "Filter" },
  fill_null:      { bg: C.blueMid,   text: "Fill" },
  group_agg:      { bg: C.navyLight, text: "Agg" },
  join_data:      { bg: C.blue,      text: "Join" },
  pyspark:        { bg: C.orange,    text: "Spark" },
  drop_col:       { bg: C.red,       text: "Drop" },
  default:        { bg: C.gold,      text: "Util" },
};
const nStyle = (type) => NODE_STYLE[type] || NODE_STYLE.default;

// ─── Custom Node ──────────────────────────────────────────────────────────────
function ETLNode({ id, data }) {
  const { bg } = nStyle(data.type);
  const isInput  = data.type === "input_dataset";
  const isOutput = data.type === "output_dataset";

  return (
    <div style={{ background: C.white, border: `2px solid ${bg}`, borderRadius: 10, minWidth: 180, boxShadow: `0 4px 16px ${bg}22`, fontFamily: "'DM Sans',sans-serif", overflow: "hidden" }}>
      {!isInput  && <Handle type="target" position={Position.Left}  style={{ background: bg, width: 10, height: 10, border: `2px solid ${C.white}` }} />}
      {!isOutput && <Handle type="source" position={Position.Right} style={{ background: bg, width: 10, height: 10, border: `2px solid ${C.white}` }} />}

      {/* Header */}
      <div style={{ background: bg, padding: "6px 10px", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <span style={{ color: C.white, fontSize: 11, fontWeight: 700 }}>
          {isInput ? "📥 " : isOutput ? "📤 " : ""}{data.label}
        </span>
        <div style={{ display: "flex", gap: 3 }}>
          <button onClick={() => data.onDuplicate(id)} title="Duplicate" style={{ background: "rgba(255,255,255,0.2)", border: "none", borderRadius: 4, width: 20, height: 20, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", color: C.white }}><Ic.Copy /></button>
          <button onClick={() => data.onDelete(id)}    title="Delete"    style={{ background: "rgba(255,255,255,0.2)", border: "none", borderRadius: 4, width: 20, height: 20, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", color: C.white }}><Ic.Trash /></button>
        </div>
      </div>

      {/* Body */}
      <div style={{ padding: "8px 10px" }}>
        {isInput && (
          <div>
            {data.config?.dataset ? (
              <div style={{ background: C.blueTint, borderRadius: 6, padding: "5px 8px" }}>
                <div style={{ fontSize: 10, fontWeight: 700, color: C.blue }}>{data.config.dataset.name}</div>
                <div style={{ fontSize: 9, color: C.g400 }}>{data.config.dataset.row_count?.toLocaleString()} rows</div>
              </div>
            ) : (
              <button onClick={() => data.onConfigure(id)} style={{ width: "100%", padding: "5px 0", borderRadius: 6, border: `1px dashed ${C.blue}`, background: C.blueTint, color: C.blue, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>
                + Select Dataset
              </button>
            )}
          </div>
        )}

        {isOutput && (
          <div>
            {data.config?.outputName ? (
              <div style={{ background: C.greenTint, borderRadius: 6, padding: "5px 8px" }}>
                <div style={{ fontSize: 10, fontWeight: 700, color: C.green }}>{data.config.outputName}</div>
                <div style={{ fontSize: 9, color: C.g400 }}>{data.config.description || "No description"}</div>
              </div>
            ) : (
              <button onClick={() => data.onConfigure(id)} style={{ width: "100%", padding: "5px 0", borderRadius: 6, border: `1px dashed ${C.green}`, background: C.greenTint, color: C.green, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>
                + Configure Output
              </button>
            )}
          </div>
        )}

        {data.type === "filter_rows" && (
          <input defaultValue={data.config?.condition || ""} onChange={e => data.onConfig(id, { condition: e.target.value })}
            placeholder="e.g. age > 18" style={{ width: "100%", padding: "4px 6px", borderRadius: 5, border: `1px solid ${C.g200}`, fontSize: 11, boxSizing: "border-box", color: C.g700 }} />
        )}

        {data.type === "pyspark" && (
          <textarea defaultValue={data.config?.code || ""} onChange={e => data.onConfig(id, { code: e.target.value })}
            placeholder="# PySpark code..." style={{ width: "100%", height: 60, padding: "4px 6px", borderRadius: 5, border: `1px solid ${C.g200}`, fontSize: 10, fontFamily: "monospace", resize: "none", boxSizing: "border-box", color: C.g700 }} />
        )}

        {data.type === "group_agg" && (
          <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            <select onChange={e => data.onConfig(id, { ...data.config, groupBy: e.target.value })}
              style={{ padding: "3px 5px", borderRadius: 5, border: `1px solid ${C.g200}`, fontSize: 11, color: C.g700 }}>
              <option value="">Group by...</option>
              {(data.columns || []).map(c => <option key={c}>{c}</option>)}
            </select>
            <select onChange={e => data.onConfig(id, { ...data.config, agg: e.target.value })}
              style={{ padding: "3px 5px", borderRadius: 5, border: `1px solid ${C.g200}`, fontSize: 11, color: C.g700 }}>
              <option>COUNT(*)</option><option>SUM</option><option>AVG</option><option>MAX</option><option>MIN</option>
            </select>
          </div>
        )}

        {!["input_dataset","output_dataset","filter_rows","pyspark","group_agg"].includes(data.type) && (
          <div>
            {data.config && Object.keys(data.config).length > 0 ? (
              <div style={{ background: "#DCFCE7", borderRadius: 6, padding: "4px 8px", fontSize: 10, color: "#16A34A", fontWeight: 600 }}>
                ✓ Configured — click to edit
              </div>
            ) : (
              <button onClick={() => data.onConfigure(id)} style={{ width: "100%", padding: "4px 0", borderRadius: 6, border: "1px dashed #F59E0B", background: "#FFFBEB", color: "#F59E0B", fontSize: 11, fontWeight: 600, cursor: "pointer" }}>
                + Configure
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

const nodeTypes = { etlNode: ETLNode };

// ─── Toast ────────────────────────────────────────────────────────────────────
function Toast({ toasts }) {
  return (
    <div style={{ position: "fixed", bottom: 24, right: 24, zIndex: 9999, display: "flex", flexDirection: "column", gap: 8, pointerEvents: "none" }}>
      {toasts.map(t => (
        <div key={t.id} style={{ padding: "10px 18px", borderRadius: 10, fontSize: 13, fontWeight: 600, background: t.type === "success" ? C.green : t.type === "error" ? C.red : C.blue, color: C.white, boxShadow: "0 4px 16px rgba(0,0,0,0.18)", animation: "slideIn .2s ease" }}>{t.msg}</div>
      ))}
    </div>
  );
}

// ─── Modal ────────────────────────────────────────────────────────────────────
function Modal({ title, onClose, children, width = 480 }) {
  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(11,30,61,.6)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000 }} onClick={onClose}>
      <div style={{ background: C.white, borderRadius: 16, width, maxWidth: "92vw", boxShadow: "0 24px 64px rgba(0,0,0,.22)", overflow: "hidden" }} onClick={e => e.stopPropagation()}>
        <div style={{ background: `linear-gradient(90deg,${C.navy},${C.navyLight})`, padding: "16px 20px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <span style={{ color: C.white, fontWeight: 800, fontSize: 15 }}>{title}</span>
          <button onClick={onClose} style={{ background: "rgba(255,255,255,.1)", border: "none", borderRadius: 6, padding: "4px 8px", cursor: "pointer", color: C.white }}><Ic.X /></button>
        </div>
        <div style={{ padding: 22 }}>{children}</div>
      </div>
    </div>
  );
}

// ─── Workflow Editor (ReactFlow canvas) ───────────────────────────────────────
function WorkflowEditor({ workflow, datasets, onSave, onBack, toast }) {
  const [nodes, setNodes, onNodesChange] = useNodesState(workflow.nodes || []);
  const [edges, setEdges, onEdgesChange] = useEdgesState(workflow.edges || []);
  const [wfTab, setWfTab]   = useState("datasets");
  const [counter, setCounter] = useState(100);
  const [running, setRunning] = useState(false);
  const [dagStatus, setDagStatus] = useState(null);
  const [taskStates, setTaskStates] = useState({});
  const [configModal, setConfigModal] = useState(null); // { nodeId, type }
  const [utilConfigNode, setUtilConfigNode] = useState(null); // for utility nodes
  const [inputConfig, setInputConfig]  = useState({ datasetId: "" });
  const [outputConfig, setOutputConfig] = useState({ outputName: "", description: "" });

  const onConnect = useCallback(
    p => setEdges(es => addEdge({ ...p, animated: true, style: { stroke: C.blue, strokeWidth: 2 } }, es)),
    [setEdges]
  );

  const makeNodeData = useCallback((label, type, extra = {}) => ({
    label, type, config: null, columns: [],
    ...extra,
    onDelete:    (nid) => setNodes(ns => ns.filter(n => n.id !== nid)),
    onDuplicate: (nid) => setNodes(ns => {
      const o = ns.find(n => n.id === nid); if (!o) return ns;
      return [...ns, { ...o, id: `n${Date.now()}`, position: { x: o.position.x + 30, y: o.position.y + 30 } }];
    }),
    onConfig:    (nid, val) => setNodes(ns => ns.map(n => n.id === nid ? { ...n, data: { ...n.data, config: { ...n.data.config, ...val } } } : n)),
    onConfigure: (nid) => {
        const isInputOutput = ["input_dataset","output_dataset"].includes(type);
        if (isInputOutput) { setConfigModal({ nodeId: nid, type }); }
        else { setNodes(ns => { const n = ns.find(x => x.id === nid); if (n) setUtilConfigNode(n); return ns; }); }
      },
  }), [setNodes]);

  const addNode = useCallback((item) => {
    const id = `n${counter}`;
    setCounter(c => c + 1);
    setNodes(ns => [...ns, {
      id, type: "etlNode",
      position: { x: 80 + (counter % 5) * 210, y: 60 + Math.floor(counter / 5) * 150 },
      data: makeNodeData(item.label, item.type),
    }]);
  }, [counter, makeNodeData]);

  // Apply dataset config to input node
  const applyInputConfig = () => {
    const ds = datasets.find(d => d.id === parseInt(inputConfig.datasetId));
    if (!ds) return;
    setNodes(ns => ns.map(n => n.id === configModal.nodeId ? {
      ...n, data: { ...n.data, config: { dataset: ds }, columns: ds.columns || [],
        onDelete: n.data.onDelete, onDuplicate: n.data.onDuplicate,
        onConfig: n.data.onConfig, onConfigure: n.data.onConfigure,
      }
    } : n));
    setConfigModal(null);
    toast(`Dataset "${ds.name}" assigned to node`, "success");
  };

  // Apply output config
  const applyOutputConfig = async () => {
    if (!outputConfig.outputName.trim()) return toast("Output name is required", "error");
    setNodes(ns => ns.map(n => n.id === configModal.nodeId ? {
      ...n, data: { ...n.data, config: { ...outputConfig },
        onDelete: n.data.onDelete, onDuplicate: n.data.onDuplicate,
        onConfig: n.data.onConfig, onConfigure: n.data.onConfigure,
      }
    } : n));
    setConfigModal(null);
    toast("Output configured", "success");
  };

  // Save workflow
  const handleSave = () => {
    const serialized = {
      ...workflow,
      nodes: nodes.map(n => ({
        id: n.id, type: n.type, position: n.position,
        data: { label: n.data.label, type: n.data.type, config: n.data.config, columns: n.data.columns },
      })),
      edges: edges.map(e => ({ id: e.id, source: e.source, target: e.target, animated: e.animated, style: e.style })),
      updatedAt: new Date().toISOString(),
    };
    onSave(serialized);
    toast("Workflow saved!", "success");
  };

  // Run pipeline
  const handleRun = async () => {
    const inputNodes  = nodes.filter(n => n.data.type === "input_dataset");
    const outputNodes = nodes.filter(n => n.data.type === "output_dataset");

    if (inputNodes.length === 0)  return toast("Add an Input Dataset node first", "error");
    if (outputNodes.length === 0) return toast("Add an Output Dataset node first", "error");

    const unconfigInput  = inputNodes.find(n => !n.data.config?.dataset);
    const unconfigOutput = outputNodes.find(n => !n.data.config?.outputName);
    if (unconfigInput)  return toast("Configure Input Dataset node first", "error");
    if (unconfigOutput) return toast("Configure Output Dataset node first", "error");

    const input      = inputNodes[0].data.config.dataset;
    const output     = outputNodes[0].data.config;
    const transforms = nodes
      .filter(n => !["input_dataset","output_dataset"].includes(n.data.type))
      .map(n => ({ type: n.data.type, config: n.data.config }));

    const inputTable = input.table_name
      ? `staging.${input.table_name}`
      : `staging.${input.name.replace(/\.[^.]+$/, '').toLowerCase().replace(/\s+/g, '_')}`;

    setRunning(true);
    try {
      const r = await API.runNewPipeline({
        workflow_id:   workflow.id,
        workflow_name: workflow.name,
        input_table:   inputTable,
        output_name:   output.outputName,
        description:   output.description || "",
        transforms,
      });

      const { run_id, dag_id } = r.data;
      toast(`Pipeline triggered! DAG: ${dag_id}`, "success");
      setDagStatus({ dag_id, state: "queued", run_id });
      handleSave();

      // Poll status via backend
      const poll = setInterval(async () => {
        try {
          const s = await API.getDagStatus(run_id);
          const { state, tasks } = s.data;
          setDagStatus(s.data);
          setTaskStates(tasks || {});

          if (["success","failed"].includes(state)) {
            clearInterval(poll);
            setRunning(false);
            toast(
              state === "success" ? "Pipeline completed!" : "Pipeline failed",
              state === "success" ? "success" : "error"
            );
            if (state === "success") handleSave();
          }
        } catch {}
      }, 6000);

    } catch (e) {
      setRunning(false);
      toast(e.response?.data?.detail || "Failed to trigger pipeline", "error");
    }
  };

  // Poll DAG
  useEffect(() => {
    if (!running) return;
    const poll = setInterval(async () => {
      try {
        const runs = await API.getDagRuns("etl_pipeline");
        const run  = runs.data.dag_runs?.[0];
        if (!run) return;
        setDagStatus(run);
        const tasks = await API.getTaskInstances("etl_pipeline", run.dag_run_id);
        const sm = {};
        tasks.data.task_instances?.forEach(t => { sm[t.task_id] = t.state || "none"; });
        setTaskStates(sm);
        if (["success","failed"].includes(run.state)) {
          setRunning(false);
          toast(run.state === "success" ? "Pipeline completed!" : "Pipeline failed", run.state === "success" ? "success" : "error");
          if (run.state === "success") handleSave();
        }
      } catch {}
    }, 6000);
    return () => clearInterval(poll);
  }, [running]);

  const STATUS_BG     = { success: C.greenTint, running: C.blueTint, failed: C.redTint, queued: C.goldTint, none: C.g50 };
  const STATUS_BORDER = { success: C.green, running: C.blue, failed: C.red, queued: C.gold, none: C.g300 };

  // Rebuild node callbacks after load (they don't serialize)
  useEffect(() => {
    setNodes(ns => ns.map(n => ({
      ...n, data: {
        ...n.data,
        onDelete:    (nid) => setNodes(ns2 => ns2.filter(x => x.id !== nid)),
        onDuplicate: (nid) => setNodes(ns2 => {
          const o = ns2.find(x => x.id === nid); if (!o) return ns2;
          return [...ns2, { ...o, id: `n${Date.now()}`, position: { x: o.position.x + 30, y: o.position.y + 30 } }];
        }),
        onConfig:    (nid, val) => setNodes(ns2 => ns2.map(x => x.id === nid ? { ...x, data: { ...x.data, config: { ...x.data.config, ...val } } } : x)),
        onConfigure: (nid) => setConfigModal({ nodeId: nid, type: n.data.type }),
      }
    })));
  }, []);

  // Update task state colors
  useEffect(() => {
    if (!Object.keys(taskStates).length) return;
    setNodes(ns => ns.map(n => {
      const state = taskStates[n.id] || "none";
      return { ...n, style: { background: STATUS_BG[state], borderColor: STATUS_BORDER[state] } };
    }));
  }, [taskStates]);

  return (
    <div style={{ display: "flex", flex: 1, overflow: "hidden", flexDirection: "column" }}>
      {/* Editor topbar */}
      <div style={{ background: C.white, borderBottom: `1px solid ${C.g200}`, padding: "0 20px", height: 50, display: "flex", alignItems: "center", justifyContent: "space-between", flexShrink: 0 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <button onClick={onBack} style={{ display: "flex", alignItems: "center", gap: 5, padding: "6px 12px", borderRadius: 7, border: `1px solid ${C.g200}`, background: C.g50, cursor: "pointer", fontSize: 12, fontWeight: 600, color: C.g600 }}>
            <Ic.Back /> Back
          </button>
          <div>
            <span style={{ fontWeight: 800, fontSize: 14, color: C.navy }}>{workflow.name}</span>
            <span style={{ fontSize: 11, color: C.g400, marginLeft: 8 }}>{nodes.length} nodes · {edges.length} edges</span>
          </div>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {dagStatus && (
            <div style={{ padding: "4px 10px", borderRadius: 20, fontSize: 11, fontWeight: 700, background: STATUS_BG[dagStatus.state] || C.g100, color: STATUS_BORDER[dagStatus.state] || C.g500 }}>
              DAG: {dagStatus.state?.toUpperCase()}
            </div>
          )}
          <button onClick={handleSave} style={{ display: "flex", alignItems: "center", gap: 5, padding: "7px 14px", borderRadius: 7, border: `1px solid ${C.g200}`, background: C.white, cursor: "pointer", fontSize: 12, fontWeight: 600, color: C.g600 }}>
            <Ic.Save /> Save
          </button>
          <button onClick={handleRun} disabled={running} style={{ display: "flex", alignItems: "center", gap: 5, padding: "7px 16px", borderRadius: 7, background: running ? C.g300 : `linear-gradient(90deg,${C.blue},${C.blueMid})`, border: "none", cursor: running ? "not-allowed" : "pointer", fontSize: 12, fontWeight: 700, color: C.white, boxShadow: running ? "none" : `0 2px 8px ${C.blue}44` }}>
            {running ? <>⟳ Running…</> : <><Ic.Play /> Run Pipeline</>}
          </button>
        </div>
      </div>

      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        {/* Left panel */}
        <div style={{ width: 210, background: C.white, borderRight: `1px solid ${C.g200}`, display: "flex", flexDirection: "column", flexShrink: 0 }}>
          {/* Tabs */}
          <div style={{ display: "flex", borderBottom: `1px solid ${C.g200}` }}>
            {Object.entries(NODE_PALETTE).map(([k, v]) => (
              <button key={k} onClick={() => setWfTab(k)} style={{ flex: 1, padding: "9px 0", fontSize: 11, fontWeight: 700, border: "none", cursor: "pointer", borderBottom: wfTab === k ? `2px solid ${v.color}` : "2px solid transparent", background: wfTab === k ? C.g50 : C.white, color: wfTab === k ? v.color : C.g400 }}>
                {v.label}
              </button>
            ))}
          </div>

          {/* Node list */}
          <div style={{ flex: 1, overflow: "auto", padding: "6px 0" }}>
            {NODE_PALETTE[wfTab].items.map(item => (
              <button key={item.type} onClick={() => addNode(item)}
                onMouseEnter={e => e.currentTarget.style.background = C.g50}
                onMouseLeave={e => e.currentTarget.style.background = "none"}
                style={{ width: "100%", display: "flex", alignItems: "center", gap: 8, padding: "8px 14px", background: "none", border: "none", cursor: "pointer", textAlign: "left" }}>
                <span style={{ width: 7, height: 7, borderRadius: 2, background: (nStyle(item.type)).bg, flexShrink: 0 }} />
                <span style={{ fontSize: 12, fontWeight: 500, color: C.g700 }}>{item.label}</span>
                <span style={{ marginLeft: "auto", color: C.g300 }}><Ic.Plus /></span>
              </button>
            ))}
          </div>

          {/* Clear button */}
          <div style={{ padding: 12, borderTop: `1px solid ${C.g200}` }}>
            <button onClick={() => { setNodes([]); setEdges([]); setDagStatus(null); setTaskStates({}); }}
              style={{ width: "100%", padding: "7px 0", background: C.g50, border: `1px solid ${C.g200}`, borderRadius: 7, color: C.g500, fontSize: 12, fontWeight: 600, cursor: "pointer" }}>
              Clear Canvas
            </button>
          </div>
        </div>

        {/* Canvas */}
        <div style={{ flex: 1, position: "relative" }}>
          {nodes.length === 0 && (
            <div style={{ position: "absolute", inset: 0, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", pointerEvents: "none", zIndex: 5 }}>
              <div style={{ fontSize: 36, marginBottom: 10 }}>⚡</div>
              <div style={{ fontWeight: 700, color: C.g400, marginBottom: 4 }}>Canvas is empty</div>
              <div style={{ fontSize: 12, color: C.g300 }}>Start with an Input Dataset node from the left panel</div>
            </div>
          )}
          <ReactFlow nodes={nodes} edges={edges} onNodesChange={onNodesChange} onEdgesChange={onEdgesChange} onConnect={onConnect} nodeTypes={nodeTypes} fitView style={{ background: C.g50 }}>
            <Background color={C.g200} gap={20} />
            <Controls style={{ background: C.white, border: `1px solid ${C.g200}` }} />
            <MiniMap style={{ background: C.white, border: `1px solid ${C.g200}`, borderRadius: 8 }} nodeColor={n => nStyle(n.data?.type).bg} />
            <Panel position="top-right">
              <div style={{ background: C.white, border: `1px solid ${C.g200}`, borderRadius: 8, padding: "4px 10px", fontSize: 11, color: C.g500 }}>
                {nodes.length} nodes · {edges.length} edges
              </div>
            </Panel>
          </ReactFlow>
        </div>
      </div>

      {/* ── Input Dataset Config Modal ── */}
      {configModal?.type === "input_dataset" && (
        <Modal title="Configure Input Dataset" onClose={() => setConfigModal(null)}>
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: C.g600, marginBottom: 8 }}>Select Dataset</div>
            {datasets.length === 0 ? (
              <div style={{ background: C.redTint, borderRadius: 8, padding: 12, fontSize: 12, color: C.red }}>
                No datasets available. Upload a dataset in Data Source first.
              </div>
            ) : (
              <select value={inputConfig.datasetId} onChange={e => setInputConfig({ datasetId: e.target.value })}
                style={{ width: "100%", padding: "10px 12px", borderRadius: 8, border: `1px solid ${C.g200}`, fontSize: 13, color: C.g700, outline: "none" }}>
                <option value="">— Choose a dataset —</option>
                {datasets.map(d => (
                  <option key={d.id} value={d.id}>{d.name} ({d.row_count?.toLocaleString() || "?"} rows · {d.type})</option>
                ))}
              </select>
            )}
          </div>

          {/* Dataset preview */}
          {inputConfig.datasetId && (() => {
            const ds = datasets.find(d => d.id === parseInt(inputConfig.datasetId));
            return ds ? (
              <div style={{ background: C.blueTint, borderRadius: 8, padding: 12, marginBottom: 16 }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: C.blue, marginBottom: 6 }}>{ds.name}</div>
                <div style={{ display: "flex", gap: 12, fontSize: 11, color: C.blue, opacity: 0.8 }}>
                  <span>{ds.row_count?.toLocaleString()} rows</span>
                  <span>{ds.file_size}</span>
                  <span>{ds.type}</span>
                </div>
                {ds.columns?.length > 0 && (
                  <div style={{ marginTop: 8, display: "flex", flexWrap: "wrap", gap: 4 }}>
                    {ds.columns.map(c => (
                      <span key={c} style={{ fontSize: 10, padding: "2px 6px", borderRadius: 4, background: C.blue + "22", color: C.blue, fontWeight: 600 }}>{c}</span>
                    ))}
                  </div>
                )}
              </div>
            ) : null;
          })()}

          <div style={{ display: "flex", gap: 8 }}>
            <button onClick={() => setConfigModal(null)} style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: `1px solid ${C.g200}`, background: C.white, color: C.g600, fontSize: 13, fontWeight: 600, cursor: "pointer" }}>Cancel</button>
            <button onClick={applyInputConfig} disabled={!inputConfig.datasetId} style={{ flex: 1, padding: "9px 0", borderRadius: 8, background: !inputConfig.datasetId ? C.g300 : C.blue, border: "none", color: C.white, fontSize: 13, fontWeight: 700, cursor: !inputConfig.datasetId ? "not-allowed" : "pointer" }}>
              Apply Dataset
            </button>
          </div>
        </Modal>
      )}

      {/* ── Output Dataset Config Modal ── */}
      {configModal?.type === "output_dataset" && (
        <Modal title="Configure Output Dataset" onClose={() => setConfigModal(null)}>
          <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
            <div>
              <div style={{ fontSize: 12, fontWeight: 700, color: C.g600, marginBottom: 6 }}>Output Table Name <span style={{ color: C.red }}>*</span></div>
              <input value={outputConfig.outputName} onChange={e => setOutputConfig(o => ({ ...o, outputName: e.target.value.toLowerCase().replace(/\s+/g,"_").replace(/[^a-z0-9_]/g,"") })) }
                placeholder="e.g. sales_summary_2026" style={{ width: "100%", padding: "10px 12px", borderRadius: 8, border: `1.5px solid ${outputConfig.outputName ? C.green : C.g200}`, fontSize: 13, boxSizing: "border-box", outline: "none", color: C.g700, fontFamily: "monospace" }} />
              <div style={{ fontSize: 10, color: C.g400, marginTop: 4 }}>Lowercase letters, numbers, and underscores only. This will be saved as warehouse.{outputConfig.outputName || "your_table"}</div>
            </div>
            <div>
              <div style={{ fontSize: 12, fontWeight: 700, color: C.g600, marginBottom: 6 }}>Description</div>
              <textarea value={outputConfig.description} onChange={e => setOutputConfig(o => ({ ...o, description: e.target.value }))}
                placeholder="Describe what this output contains..." rows={3} style={{ width: "100%", padding: "10px 12px", borderRadius: 8, border: `1px solid ${C.g200}`, fontSize: 13, boxSizing: "border-box", outline: "none", color: C.g700, resize: "none", fontFamily: "'DM Sans',sans-serif" }} />
            </div>

            {outputConfig.outputName && (
              <div style={{ background: C.greenTint, borderRadius: 8, padding: 10, fontSize: 11, color: C.green }}>
                <span style={{ fontWeight: 700 }}>Preview: </span>
                Data will be saved to <code style={{ background: C.white, padding: "1px 5px", borderRadius: 4 }}>warehouse.{outputConfig.outputName}</code>
                {outputConfig.description && <div style={{ marginTop: 4, opacity: 0.8 }}>{outputConfig.description}</div>}
              </div>
            )}
          </div>

          <div style={{ display: "flex", gap: 8, marginTop: 18 }}>
            <button onClick={() => setConfigModal(null)} style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: `1px solid ${C.g200}`, background: C.white, color: C.g600, fontSize: 13, fontWeight: 600, cursor: "pointer" }}>Cancel</button>
            <button onClick={applyOutputConfig} disabled={!outputConfig.outputName} style={{ flex: 1, padding: "9px 0", borderRadius: 8, background: !outputConfig.outputName ? C.g300 : C.green, border: "none", color: C.white, fontSize: 13, fontWeight: 700, cursor: !outputConfig.outputName ? "not-allowed" : "pointer" }}>
              Save Output Config
            </button>
          </div>
        </Modal>
      )}

      {/* ── Utility Node Config Modal ── */}
      {utilConfigNode && (
        <UtilityConfigModal
          node={utilConfigNode}
          columns={(() => {
            // Find columns from connected input dataset node
            const inputNode = nodes.find(n => n.data?.type === "input_dataset" && n.data?.config?.dataset);
            return inputNode?.data?.config?.dataset?.columns || [];
          })()}
          allNodes={nodes}
          onSave={(nodeId, cfg) => {
            setNodes(ns => ns.map(n => n.id === nodeId ? {
              ...n, data: { ...n.data, config: cfg }
            } : n));
            setUtilConfigNode(null);
          }}
          onClose={() => setUtilConfigNode(null)}
        />
      )}
    </div>
  );
}

// ─── Main App ─────────────────────────────────────────────────────────────────
export default function ETLPipelineApp() {
  const [page, setPage]         = useState("workflow");
  const [collapsed, setCollapsed] = useState(false);
  const [toasts, setToasts]     = useState([]);
  const [airflowOk, setAirflowOk] = useState(false);

  // Dataset state
  const [datasets, setDatasets]   = useState([]);
  const [dsLoading, setDsLoading] = useState(false);
  const [dsTab, setDsTab]         = useState("files");
  const [searchQ, setSearchQ]     = useState("");
  const [filterType, setFilterType] = useState("all");
  const [selectedDS, setSelectedDS] = useState(null);
  const [preview, setPreview]     = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [showAddModal, setShowAddModal] = useState(false);
  const [addTab, setAddTab]       = useState("csv");
  const [uploadFile, setUploadFile] = useState(null);
  const [uploadName, setUploadName] = useState("");
  const [uploading, setUploading]   = useState(false);
  const [dbForm, setDbForm] = useState({ host: "postgres", port: "5432", database: "airflow", username: "airflow", password: "airflow" });
  const [dbConnecting, setDbConnecting] = useState(false);
  const [pipelineRuns, setPipelineRuns] = useState([]);
  const [selectedRun, setSelectedRun]  = useState(null);
  const [runPreview, setRunPreview]     = useState(null);
  const [runPreviewLoading, setRunPreviewLoading] = useState(false);

  // Workflow state
  const [workflows, setWorkflows]       = useState([]);
  const [wfSearch, setWfSearch]         = useState("");
  const [wfFilter, setWfFilter]         = useState("all");
  const [activeWorkflow, setActiveWorkflow] = useState(null); // editing
  const [showNewWfModal, setShowNewWfModal] = useState(false);
  const [newWfForm, setNewWfForm]       = useState({ name: "", description: "" });

  // Visualization state
  const [vizDatasets, setVizDatasets]     = useState([]);
  const [warehouseTables, setWarehouseTables] = useState([]);

  const toast = useCallback((msg, type = "info") => {
    const id = Date.now();
    setToasts(t => [...t, { id, msg, type }]);
    setTimeout(() => setToasts(t => t.filter(x => x.id !== id)), 3500);
  }, []);

  // ── Boot ───────────────────────────────────────────────────────
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
      // Load columns for each deployed dataset
      const enriched = await Promise.all(data.map(async ds => {
        if (ds.status === "deployed" && ds.table_name) {
          try { const p = await API.previewDataset(ds.id, 1); return { ...ds, columns: p.data.columns || [] }; }
          catch { return ds; }
        }
        return ds;
      }));
      setDatasets(enriched);
      setVizDatasets(enriched.filter(d => d.status === "deployed"));
    } catch {
      toast("Backend not running — some features may be unavailable", "error");
      setDatasets([]); setVizDatasets([]);
    } finally { setDsLoading(false); }
  };

  const loadWarehouse = async () => {
    try {
      const [tablesRes, runsRes] = await Promise.all([
        API.getWarehouseTables(),
        API.getPipelineRuns(),
      ]);
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
      await API.uploadDataset(uploadFile, uploadName || uploadFile.name);
      toast("Dataset uploaded & deployed!", "success");
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

  // ── Workflow actions ───────────────────────────────────────────
  const createWorkflow = () => {
    if (!newWfForm.name.trim()) return toast("Workflow name is required", "error");
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

  // Filtered
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

  // If editing a workflow, show editor full screen
  if (activeWorkflow && page === "workflow") {
    return (
      <div style={{ display: "flex", height: "100vh", fontFamily: "'DM Sans','Segoe UI',sans-serif", overflow: "hidden" }}>
        <style>{`*{box-sizing:border-box;margin:0;padding:0} @keyframes slideIn{from{transform:translateX(40px);opacity:0}to{transform:none;opacity:1}} .hrow:hover{background:${C.blueTint}!important}`}</style>
        <WorkflowEditor
          workflow={activeWorkflow}
          datasets={datasets}
          onSave={saveWorkflow}
          onBack={() => setActiveWorkflow(null)}
          toast={toast}
        />
        <Toast toasts={toasts} />
      </div>
    );
  }

  return (
    <div style={{ display: "flex", height: "100vh", fontFamily: "'DM Sans','Segoe UI',sans-serif", background: C.off, overflow: "hidden" }}>
      <style>{`*{box-sizing:border-box;margin:0;padding:0} @keyframes slideIn{from{transform:translateX(40px);opacity:0}to{transform:none;opacity:1}} .hrow:hover{background:${C.blueTint}!important}`}</style>

      {/* SIDEBAR */}
      <aside style={{ width: collapsed ? 58 : 210, flexShrink: 0, background: `linear-gradient(180deg,${C.navy},${C.navyMid})`, display: "flex", flexDirection: "column", transition: "width .22s cubic-bezier(.4,0,.2,1)", boxShadow: "3px 0 20px rgba(0,0,0,0.22)", zIndex: 20 }}>
        <div style={{ padding: collapsed ? "18px 0" : "18px 16px", borderBottom: `1px solid ${C.navyLight}`, display: "flex", alignItems: "center", justifyContent: collapsed ? "center" : "space-between", minHeight: 60 }}>
          {!collapsed && <div><span style={{ color: C.white, fontWeight: 800, fontSize: 17 }}>ETL<span style={{ color: C.gold }}>Flow</span></span><div style={{ color: C.blueLight, fontSize: 9, marginTop: 1, letterSpacing: 1 }}>PIPELINE STUDIO</div></div>}
          <button onClick={() => setCollapsed(v => !v)} style={{ background: "none", border: "none", cursor: "pointer", color: C.blueLight, display: "flex", padding: 4, borderRadius: 5 }}><Ic.Menu /></button>
        </div>
        <nav style={{ flex: 1, padding: "10px 0" }}>
          {nav.map(({ id, label, Icon }) => {
            const active = page === id;
            return (
              <button key={id} onClick={() => setPage(id)} style={{ width: "100%", display: "flex", alignItems: "center", gap: 10, padding: collapsed ? "12px 0" : "11px 16px", justifyContent: collapsed ? "center" : "flex-start", background: active ? `linear-gradient(90deg,rgba(29,111,235,.28),transparent)` : "none", border: "none", borderLeft: active ? `3px solid ${C.gold}` : "3px solid transparent", cursor: "pointer", color: active ? C.white : C.blueLight, transition: "all .12s" }}>
                <span style={{ padding: 7, borderRadius: 8, background: active ? C.blue : "rgba(255,255,255,.06)", display: "flex" }}><Icon /></span>
                {!collapsed && <span style={{ fontSize: 13, fontWeight: active ? 700 : 500 }}>{label}</span>}
              </button>
            );
          })}
        </nav>
        <div style={{ padding: collapsed ? "14px 0" : "14px 16px", borderTop: `1px solid ${C.navyLight}`, display: "flex", justifyContent: collapsed ? "center" : "flex-start" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6, background: airflowOk ? "rgba(22,163,74,.15)" : "rgba(220,38,38,.15)", borderRadius: 20, padding: "4px 10px 4px 6px" }}>
            <span style={{ width: 7, height: 7, borderRadius: "50%", background: airflowOk ? C.green : C.red, display: "inline-block" }} />
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
              {page === "datasource"    && "Upload and manage your datasets"}
              {page === "workflow"      && "Create and manage ETL workflows"}
              {page === "visualization" && "Explore deployed data in Metabase"}
            </div>
          </div>
          {page === "workflow" && (
            <button onClick={() => setShowNewWfModal(true)} style={{ display: "flex", alignItems: "center", gap: 6, padding: "7px 16px", borderRadius: 8, background: C.blue, color: C.white, border: "none", fontWeight: 700, fontSize: 13, cursor: "pointer", boxShadow: `0 2px 8px ${C.blue}44` }}>
              <Ic.Plus /> New Workflow
            </button>
          )}
        </header>

        {/* ════ DATA SOURCE PAGE ════ */}
        {page === "datasource" && (
          <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
            {/* Left nav */}
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

            {/* Content */}
            <div style={{ flex: 1, padding: 22, overflow: "auto" }}>
              <div style={{ display: "flex", gap: 10, marginBottom: 18, flexWrap: "wrap" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 7, background: C.white, border: `1px solid ${C.g200}`, borderRadius: 8, padding: "7px 12px", flex: 1, minWidth: 200, maxWidth: 300 }}>
                  <Ic.Search />
                  <input value={searchQ} onChange={e => setSearchQ(e.target.value)} placeholder="Search datasets..." style={{ border: "none", outline: "none", fontSize: 13, color: C.g700, background: "transparent", width: "100%" }} />
                </div>
                <div style={{ display: "flex", gap: 5 }}>
                  {["all","csv","excel","postgresql","mysql"].map(t => (
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
                    <div key={ds.id} onClick={() => loadPreview(ds)} style={{ background: C.white, borderRadius: 12, border: selectedDS?.id === ds.id ? `2px solid ${C.blue}` : `1px solid ${C.g200}`, padding: 18, cursor: "pointer", transition: "all .14s", boxShadow: selectedDS?.id === ds.id ? `0 0 0 3px ${C.blue}22` : "0 1px 4px rgba(0,0,0,.05)" }}>
                      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 10 }}>
                        <span style={{ padding: "3px 9px", borderRadius: 5, fontSize: 10, fontWeight: 700, background: `${TYPE_COLOR[ds.type] || C.g400}18`, color: TYPE_COLOR[ds.type] || C.g400 }}>{ds.type}</span>
                        <span style={{ padding: "3px 9px", borderRadius: 20, fontSize: 10, fontWeight: 700, background: `${STATUS_COLOR[ds.status] || C.g400}18`, color: STATUS_COLOR[ds.status] || C.g400 }}>{ds.status}</span>
                      </div>
                      <div style={{ fontWeight: 700, fontSize: 14, color: C.navy, marginBottom: 3, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{ds.name}</div>
                      <div style={{ display: "flex", gap: 12, fontSize: 11, color: C.g400 }}>
                        {ds.row_count && <span>{ds.row_count.toLocaleString()} rows</span>}
                        {ds.file_size && <span>{ds.file_size}</span>}
                        <span>{ds.created_at?.slice(0,10)}</span>
                      </div>
                      <div style={{ display: "flex", gap: 6, marginTop: 12 }}>
                        <button onClick={e => { e.stopPropagation(); loadPreview(ds); }} style={{ flex: 1, padding: "6px 0", borderRadius: 6, border: `1px solid ${C.g200}`, background: C.g50, color: C.g600, fontSize: 11, fontWeight: 600, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 4 }}><Ic.Eye /> Preview</button>
                        <button onClick={e => handleDeleteDS(ds.id, e)} style={{ padding: "6px 10px", borderRadius: 6, border: `1px solid ${C.redTint}`, background: C.redTint, color: C.red, cursor: "pointer", display: "flex", alignItems: "center" }}><Ic.Trash /></button>
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

              {/* Preview */}
              {selectedDS && (
                <div style={{ marginTop: 20, background: C.white, borderRadius: 12, border: `1px solid ${C.g200}`, overflow: "hidden", boxShadow: "0 2px 12px rgba(0,0,0,.06)" }}>
                  <div style={{ padding: "12px 18px", background: C.navy, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <span style={{ color: C.white, fontWeight: 700, fontSize: 13 }}>📋 {selectedDS.name}</span>
                    <button onClick={() => { setSelectedDS(null); setPreview(null); }} style={{ background: "rgba(255,255,255,.1)", border: "none", borderRadius: 5, padding: "3px 8px", cursor: "pointer", color: C.white }}><Ic.X /></button>
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

        {/* ════ WORKFLOW PAGE ════ */}
        {page === "workflow" && (
          <div style={{ flex: 1, padding: 24, overflow: "auto" }}>
            {/* Search & filter */}
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

            {/* Workflow grid */}
            {filteredWF.length === 0 ? (
              <div style={{ textAlign: "center", padding: 60, color: C.g400 }}>
                <div style={{ fontSize: 36, marginBottom: 12 }}>⚡</div>
                <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 4 }}>No workflows yet</div>
                <div style={{ fontSize: 12, marginBottom: 20 }}>Create your first workflow to start building ETL pipelines</div>
                <button onClick={() => setShowNewWfModal(true)} style={{ padding: "10px 24px", borderRadius: 8, background: C.blue, border: "none", color: C.white, fontWeight: 700, fontSize: 13, cursor: "pointer" }}>
                  + Create Workflow
                </button>
              </div>
            ) : (
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(300px,1fr))", gap: 16 }}>
                {filteredWF.map(wf => {
                  const wfStatusColor = { draft: C.g400, running: C.blue, success: C.green, failed: C.red }[wf.status] || C.g400;
                  const wfStatusBg    = { draft: C.g100, running: C.blueTint, success: C.greenTint, failed: C.redTint }[wf.status] || C.g100;
                  return (
                    <div key={wf.id} style={{ background: C.white, borderRadius: 14, border: `1px solid ${C.g200}`, overflow: "hidden", boxShadow: "0 2px 10px rgba(0,0,0,.05)", transition: "transform .14s,box-shadow .14s" }}
                      onMouseEnter={e => { e.currentTarget.style.transform = "translateY(-2px)"; e.currentTarget.style.boxShadow = `0 8px 24px ${C.blue}18`; }}
                      onMouseLeave={e => { e.currentTarget.style.transform = "none"; e.currentTarget.style.boxShadow = "0 2px 10px rgba(0,0,0,.05)"; }}>

                      {/* Header stripe */}
                      <div style={{ height: 5, background: `linear-gradient(90deg,${C.blue},${C.gold})` }} />

                      <div style={{ padding: 18 }}>
                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 10 }}>
                          <div style={{ fontWeight: 800, fontSize: 15, color: C.navy, flex: 1, marginRight: 8 }}>{wf.name}</div>
                          <span style={{ padding: "3px 9px", borderRadius: 20, fontSize: 10, fontWeight: 700, background: wfStatusBg, color: wfStatusColor, flexShrink: 0 }}>{wf.status}</span>
                        </div>

                        {wf.description && <div style={{ fontSize: 12, color: C.g500, marginBottom: 10, lineHeight: 1.5 }}>{wf.description}</div>}

                        <div style={{ display: "flex", gap: 12, fontSize: 11, color: C.g400, marginBottom: 14 }}>
                          <span>{wf.nodes?.length || 0} nodes</span>
                          <span>{wf.edges?.length || 0} edges</span>
                          <span>{wf.updatedAt?.slice(0,10)}</span>
                        </div>

                        <div style={{ display: "flex", gap: 8 }}>
                          <button onClick={() => setActiveWorkflow(wf)} style={{ flex: 1, padding: "8px 0", borderRadius: 8, background: `linear-gradient(90deg,${C.blue},${C.blueMid})`, border: "none", color: C.white, fontWeight: 700, fontSize: 12, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 5 }}>
                            <Ic.Edit /> Edit
                          </button>
                          <button onClick={() => { setActiveWorkflow(wf); }} style={{ padding: "8px 10px", borderRadius: 8, border: `1px solid ${C.g200}`, background: C.g50, color: C.g500, cursor: "pointer", display: "flex", alignItems: "center" }} title="Open">
                            <Ic.ArrowRight />
                          </button>
                          <button onClick={() => deleteWorkflow(wf.id)} style={{ padding: "8px 10px", borderRadius: 8, border: `1px solid ${C.redTint}`, background: C.redTint, color: C.red, cursor: "pointer", display: "flex", alignItems: "center" }}>
                            <Ic.Trash />
                          </button>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        )}

        {/* ════ VISUALIZATION PAGE ════ */}
        {page === "visualization" && (
          <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>

            {/* Left panel — list pipeline runs */}
            <div style={{ width: 280, background: C.white, borderRight: `1px solid ${C.g200}`, display: "flex", flexDirection: "column", flexShrink: 0 }}>
              <div style={{ padding: "14px 16px", borderBottom: `1px solid ${C.g200}`, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <div style={{ fontWeight: 700, fontSize: 13, color: C.navy }}>Pipeline Outputs</div>
                <button onClick={loadWarehouse} style={{ background: "none", border: "none", cursor: "pointer", color: C.g400, display: "flex" }}><Ic.Refresh /></button>
              </div>

              <div style={{ flex: 1, overflow: "auto" }}>
                {pipelineRuns.length === 0 ? (
                  <div style={{ padding: 24, textAlign: "center", color: C.g400 }}>
                    <div style={{ fontSize: 24, marginBottom: 8 }}>📭</div>
                    <div style={{ fontSize: 12 }}>No pipeline runs yet</div>
                    <div style={{ fontSize: 11, marginTop: 4, color: C.g300 }}>Run a workflow first</div>
                  </div>
                ) : (
                  pipelineRuns.map(run => {
                    const active = selectedRun?.id === run.id;
                    const tableName = run.output_table?.replace('warehouse.', '') || '';
                    return (
                      <div key={run.id} onClick={async () => {
                        setSelectedRun(run);
                        setRunPreview(null);
                        setRunPreviewLoading(true);
                        try {
                          const r = await API.previewPipelineRun(run.id);
                          setRunPreview(r.data);
                        } catch { }
                        finally { setRunPreviewLoading(false); }
                      }} style={{
                        padding: "12px 16px", cursor: "pointer",
                        background: active ? C.blueTint : C.white,
                        borderLeft: active ? `3px solid ${C.blue}` : "3px solid transparent",
                        borderBottom: `1px solid ${C.g100}`,
                        transition: "all .12s",
                      }}>
                        <div style={{ fontWeight: 700, fontSize: 13, color: active ? C.blue : C.navy, marginBottom: 3 }}>
                          {tableName}
                        </div>
                        <div style={{ fontSize: 11, color: C.g400, marginBottom: 2 }}>
                          {run.row_count?.toLocaleString()} rows
                        </div>
                        <div style={{ fontSize: 10, color: C.g300 }}>
                          {run.ran_at?.slice(0, 16).replace('T', ' ')}
                        </div>
                        {run.workflow_id && run.workflow_id !== 'manual' && (
                          <div style={{ marginTop: 4, fontSize: 10, padding: "1px 6px", borderRadius: 10, background: C.blueTint2, color: C.blue, display: "inline-block" }}>
                            {run.workflow_id.slice(0, 12)}…
                          </div>
                        )}
                      </div>
                    );
                  })
                )}
              </div>
            </div>

            {/* Right panel — preview + Metabase link */}
            <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
              {!selectedRun ? (
                <div style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", color: C.g400 }}>
                  <div style={{ fontSize: 36, marginBottom: 12 }}>📊</div>
                  <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 4 }}>Select a pipeline output</div>
                  <div style={{ fontSize: 12 }}>Choose from the list on the left to preview data</div>
                </div>
              ) : (
                <>
                  {/* Header */}
                  <div style={{ padding: "14px 22px", borderBottom: `1px solid ${C.g200}`, background: C.white, display: "flex", justifyContent: "space-between", alignItems: "center", flexShrink: 0 }}>
                    <div>
                      <div style={{ fontWeight: 800, fontSize: 15, color: C.navy }}>
                        {selectedRun.output_table}
                      </div>
                      <div style={{ fontSize: 11, color: C.g400, marginTop: 2 }}>
                        {selectedRun.row_count?.toLocaleString()} rows ·{" "}
                        {runPreview?.columns?.length || 0} columns ·{" "}
                        {selectedRun.ran_at?.slice(0, 16).replace("T", " ")}
                      </div>
                    </div>
                    <a href="http://localhost:3000" target="_blank" rel="noopener noreferrer"
                      style={{ display: "flex", alignItems: "center", gap: 6, padding: "8px 16px", borderRadius: 8, background: `linear-gradient(90deg,${C.blue},${C.blueMid})`, color: C.white, fontWeight: 700, fontSize: 13, textDecoration: "none", boxShadow: `0 2px 8px ${C.blue}33` }}>
                      <Ic.Link /> Open in Metabase
                    </a>
                  </div>

                  {/* Column info */}
                  {runPreview && !runPreviewLoading && (
                    <div style={{ padding: "10px 22px", borderBottom: `1px solid ${C.g100}`, background: C.g50, display: "flex", flexWrap: "wrap", gap: 5, flexShrink: 0 }}>
                      {runPreview.columns.map(c => (
                        <span key={c} style={{ fontSize: 10, padding: "2px 7px", borderRadius: 4, background: C.blueTint2, color: C.blue, fontWeight: 600, fontFamily: "monospace" }}>{c}</span>
                      ))}
                    </div>
                  )}

                  {/* Data preview table */}
                  <div style={{ flex: 1, overflow: "auto" }}>
                    {runPreviewLoading && (
                      <div style={{ padding: 40, textAlign: "center", color: C.g400 }}>Loading data preview…</div>
                    )}
                    {runPreview && !runPreviewLoading && (
                      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                        <thead>
                          <tr style={{ background: C.g50, position: "sticky", top: 0, zIndex: 1 }}>
                            {runPreview.columns.map(c => (
                              <th key={c} style={{ padding: "8px 14px", textAlign: "left", fontWeight: 700, color: C.g600, borderBottom: `2px solid ${C.g200}`, whiteSpace: "nowrap", fontSize: 11 }}>{c}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {runPreview.rows.map((row, i) => (
                            <tr key={i} style={{ background: i % 2 === 0 ? C.white : C.g50 }}
                              onMouseEnter={e => e.currentTarget.style.background = C.blueTint}
                              onMouseLeave={e => e.currentTarget.style.background = i % 2 === 0 ? C.white : C.g50}>
                              {runPreview.columns.map(c => (
                                <td key={c} style={{ padding: "6px 14px", borderBottom: `1px solid ${C.g100}`, color: C.g700, whiteSpace: "nowrap", maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis" }}>
                                  {row[c] == null
                                    ? <span style={{ color: C.g300, fontStyle: "italic" }}>null</span>
                                    : String(row[c])}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </div>

                  {/* Footer */}
                  {runPreview && (
                    <div style={{ padding: "8px 22px", borderTop: `1px solid ${C.g200}`, background: C.g50, fontSize: 11, color: C.g400, display: "flex", justifyContent: "space-between", flexShrink: 0 }}>
                      <span>Showing {Math.min(100, runPreview.rows.length)} of {selectedRun.row_count?.toLocaleString()} rows</span>
                      <span>Table: <code style={{ fontFamily: "monospace", color: C.blue }}>{selectedRun.output_table}</code></span>
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        )}
          </div> 
          


      {/* ════ ADD SOURCE MODAL ════ */}
      {showAddModal && (
        <Modal title="Add Data Source" onClose={() => setShowAddModal(false)}>
          <div style={{ display: "flex", borderBottom: `1px solid ${C.g200}`, marginBottom: 20 }}>
            {[{id:"csv",l:"CSV"},{id:"excel",l:"Excel"},{id:"postgres",l:"PostgreSQL"},{id:"mysql",l:"MySQL"}].map(({id,l}) => (
              <button key={id} onClick={() => setAddTab(id)} style={{ flex: 1, padding: "10px 0", fontSize: 11, fontWeight: 700, border: "none", cursor: "pointer", borderBottom: addTab===id?`2px solid ${C.blue}`:"2px solid transparent", background: "none", color: addTab===id?C.blue:C.g400 }}>{l}</button>
            ))}
          </div>
          {(addTab==="csv"||addTab==="excel") && (
            <div>
              <label style={{ display: "block", border: `2px dashed ${uploadFile?C.blue:C.g300}`, borderRadius: 10, padding: "28px 20px", textAlign: "center", background: uploadFile?C.blueTint:C.g50, cursor: "pointer", marginBottom: 14 }}>
                <input type="file" accept={addTab==="csv"?".csv":".xlsx,.xls"} onChange={e=>{setUploadFile(e.target.files[0]);setUploadName(e.target.files[0]?.name||"");}} style={{display:"none"}}/>
                <div style={{fontSize:26,marginBottom:6}}>{uploadFile?"✅":"📁"}</div>
                <div style={{fontWeight:700,color:uploadFile?C.blue:C.g600,fontSize:13}}>{uploadFile?uploadFile.name:`Drop your ${addTab.toUpperCase()} file here`}</div>
                <div style={{fontSize:11,color:C.g400,marginTop:3}}>{uploadFile?`${(uploadFile.size/1024).toFixed(1)} KB`:"or click to browse"}</div>
              </label>
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

      {/* ════ NEW WORKFLOW MODAL ════ */}
      {showNewWfModal && (
        <Modal title="Create New Workflow" onClose={() => setShowNewWfModal(false)}>
          <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
            <div>
              <div style={{ fontSize: 12, fontWeight: 700, color: C.g600, marginBottom: 6 }}>Workflow Name <span style={{ color: C.red }}>*</span></div>
              <input value={newWfForm.name} onChange={e => setNewWfForm(f => ({ ...f, name: e.target.value }))} placeholder="e.g. Sales ETL Pipeline" style={{ width: "100%", padding: "10px 12px", borderRadius: 8, border: `1.5px solid ${newWfForm.name ? C.blue : C.g200}`, fontSize: 13, boxSizing: "border-box", outline: "none", color: C.g700 }} />
            </div>
            <div>
              <div style={{ fontSize: 12, fontWeight: 700, color: C.g600, marginBottom: 6 }}>Description</div>
              <textarea value={newWfForm.description} onChange={e => setNewWfForm(f => ({ ...f, description: e.target.value }))} placeholder="Describe what this pipeline does..." rows={3} style={{ width: "100%", padding: "10px 12px", borderRadius: 8, border: `1px solid ${C.g200}`, fontSize: 13, boxSizing: "border-box", outline: "none", color: C.g700, resize: "none", fontFamily: "'DM Sans',sans-serif" }} />
            </div>
          </div>
          <div style={{ display: "flex", gap: 8, marginTop: 20 }}>
            <button onClick={() => setShowNewWfModal(false)} style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: `1px solid ${C.g200}`, background: C.white, color: C.g600, fontSize: 13, fontWeight: 600, cursor: "pointer" }}>Cancel</button>
            <button onClick={createWorkflow} disabled={!newWfForm.name.trim()} style={{ flex: 1, padding: "9px 0", borderRadius: 8, background: !newWfForm.name.trim() ? C.g300 : `linear-gradient(90deg,${C.blue},${C.blueMid})`, border: "none", color: C.white, fontSize: 13, fontWeight: 700, cursor: !newWfForm.name.trim() ? "not-allowed" : "pointer" }}>
              Create & Open Editor
            </button>
          </div>
        </Modal>
      )}

      <Toast toasts={toasts} />
    </div>
  );
}
