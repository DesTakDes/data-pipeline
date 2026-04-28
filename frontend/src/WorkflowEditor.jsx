// WorkflowEditor.jsx
// Drop this file into src/ and import in App.jsx:
// import WorkflowEditor from "./WorkflowEditor";

import { useState, useCallback, useEffect, useMemo } from "react";
import {
  ReactFlow, Background, Controls, MiniMap, addEdge,
  useNodesState, useEdgesState, Panel, Handle, Position,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { UtilityConfigModal } from "./UtilityConfigs";
import { NodePreviewPanel } from "./NodePreview";
import { computeNodeColumns, getUpstreamColumns } from "./NodePropagation";
import axios from "axios";

const airflow = axios.create({
  baseURL: "/airflow-api/api/v1",
  headers: { Authorization: "Basic " + btoa("admin:admin123") },
});
const api = axios.create({ baseURL: "/api" });

// ── Colors ────────────────────────────────────────────────────────────────────
const C = {
  navy:"#0B1E3D", navyMid:"#122850", navyLight:"#1A3A6B",
  blue:"#1D6FEB", blueMid:"#3B82F6", blueLight:"#93C5FD",
  blueTint:"#EFF6FF", blueTint2:"#DBEAFE",
  gold:"#F59E0B", goldTint:"#FFFBEB",
  white:"#FFFFFF", g50:"#F8FAFC", g100:"#F1F5F9", g200:"#E2E8F0",
  g300:"#CBD5E1", g400:"#94A3B8", g500:"#64748B", g600:"#475569", g700:"#334155",
  green:"#16A34A", greenTint:"#DCFCE7",
  red:"#DC2626", redTint:"#FEE2E2",
  orange:"#EA580C",
};

// ── Node palette ──────────────────────────────────────────────────────────────
export const NODE_PALETTE = {
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
  pyspark: C.orange, drop_col: C.red, default: C.gold,
};
const nodeBg = (type) => NODE_BG[type] || NODE_BG.default;

// ── Icons ─────────────────────────────────────────────────────────────────────
const Ic = {
  Back:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16"><line x1="19" y1="12" x2="5" y2="12"/><polyline points="12 19 5 12 12 5"/></svg>,
  Save:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><path d="M19 21H5a2 2 0 01-2-2V5a2 2 0 012-2h11l5 5v11a2 2 0 01-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>,
  Play:    () => <svg viewBox="0 0 24 24" fill="currentColor" width="13" height="13"><polygon points="5 3 19 12 5 21 5 3"/></svg>,
  Plus:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" width="14" height="14"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>,
  Trash:   () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="13" height="13"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6M14 11v6M9 6V4h6v2"/></svg>,
  Copy:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="13" height="13"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>,
  Eye:     () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="13" height="13"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>,
  X:       () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>,
  Refresh: () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 11-2.12-9.36L23 10"/></svg>,
  Link:    () => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="13" height="13"><path d="M10 13a5 5 0 007.54.54l3-3a5 5 0 00-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 00-7.54-.54l-3 3a5 5 0 007.07 7.07l1.71-1.71"/></svg>,
};

// ── ETL Node component ────────────────────────────────────────────────────────
function ETLNode({ id, data }) {
  const bg       = nodeBg(data.type);
  const isInput  = data.type === "input_dataset";
  const isOutput = data.type === "output_dataset";
  const isUtil   = !isInput && !isOutput;

  // Columns that come INTO this node from upstream
  const upstreamCols = data.upstreamColumns || [];
  const outputCols   = data.outputColumns   || [];
  const isConnected  = data.isConnected || false;

  const configured = data.config && Object.keys(data.config).length > 0 &&
    (isInput ? !!data.config.dataset : isOutput ? !!data.config.outputName : true);

  return (
    <div style={{
      background: C.white, border: `2px solid ${bg}`, borderRadius: 10,
      minWidth: 190, boxShadow: `0 4px 16px ${bg}22`,
      fontFamily: "'DM Sans',sans-serif", overflow: "hidden",
      outline: data.selected ? `3px solid ${bg}88` : "none",
    }}>
      {!isInput  && <Handle type="target" position={Position.Left}  style={{ background: bg, width: 10, height: 10, border: `2px solid ${C.white}` }} />}
      {!isOutput && <Handle type="source" position={Position.Right} style={{ background: bg, width: 10, height: 10, border: `2px solid ${C.white}` }} />}

      {/* Header */}
      <div style={{ background: bg, padding: "6px 8px", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <span style={{ color: C.white, fontSize: 11, fontWeight: 700, flex: 1 }}>
          {isInput ? "📥 " : isOutput ? "📤 " : ""}{data.label}
        </span>
        <div style={{ display: "flex", gap: 2 }}>
          {/* Preview button */}
          <button onClick={() => data.onPreview(id)} title="Preview"
            style={{ background: "rgba(255,255,255,0.2)", border: "none", borderRadius: 4, width: 20, height: 20, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", color: C.white }}>
            <Ic.Eye />
          </button>
          <button onClick={() => data.onDuplicate(id)} title="Duplicate"
            style={{ background: "rgba(255,255,255,0.2)", border: "none", borderRadius: 4, width: 20, height: 20, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", color: C.white }}>
            <Ic.Copy />
          </button>
          <button onClick={() => data.onDelete(id)} title="Delete"
            style={{ background: "rgba(255,255,255,0.2)", border: "none", borderRadius: 4, width: 20, height: 20, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", color: C.white }}>
            <Ic.Trash />
          </button>
        </div>
      </div>

      {/* Body */}
      <div style={{ padding: "8px 10px" }}>

        {/* Input dataset */}
        {isInput && (
          data.config?.dataset ? (
            <div style={{ background: C.blueTint, borderRadius: 6, padding: "5px 8px" }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: C.blue }}>{data.config.dataset.name}</div>
              <div style={{ fontSize: 9, color: C.g400 }}>{data.config.dataset.row_count?.toLocaleString()} rows · {outputCols.length} cols</div>
            </div>
          ) : (
            <button onClick={() => data.onConfigure(id)} style={{ width: "100%", padding: "5px 0", borderRadius: 6, border: `1px dashed ${C.blue}`, background: C.blueTint, color: C.blue, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>
              + Select Dataset
            </button>
          )
        )}

        {/* Output dataset */}
        {isOutput && (
          data.config?.outputName ? (
            <div style={{ background: C.greenTint, borderRadius: 6, padding: "5px 8px" }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: C.green }}>warehouse.{data.config.outputName}</div>
              <div style={{ fontSize: 9, color: C.g400 }}>{upstreamCols.length} cols from upstream</div>
            </div>
          ) : (
            <button onClick={() => data.onConfigure(id)} style={{ width: "100%", padding: "5px 0", borderRadius: 6, border: `1px dashed ${C.green}`, background: C.greenTint, color: C.green, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>
              + Configure Output
            </button>
          )
        )}

        {/* Utility node */}
        {isUtil && (
          <div>
            {/* Connection status */}
            {!isConnected ? (
              <div style={{ fontSize: 10, color: C.g400, textAlign: "center", padding: "3px 0", fontStyle: "italic" }}>
                ← Connect to upstream node
              </div>
            ) : !configured ? (
              <button onClick={() => data.onConfigure(id)} style={{ width: "100%", padding: "5px 0", borderRadius: 6, border: `1px dashed ${C.gold}`, background: C.goldTint, color: C.gold, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>
                + Configure ({upstreamCols.length} cols available)
              </button>
            ) : (
              <div>
                <div style={{ background: C.greenTint, borderRadius: 6, padding: "4px 8px", marginBottom: 4, fontSize: 10, color: C.green, fontWeight: 600 }}>
                  ✓ Configured
                </div>
                {/* Show output column count */}
                <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: C.g400 }}>
                  <span>In: {upstreamCols.length} cols</span>
                  <span>→</span>
                  <span>Out: {outputCols.length} cols</span>
                </div>
                {/* Show output columns preview */}
                {outputCols.length > 0 && outputCols.length <= 6 && (
                  <div style={{ marginTop: 4, display: "flex", flexWrap: "wrap", gap: 2 }}>
                    {outputCols.map(c => (
                      <span key={c} style={{ fontSize: 8, padding: "1px 4px", borderRadius: 3, background: C.blueTint2, color: C.blue, fontWeight: 600, fontFamily: "monospace" }}>{c}</span>
                    ))}
                  </div>
                )}
                {outputCols.length > 6 && (
                  <div style={{ marginTop: 4, fontSize: 9, color: C.g400 }}>
                    {outputCols.slice(0,4).join(", ")}… +{outputCols.length - 4} more
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

const nodeTypes = { etlNode: ETLNode };

// ── WorkflowEditor ────────────────────────────────────────────────────────────
export default function WorkflowEditor({ workflow, datasets, onSave, onBack, toast }) {
  const [nodes, setNodes, onNodesChange] = useNodesState(workflow.nodes || []);
  const [edges, setEdges, onEdgesChange] = useEdgesState(workflow.edges || []);
  const [wfTab, setWfTab]       = useState("datasets");
  const [counter, setCounter]   = useState(100);
  const [running, setRunning]   = useState(false);
  const [dagStatus, setDagStatus] = useState(null);
  const [taskStates, setTaskStates] = useState({});

  // Modals
  const [configModal, setConfigModal]   = useState(null); // input/output config
  const [utilNode, setUtilNode]         = useState(null); // utility config
  const [previewNode, setPreviewNode]   = useState(null); // preview panel

  // Input/Output config forms
  const [inputConfig,  setInputConfig]  = useState({ datasetId: "" });
  const [outputConfig, setOutputConfig] = useState({ outputName: "", description: "" });

  // ── Column propagation ─────────────────────────────────────────
  // Recompute every time nodes or edges change
  const columnMap = useMemo(() => computeNodeColumns(nodes, edges), [nodes, edges]);

  // Inject computed columns back into node data
  useEffect(() => {
    setNodes(ns => ns.map(n => {
      const upstreamCols = (() => {
        const sourceEdge = edges.find(e => e.target === n.id);
        if (!sourceEdge) return [];
        return columnMap[sourceEdge.source] || [];
      })();
      const outputCols   = columnMap[n.id] || [];
      const isConnected  = edges.some(e => e.target === n.id);

      if (
        JSON.stringify(n.data.upstreamColumns) !== JSON.stringify(upstreamCols) ||
        JSON.stringify(n.data.outputColumns)   !== JSON.stringify(outputCols)   ||
        n.data.isConnected !== isConnected
      ) {
        return { ...n, data: { ...n.data, upstreamColumns: upstreamCols, outputColumns: outputCols, isConnected } };
      }
      return n;
    }));
  }, [columnMap, edges]);

  // ── Node callbacks ─────────────────────────────────────────────
  const makeCallbacks = useCallback((nodeId, type) => ({
    onDelete:    (nid) => setNodes(ns => ns.filter(n => n.id !== nid)),
    onDuplicate: (nid) => setNodes(ns => {
      const o = ns.find(n => n.id === nid); if (!o) return ns;
      return [...ns, { ...o, id: `n${Date.now()}`, position: { x: o.position.x + 30, y: o.position.y + 30 } }];
    }),
    onConfigure: (nid) => {
      const isIO = ["input_dataset","output_dataset"].includes(type);
      if (isIO) {
        setConfigModal({ nodeId: nid, type });
      } else {
        setNodes(ns => { const n = ns.find(x => x.id === nid); if (n) setUtilNode(n); return ns; });
      }
    },
    onPreview: (nid) => {
      const n = nodes.find(x => x.id === nid);
      if (n) setPreviewNode(n);
    },
  }), [nodes, setNodes]);

  const addNode = useCallback((item) => {
    const id = `n${counter}`;
    setCounter(c => c + 1);
    const callbacks = {
      onDelete:    (nid) => setNodes(ns => ns.filter(n => n.id !== nid)),
      onDuplicate: (nid) => setNodes(ns => {
        const o = ns.find(n => n.id === nid); if (!o) return ns;
        return [...ns, { ...o, id: `n${Date.now()}`, position: { x: o.position.x + 30, y: o.position.y + 30 } }];
      }),
      onConfigure: (nid) => {
        const isIO = ["input_dataset","output_dataset"].includes(item.type);
        if (isIO) { setConfigModal({ nodeId: nid, type: item.type }); }
        else { setNodes(ns => { const n = ns.find(x => x.id === nid); if (n) setUtilNode(n); return ns; }); }
      },
      onPreview: (nid) => {
        setNodes(ns => { const n = ns.find(x => x.id === nid); if (n) setPreviewNode(n); return ns; });
      },
    };

    setNodes(ns => [...ns, {
      id, type: "etlNode",
      position: { x: 80 + (counter % 5) * 220, y: 60 + Math.floor(counter / 5) * 160 },
      data: { label: item.label, type: item.type, config: null, columns: [], upstreamColumns: [], outputColumns: [], isConnected: false, ...callbacks },
    }]);
  }, [counter, setNodes]);

  // Rebuild callbacks after node load (callbacks don't serialize)
  useEffect(() => {
    setNodes(ns => ns.map(n => ({
      ...n, data: { ...n.data, ...makeCallbacks(n.id, n.data.type) }
    })));
  }, []);

  const onConnect = useCallback((p) => {
    setEdges(es => addEdge({ ...p, animated: true, style: { stroke: C.blue, strokeWidth: 2 } }, es));
  }, [setEdges]);

  // ── Config handlers ────────────────────────────────────────────
  const applyInputConfig = async () => {
    const ds = datasets.find(d => d.id === parseInt(inputConfig.datasetId));
    if (!ds) return;

    // Load columns if not already loaded
    let dsWithCols = ds;
    if (!ds.columns || ds.columns.length === 0) {
      try {
        const r = await api.get(`/datasets/${ds.id}/preview?limit=1`);
        dsWithCols = { ...ds, columns: r.data.columns || [] };
      } catch {}
    }

    setNodes(ns => ns.map(n => n.id === configModal.nodeId ? {
      ...n, data: {
        ...n.data,
        config: { dataset: dsWithCols },
        columns: dsWithCols.columns || [],
        outputColumns: dsWithCols.columns || [],
        ...makeCallbacks(n.id, "input_dataset"),
      }
    } : n));
    setConfigModal(null);
    toast(`Dataset "${ds.name}" assigned`, "success");
  };

  const applyOutputConfig = () => {
    if (!outputConfig.outputName.trim()) return toast("Output name is required", "error");
    setNodes(ns => ns.map(n => n.id === configModal.nodeId ? {
      ...n, data: { ...n.data, config: { ...outputConfig }, ...makeCallbacks(n.id, "output_dataset") }
    } : n));
    setConfigModal(null);
    toast("Output configured", "success");
  };

  // ── Save ───────────────────────────────────────────────────────
  const handleSave = () => {
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
  };

  // ── Run ────────────────────────────────────────────────────────
  const handleRun = async () => {
    const inputNodes  = nodes.filter(n => n.data.type === "input_dataset");
    const outputNodes = nodes.filter(n => n.data.type === "output_dataset");

    if (!inputNodes.length)  return toast("Add an Input Dataset node first", "error");
    if (!outputNodes.length) return toast("Add an Output Dataset node first", "error");
    if (!inputNodes[0].data.config?.dataset)  return toast("Configure Input Dataset node first", "error");
    if (!outputNodes[0].data.config?.outputName) return toast("Configure Output Dataset node first", "error");

    const input  = inputNodes[0].data.config.dataset;
    const output = outputNodes[0].data.config;

    // Build transforms in order (topological)
    const { topoSort } = await import("./NodePropagation").catch(() => ({ topoSort: null }));
    const transformNodes = nodes.filter(n => !["input_dataset","output_dataset"].includes(n.data.type));
    const transforms = transformNodes.map(n => ({ type: n.data.type, config: n.data.config }));

    const inputTable = input.table_name
      ? `staging.${input.table_name}`
      : `staging.${input.name.replace(/\.[^.]+$/, '').toLowerCase().replace(/[\s-]+/g, '_')}`;

    setRunning(true);
    try {
      const r = await api.post("/pipelines/run", {
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

      // Poll via backend
      const poll = setInterval(async () => {
        try {
          const s = await api.get(`/pipelines/runs/${run_id}/dag-status`);
          const { state, tasks } = s.data;
          setDagStatus(s.data);
          setTaskStates(tasks || {});

          if (["success","failed"].includes(state)) {
            clearInterval(poll);
            setRunning(false);
            toast(state === "success" ? "Pipeline completed!" : "Pipeline failed", state === "success" ? "success" : "error");
            if (state === "success") handleSave();
          }
        } catch {}
      }, 6000);

    } catch (e) {
      setRunning(false);
      toast(e.response?.data?.detail || "Failed to trigger pipeline", "error");
    }
  };

  // ── Status colors ──────────────────────────────────────────────
  const STATUS_BG     = { success: C.greenTint, running: C.blueTint, failed: C.redTint, queued: C.goldTint, none: C.g50 };
  const STATUS_BORDER = { success: C.green, running: C.blue, failed: C.red, queued: C.gold, none: C.g300 };

  useEffect(() => {
    if (!Object.keys(taskStates).length) return;
    setNodes(ns => ns.map(n => {
      const state = taskStates[n.id] || "none";
      if (!STATUS_BG[state]) return n;
      return { ...n, style: { background: STATUS_BG[state], borderColor: STATUS_BORDER[state] } };
    }));
  }, [taskStates]);

  // Get upstream columns for utility config modal
  const utilNodeUpstreamCols = useMemo(() => {
    if (!utilNode) return [];
    return getUpstreamColumns(utilNode.id, nodes, edges);
  }, [utilNode, nodes, edges]);

  return (
    <div style={{ display: "flex", flex: 1, overflow: "hidden", flexDirection: "column", fontFamily: "'DM Sans',sans-serif" }}>

      {/* Top bar */}
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
          <button onClick={handleRun} disabled={running} style={{ display: "flex", alignItems: "center", gap: 5, padding: "7px 16px", borderRadius: 7, background: running ? C.g300 : `linear-gradient(90deg,${C.blue},${C.blueMid})`, border: "none", cursor: running ? "not-allowed" : "pointer", fontSize: 12, fontWeight: 700, color: C.white }}>
            {running ? <>⟳ Running…</> : <><Ic.Play /> Run Pipeline</>}
          </button>
        </div>
      </div>

      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        {/* Left panel */}
        <div style={{ width: 210, background: C.white, borderRight: `1px solid ${C.g200}`, display: "flex", flexDirection: "column", flexShrink: 0 }}>
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
                <span style={{ marginLeft: "auto", color: C.g300 }}><Ic.Plus /></span>
              </button>
            ))}
          </div>

          <div style={{ padding: 12, borderTop: `1px solid ${C.g200}` }}>
            <button onClick={() => { setNodes([]); setEdges([]); setDagStatus(null); setTaskStates({}); }}
              style={{ width: "100%", padding: "7px 0", background: C.g50, border: `1px solid ${C.g200}`, borderRadius: 7, color: C.g500, fontSize: 12, fontWeight: 600, cursor: "pointer" }}>
              Clear Canvas
            </button>
          </div>
        </div>

        {/* Canvas area */}
        <div style={{ flex: 1, position: "relative", overflow: "hidden" }}>
          {nodes.length === 0 && (
            <div style={{ position: "absolute", inset: 0, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", pointerEvents: "none", zIndex: 5 }}>
              <div style={{ fontSize: 36, marginBottom: 10 }}>⚡</div>
              <div style={{ fontWeight: 700, color: C.g400, marginBottom: 4 }}>Canvas is empty</div>
              <div style={{ fontSize: 12, color: C.g300 }}>Start with an Input Dataset node</div>
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
              <div style={{ background: C.white, border: `1px solid ${C.g200}`, borderRadius: 8, padding: "4px 10px", fontSize: 11, color: C.g500 }}>
                {nodes.length} nodes · {edges.length} edges
              </div>
            </Panel>
          </ReactFlow>

          {/* Preview panel — slides in from right */}
          {previewNode && (
            <NodePreviewPanel
              node={previewNode}
              columns={columnMap[previewNode.id] || []}
              onClose={() => setPreviewNode(null)}
            />
          )}
        </div>
      </div>

      {/* ── Input Dataset Config Modal ── */}
      {configModal?.type === "input_dataset" && (
        <div style={{ position: "fixed", inset: 0, background: "rgba(11,30,61,.6)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000 }} onClick={() => setConfigModal(null)}>
          <div style={{ background: C.white, borderRadius: 16, width: 460, maxWidth: "92vw", boxShadow: "0 24px 64px rgba(0,0,0,.22)", overflow: "hidden" }} onClick={e => e.stopPropagation()}>
            <div style={{ background: `linear-gradient(90deg,${C.navy},${C.navyLight})`, padding: "16px 20px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <span style={{ color: C.white, fontWeight: 800, fontSize: 15 }}>Configure Input Dataset</span>
              <button onClick={() => setConfigModal(null)} style={{ background: "rgba(255,255,255,.1)", border: "none", borderRadius: 6, padding: "4px 8px", cursor: "pointer", color: C.white }}><Ic.X /></button>
            </div>
            <div style={{ padding: 22 }}>
              <div style={{ fontSize: 12, fontWeight: 700, color: C.g600, marginBottom: 8 }}>Select Dataset</div>
              {datasets.length === 0 ? (
                <div style={{ background: C.redTint, borderRadius: 8, padding: 12, fontSize: 12, color: C.red }}>No datasets available. Upload one in Data Source first.</div>
              ) : (
                <select value={inputConfig.datasetId} onChange={e => setInputConfig({ datasetId: e.target.value })}
                  style={{ width: "100%", padding: "10px 12px", borderRadius: 8, border: `1px solid ${C.g200}`, fontSize: 13, color: C.g700, outline: "none" }}>
                  <option value="">— Choose a dataset —</option>
                  {datasets.map(d => (
                    <option key={d.id} value={d.id}>{d.name} ({d.row_count?.toLocaleString() || "?"} rows · {d.type})</option>
                  ))}
                </select>
              )}

              {inputConfig.datasetId && (() => {
                const ds = datasets.find(d => d.id === parseInt(inputConfig.datasetId));
                return ds ? (
                  <div style={{ marginTop: 12, background: C.blueTint, borderRadius: 8, padding: 12 }}>
                    <div style={{ fontSize: 11, fontWeight: 700, color: C.blue, marginBottom: 6 }}>{ds.name}</div>
                    <div style={{ display: "flex", gap: 12, fontSize: 11, color: C.blue, opacity: 0.8, marginBottom: 8 }}>
                      <span>{ds.row_count?.toLocaleString()} rows</span>
                      <span>{ds.file_size}</span>
                      <span>{ds.type}</span>
                    </div>
                    {ds.columns?.length > 0 && (
                      <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
                        {ds.columns.map(c => (
                          <span key={c} style={{ fontSize: 10, padding: "2px 6px", borderRadius: 4, background: C.blue + "22", color: C.blue, fontWeight: 600, fontFamily: "monospace" }}>{c}</span>
                        ))}
                      </div>
                    )}
                  </div>
                ) : null;
              })()}

              <div style={{ display: "flex", gap: 8, marginTop: 16 }}>
                <button onClick={() => setConfigModal(null)} style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: `1px solid ${C.g200}`, background: C.white, color: C.g600, fontSize: 13, fontWeight: 600, cursor: "pointer" }}>Cancel</button>
                <button onClick={applyInputConfig} disabled={!inputConfig.datasetId} style={{ flex: 1, padding: "9px 0", borderRadius: 8, background: !inputConfig.datasetId ? C.g300 : C.blue, border: "none", color: C.white, fontSize: 13, fontWeight: 700, cursor: !inputConfig.datasetId ? "not-allowed" : "pointer" }}>
                  Apply Dataset
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ── Output Dataset Config Modal ── */}
      {configModal?.type === "output_dataset" && (
        <div style={{ position: "fixed", inset: 0, background: "rgba(11,30,61,.6)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000 }} onClick={() => setConfigModal(null)}>
          <div style={{ background: C.white, borderRadius: 16, width: 460, maxWidth: "92vw", boxShadow: "0 24px 64px rgba(0,0,0,.22)", overflow: "hidden" }} onClick={e => e.stopPropagation()}>
            <div style={{ background: `linear-gradient(90deg,${C.navy},${C.navyLight})`, padding: "16px 20px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <span style={{ color: C.white, fontWeight: 800, fontSize: 15 }}>Configure Output Dataset</span>
              <button onClick={() => setConfigModal(null)} style={{ background: "rgba(255,255,255,.1)", border: "none", borderRadius: 6, padding: "4px 8px", cursor: "pointer", color: C.white }}><Ic.X /></button>
            </div>
            <div style={{ padding: 22 }}>
              <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
                <div>
                  <div style={{ fontSize: 12, fontWeight: 700, color: C.g600, marginBottom: 6 }}>Output Table Name *</div>
                  <input value={outputConfig.outputName}
                    onChange={e => setOutputConfig(o => ({ ...o, outputName: e.target.value.toLowerCase().replace(/\s+/g,"_").replace(/[^a-z0-9_]/g,"") }))}
                    placeholder="e.g. sales_clean_2026"
                    style={{ width: "100%", padding: "10px 12px", borderRadius: 8, border: `1.5px solid ${outputConfig.outputName ? C.green : C.g200}`, fontSize: 13, boxSizing: "border-box", outline: "none", color: C.g700, fontFamily: "monospace" }} />
                  <div style={{ fontSize: 10, color: C.g400, marginTop: 4 }}>Will be saved as warehouse.{outputConfig.outputName || "your_table"}</div>
                </div>
                <div>
                  <div style={{ fontSize: 12, fontWeight: 700, color: C.g600, marginBottom: 6 }}>Description</div>
                  <textarea value={outputConfig.description} onChange={e => setOutputConfig(o => ({ ...o, description: e.target.value }))}
                    placeholder="Describe this output..." rows={3}
                    style={{ width: "100%", padding: "10px 12px", borderRadius: 8, border: `1px solid ${C.g200}`, fontSize: 13, boxSizing: "border-box", outline: "none", color: C.g700, resize: "none" }} />
                </div>

                {/* Show columns that will be output */}
                {(() => {
                  const outputNodeId = configModal.nodeId;
                  const sourceEdge   = edges.find(e => e.target === outputNodeId);
                  const upCols       = sourceEdge ? (columnMap[sourceEdge.source] || []) : [];
                  return upCols.length > 0 ? (
                    <div style={{ background: C.greenTint, borderRadius: 8, padding: 12 }}>
                      <div style={{ fontSize: 11, fontWeight: 700, color: C.green, marginBottom: 6 }}>
                        {upCols.length} columns will be saved:
                      </div>
                      <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
                        {upCols.map(c => (
                          <span key={c} style={{ fontSize: 10, padding: "2px 6px", borderRadius: 4, background: C.green + "22", color: C.green, fontWeight: 600, fontFamily: "monospace" }}>{c}</span>
                        ))}
                      </div>
                    </div>
                  ) : null;
                })()}
              </div>

              <div style={{ display: "flex", gap: 8, marginTop: 18 }}>
                <button onClick={() => setConfigModal(null)} style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: `1px solid ${C.g200}`, background: C.white, color: C.g600, fontSize: 13, fontWeight: 600, cursor: "pointer" }}>Cancel</button>
                <button onClick={applyOutputConfig} disabled={!outputConfig.outputName} style={{ flex: 1, padding: "9px 0", borderRadius: 8, background: !outputConfig.outputName ? C.g300 : C.green, border: "none", color: C.white, fontSize: 13, fontWeight: 700, cursor: !outputConfig.outputName ? "not-allowed" : "pointer" }}>
                  Save Output Config
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ── Utility Config Modal ── */}
      {utilNode && (
        <UtilityConfigModal
          node={utilNode}
          columns={utilNodeUpstreamCols}
          allNodes={nodes}
          onSave={(nodeId, cfg) => {
            setNodes(ns => ns.map(n => n.id === nodeId ? { ...n, data: { ...n.data, config: cfg } } : n));
            setUtilNode(null);
            toast("Node configured!", "success");
          }}
          onClose={() => setUtilNode(null)}
        />
      )}
    </div>
  );
}
