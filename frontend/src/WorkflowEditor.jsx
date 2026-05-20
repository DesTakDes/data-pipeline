// WorkflowEditor.jsx — Config sidebar (no popups)
import { useState, useCallback, useEffect, useMemo, useRef } from "react";
import {
  ReactFlow, Background, Controls, MiniMap, addEdge,
  useNodesState, useEdgesState, Panel, Handle, Position,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { UtilityConfigModal } from "./UtilityConfigs";
import { computeNodeColumns, getUpstreamColumns } from "./NodePropagation";
import axios from "axios";
import { computeNodeColumns } from "./NodePropagation"; 

const api     = axios.create({ baseURL: "/api" });
const airflow = axios.create({
  baseURL: "/airflow-api/api/v1",
  headers: { Authorization: "Basic " + btoa("admin:admin123") },
});

// ── Colors ────────────────────────────────────────────────────────────────────
const C = {
  navy:"#0B1E3D", navyLight:"#1A3A6B",
  blue:"#1D6FEB", blueMid:"#3B82F6", blueTint:"#EFF6FF", blueTint2:"#DBEAFE",
  gold:"#F59E0B", goldTint:"#FFFBEB",
  white:"#FFFFFF", g50:"#F8FAFC", g100:"#F1F5F9", g200:"#E2E8F0",
  g300:"#CBD5E1", g400:"#94A3B8", g500:"#64748B", g600:"#475569", g700:"#334155",
  green:"#16A34A", greenTint:"#DCFCE7",
  red:"#DC2626", redTint:"#FEE2E2", orange:"#EA580C",
};

// ── Node palette ──────────────────────────────────────────────────────────────
const NODE_PALETTE = {
  datasets: {
    label: "Datasets", color: C.blue,
    items: [
      { label: "Input Dataset",  type: "input_dataset"  },
      { label: "Output Dataset", type: "output_dataset" },
    ],
  },
  utility: {
    label: "Utility", color: C.gold,
    items: [
      { label: "Select Column",        type: "select_col"  },
      { label: "Rename Columns",       type: "rename_col"  },
      { label: "Drop Columns",         type: "drop_col"    },
      { label: "Add Constant",         type: "add_const"   },
      { label: "Set Column Value",     type: "set_val"     },
      { label: "Value Mapper",         type: "val_mapper"  },
      { label: "Change Data Type",     type: "change_type" },
      { label: "Fill NULL",            type: "fill_null"   },
      { label: "Filter Rows",          type: "filter_rows" },
      { label: "Order Table",          type: "order_table" },
      { label: "Group By & Aggregate", type: "group_agg"   },
      { label: "Join Data",            type: "join_data"   },
      { label: "PySpark Node",         type: "pyspark"     },
    ],
  },
};

const NODE_BG = {
  input_dataset: C.blue, output_dataset: C.green,
  filter_rows: C.blueMid, fill_null: C.blueMid,
  group_agg: C.navyLight, join_data: C.blue,
  pyspark: C.orange, drop_col: C.red,
  default: C.gold,
};
const nbg = (t) => NODE_BG[t] || NODE_BG.default;

const generateSparkPreview = (node) => {
  if (!node) return "";
  const { type, config } = node.data;

  switch (type) {
    case "input_dataset":  return `df = spark.read.table("${config.dataset?.name || '...'}")`;
    case "output_dataset": return `df.write.mode("overwrite").saveAsTable("warehouse.${config.outputName || '...'}")`;
    case "filter_rows":    return `df = df.filter(expr("${config.formula || '1=1'}"))`;
    case "select_col":     return `df = df.select(${config.columns?.map(c=>`"${c}"`).join(", ") || "*"})`;
    case "drop_col":       return `df = df.drop(${config.columns?.map(c=>`"${c}"`).join(", ") || '""'})`;
    case "rename_col":     return Object.entries(config.renames || {}).map(([o,n]) => `df = df.withColumnRenamed("${o}", "${n}")`).join("\n");
    case "add_const":      return `df = df.withColumn("${config.name || 'new_col'}", lit("${config.value || ''}").cast("${config.dtype || 'TEXT'}"))`;
    case "group_agg":      return `df = df.groupBy(${config.groupCols?.map(c=>`"${c}"`).join(", ") || '""'})\n  .agg(\n    ${config.aggCols?.map(a=>`expr("${a.func}(\`${a.col}\`)").alias("${a.alias}")`).join(",\n    ") || ""}\n  )`;
    case "join_data":      return `df = df.join(right_df, df["${config.leftCol || ''}"] == right_df["${config.rightCol || ''}"], "${(config.joinType||'INNER').replace(' JOIN','')}")`;
    case "pyspark":        return config.code || "# Custom code";
    default:               return `# Transform code for ${type} will be generated here.`;
  }
};

// 2. GRAPH TRAVERSAL HELPER
// ==========================================
// Fungsi ini melacak dari node saat ini mundur ke belakang 
// untuk melihat apakah ada input dan mengambil nama kolomnya.
const getUpstreamData = (nodeId, nodes, edges) => {
  const incomingEdges = edges.filter(e => e.target === nodeId);
  if (incomingEdges.length === 0) return { connected: false, columns: [] };

  const parentId = incomingEdges[0].source;
  const parentNode = nodes.find(n => n.id === parentId);
  
  if (!parentNode) return { connected: false, columns: [] };

  if (parentNode.data.type === "input_dataset") {
    // Jika parent adalah input, ambil kolom aslinya
    const cols = parentNode.data.config?.dataset?.columns || ["id", "name", "created_at", "amount"];
    return { connected: true, columns: cols };
  } else {
    // Jika parent adalah utility, telusuri terus ke atas (rekursif)
    return getUpstreamData(parentNode.id, nodes, edges);
  }
};

// 4. CUSTOM NODE COMPONENT
function ETLNode({ id, data, selected }) {
  const nodeDef = NODE_TYPES.find(n => n.type === data.type) || { color: C.gray };
  const isInput  = data.type === "input_dataset";
  const isOutput = data.type === "output_dataset";

  return (
    <div style={{
      background: "#fff",
      border: `2px solid ${selected ? nodeDef.color : "#E2E8F0"}`,
      borderRadius: "8px", minWidth: "150px",
      boxShadow: selected ? `0 0 0 4px ${nodeDef.color}22` : "0 2px 4px rgba(0,0,0,0.05)"
    }}>
      {!isInput && <Handle type="target" position={Position.Left} style={{ background: nodeDef.color }} />}
      {!isOutput && <Handle type="source" position={Position.Right} style={{ background: nodeDef.color }} />}

      <div style={{ background: nodeDef.color, padding: "8px", borderRadius: "5px 5px 0 0", color: "#fff", fontSize: "11px", fontWeight: "bold", textAlign: "center" }}>
        {data.label}
      </div>
      <div style={{ padding: "10px", fontSize: "10px", textAlign: "center", color: C.dark }}>
        {data.isConnected ? (
          <span style={{ color: C.green }}>🔗 Connected</span>
        ) : isInput ? (
          <span>Data Source</span>
        ) : (
          <span style={{ color: C.red }}>⚠ Unconnected</span>
        )}
      </div>
    </div>
  );
}

const customNodeTypes = { etlNode: CustomNode };

// ── Icons ─────────────────────────────────────────────────────────────────────
const Ic = {
  Back:    ()=><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="16" height="16"><line x1="19" y1="12" x2="5" y2="12"/><polyline points="12 19 5 12 12 5"/></svg>,
  Save:    ()=><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><path d="M19 21H5a2 2 0 01-2-2V5a2 2 0 012-2h11l5 5v11a2 2 0 01-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>,
  Play:    ()=><svg viewBox="0 0 24 24" fill="currentColor" width="13" height="13"><polygon points="5 3 19 12 5 21 5 3"/></svg>,
  Plus:    ()=><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" width="14" height="14"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>,
  Trash:   ()=><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="13" height="13"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14H6L5 6"/><path d="M10 11v6M14 11v6M9 6V4h6v2"/></svg>,
  Copy:    ()=><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="13" height="13"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>,
  Config:  ()=><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="13" height="13"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z"/></svg>,
  X:       ()=><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="14" height="14"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>,
  Check:   ()=><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" width="13" height="13"><polyline points="20 6 9 17 4 12"/></svg>,
};

// ── ETL Node ──────────────────────────────────────────────────────────────────
function ETLNode({ id, data, selected }) {
  const bg = nbg(data.type);
  const isInput  = data.type === "input_dataset";
  const isOutput = data.type === "output_dataset";
  const isUtil   = !isInput && !isOutput;

  return (
    <div onClick={() => data.onSelect(id)} style={{
        background: C.white, border: `2px solid ${selected ? bg : C.g200}`,
        borderRadius: 10, minWidth: 190, cursor: "pointer",
        boxShadow: selected ? `0 0 0 3px ${bg}33, 0 4px 16px ${bg}22` : `0 2px 8px rgba(0,0,0,.08)`,
      }}>
      {!isInput  && <Handle type="target" position={Position.Left} style={{ background: bg, width:10, height:10 }} />}
      {!isOutput && <Handle type="source" position={Position.Right} style={{ background: bg, width:10, height:10 }} />}

      <div style={{ background: bg, padding: "6px 8px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <span style={{ color: C.white, fontSize: 11, fontWeight: 700 }}>
          {isInput ? "📥 " : isOutput ? "📤 " : ""}{data.label}
        </span>
        <div style={{ display: "flex", gap: 2 }}>
          <button onClick={e => { e.stopPropagation(); data.onDelete(id); }} style={{ background: "transparent", border: "none", color: C.white, cursor: "pointer" }}><Ic.Trash /></button>
        </div>
      </div>
      <div style={{ padding: "8px 10px", fontSize: 10, color: C.g500 }}>
        {Object.keys(data.config || {}).length > 0 ? <span style={{color: C.green}}>✓ Configured</span> : "Click to configure →"}
      </div>
    </div>
  );
}
const nodeTypes = { etlNode: ETLNode };

// ── Sidebar Config Panels ─────────────────────────────────────────────────────

function SidebarLabel({ children, required }) {
  return <div style={{ fontSize: 11, fontWeight: 700, color: C.g600, marginBottom: 5 }}>{children}{required && <span style={{ color: C.red }}>*</span>}</div>;
}
function SidebarInput({ value, onChange, placeholder, mono = false }) {
  return <input value={value||""} onChange={e => onChange(e.target.value)} placeholder={placeholder} style={{ width: "100%", padding: "8px", borderRadius: 6, border: `1px solid ${C.g200}`, fontSize: 12, fontFamily: mono ? "monospace" : "inherit" }} />;
}
function SidebarSelect({ value, onChange, options, placeholder }) {
  return (
    <select value={value} onChange={e => onChange(e.target.value)}
      style={{ width: "100%", padding: "8px 10px", borderRadius: 7, border: `1px solid ${C.g200}`, fontSize: 12, outline: "none", color: value ? C.g700 : C.g400, background: C.white }}>
      {placeholder && <option value="">{placeholder}</option>}
      {options.map(o => typeof o === "string" ? <option key={o}>{o}</option> : <option key={o.value} value={o.value}>{o.label}</option>)}
    </select>
  );
}

// Input Dataset sidebar
function InputDatasetSidebar({ node, datasets, onSave }) {
  const [datasetId, setDatasetId] = useState(node.data?.config?.dataset?.id?.toString() || "");
  const ds = datasets.find(d => d.id === parseInt(datasetId));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      <div>
        <SidebarLabel required>Select Dataset</SidebarLabel>
        {datasets.length === 0 ? (
          <div style={{ background: C.redTint, borderRadius: 7, padding: 10, fontSize: 11, color: C.red }}>
            No datasets available. Upload one in Data Source.
          </div>
        ) : (
          <SidebarSelect
            value={datasetId} onChange={setDatasetId}
            options={datasets.map(d => ({ value: d.id.toString(), label: `${d.name} (${d.row_count?.toLocaleString() || "?"} rows)` }))}
            placeholder="— Choose dataset —"
          />
        )}
      </div>

      {ds && (
        <div style={{ background: C.blueTint, borderRadius: 8, padding: 12 }}>
          <div style={{ fontWeight: 700, fontSize: 12, color: C.blue, marginBottom: 6 }}>{ds.name}</div>
          <div style={{ display: "flex", gap: 10, fontSize: 11, color: C.blue, opacity: 0.8, marginBottom: 8 }}>
            <span>{ds.row_count?.toLocaleString()} rows</span>
            <span>{ds.file_size}</span>
            <span>{ds.type}</span>
            {ds.is_parquet && <span style={{ background: C.blue, color: C.white, borderRadius: 4, padding: "1px 6px", fontSize: 9, fontWeight: 700 }}>PARQUET</span>}
          </div>
          {ds.columns?.length > 0 && (
            <div style={{ display: "flex", flexWrap: "wrap", gap: 3 }}>
              {ds.columns.map(c => (
                <span key={c} style={{ fontSize: 9, padding: "2px 5px", borderRadius: 3, background: `${C.blue}22`, color: C.blue, fontWeight: 600, fontFamily: "monospace" }}>{c}</span>
              ))}
            </div>
          )}
        </div>
      )}

      <button
        onClick={() => ds && onSave({ dataset: ds })}
        disabled={!datasetId || !ds}
        style={{
          padding: "9px 0", borderRadius: 8, border: "none",
          background: !datasetId ? C.g300 : C.blue,
          color: C.white, fontWeight: 700, fontSize: 13,
          cursor: !datasetId ? "not-allowed" : "pointer",
        }}
      >
        Apply Dataset
      </button>
    </div>
  );
}

// Output Dataset sidebar
function OutputDatasetSidebar({ node, upstreamCols, onSave }) {
  const [outputName,  setOutputName]  = useState(node.data?.config?.outputName  || "");
  const [description, setDescription] = useState(node.data?.config?.description || "");

  const handleName = (v) => setOutputName(v.toLowerCase().replace(/\s+/g,"_").replace(/[^a-z0-9_]/g,""));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      <div>
        <SidebarLabel required>Output Table Name</SidebarLabel>
        <SidebarInput value={outputName} onChange={handleName} placeholder="e.g. sales_clean_2026" mono />
        {outputName && (
          <div style={{ fontSize: 10, color: C.g400, marginTop: 4 }}>
            → <code style={{ fontFamily: "monospace", color: C.blue }}>warehouse.{outputName}</code>
          </div>
        )}
      </div>

      <div>
        <SidebarLabel>Description</SidebarLabel>
        <textarea
          value={description} onChange={e => setDescription(e.target.value)}
          placeholder="Describe this output..." rows={3}
          style={{ width: "100%", padding: "8px 10px", borderRadius: 7, border: `1px solid ${C.g200}`, fontSize: 12, boxSizing: "border-box", outline: "none", color: C.g700, resize: "none", fontFamily: "inherit" }}
        />
      </div>

      {upstreamCols.length > 0 && (
        <div>
          <SidebarLabel>Columns that will be saved ({upstreamCols.length})</SidebarLabel>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 3, background: C.greenTint, borderRadius: 7, padding: 8 }}>
            {upstreamCols.map(c => (
              <span key={c} style={{ fontSize: 9, padding: "2px 5px", borderRadius: 3, background: `${C.green}22`, color: C.green, fontWeight: 600, fontFamily: "monospace" }}>{c}</span>
            ))}
          </div>
        </div>
      )}

      <button
        onClick={() => outputName && onSave({ outputName, description })}
        disabled={!outputName}
        style={{
          padding: "9px 0", borderRadius: 8, border: "none",
          background: !outputName ? C.g300 : C.green,
          color: C.white, fontWeight: 700, fontSize: 13,
          cursor: !outputName ? "not-allowed" : "pointer",
        }}
      >
        Save Output Config
      </button>
    </div>
  );
}

// ── Config Sidebar Shell ──────────────────────────────────────────────────────
const SIDEBAR_TITLES = {
  input_dataset:  { title: "Input Dataset",         icon: "📥", color: C.blue  },
  output_dataset: { title: "Output Dataset",         icon: "📤", color: C.green },
  select_col:     { title: "Select Column",          icon: "🔍", color: C.gold  },
  rename_col:     { title: "Rename Columns",         icon: "✏️",  color: C.gold  },
  drop_col:       { title: "Drop Columns",           icon: "🗑️",  color: C.red   },
  add_const:      { title: "Add Constant",           icon: "➕", color: C.gold  },
  set_val:        { title: "Set Column Value",       icon: "🔄", color: C.gold  },
  val_mapper:     { title: "Value Mapper",           icon: "🗺️",  color: C.gold  },
  change_type:    { title: "Change Data Type",       icon: "🔀", color: C.gold  },
  fill_null:      { title: "Fill NULL",              icon: "⬛", color: C.blueMid },
  filter_rows:    { title: "Filter Rows",            icon: "🔎", color: C.blueMid },
  order_table:    { title: "Order Table",            icon: "📊", color: C.gold  },
  group_agg:      { title: "Group By & Aggregate",   icon: "📦", color: C.navyLight },
  join_data:      { title: "Join Data",              icon: "🔗", color: C.blue  },
  pyspark:        { title: "PySpark Node",           icon: "⚡", color: C.orange },
};

function ConfigSidebar({
  node, datasets, upstreamCols, allNodes,
  onSave, onClose, columnMap,
}) {
  if (!node) return null;

  const meta  = SIDEBAR_TITLES[node.data?.type] || { title: "Configure", icon: "⚙️", color: C.blue };
  const type  = node.data?.type;
  const isUtil = !["input_dataset","output_dataset"].includes(type);

  return (
    <div style={{
      width: 300, flexShrink: 0,
      background: C.white,
      borderLeft: `1px solid ${C.g200}`,
      display: "flex", flexDirection: "column",
      overflow: "hidden",
      fontFamily: "'DM Sans',sans-serif",
    }}>
      {/* Sidebar header */}
      <div style={{
        background: `linear-gradient(135deg, ${C.navy} 0%, ${meta.color}44 100%)`,
        padding: "14px 16px",
        display: "flex", justifyContent: "space-between", alignItems: "center",
        flexShrink: 0,
      }}>
        <div>
          <div style={{ color: C.white, fontWeight: 800, fontSize: 14 }}>
            {meta.icon} {meta.title}
          </div>
          <div style={{ color: "rgba(255,255,255,.5)", fontSize: 10, marginTop: 2 }}>
            {node.id} · {upstreamCols.length} upstream cols
          </div>
        </div>
        <button onClick={onClose} style={{ background: "rgba(255,255,255,.1)", border: "none", borderRadius: 6, padding: "4px 8px", cursor: "pointer", color: C.white, fontSize: 16 }}>×</button>
      </div>

      {/* Upstream columns strip */}
      {upstreamCols.length > 0 && (
        <div style={{ padding: "8px 14px", background: C.g50, borderBottom: `1px solid ${C.g100}`, flexShrink: 0 }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: C.g400, textTransform: "uppercase", letterSpacing: 1, marginBottom: 5 }}>
            Available columns
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 3, maxHeight: 60, overflowY: "auto" }}>
            {upstreamCols.map(c => (
              <span key={c} style={{ fontSize: 9, padding: "2px 5px", borderRadius: 3, background: C.blueTint2, color: C.blue, fontWeight: 600, fontFamily: "monospace" }}>{c}</span>
            ))}
          </div>
        </div>
      )}

      {/* Scrollable content */}
      <div style={{ flex: 1, overflowY: "auto", padding: 16 }}>
        {type === "input_dataset" && (
          <InputDatasetSidebar
            node={node} datasets={datasets}
            onSave={cfg => { onSave(node.id, cfg); }}
          />
        )}

        {type === "output_dataset" && (
          <OutputDatasetSidebar
            node={node}
            upstreamCols={upstreamCols}
            onSave={cfg => { onSave(node.id, cfg); }}
          />
        )}

        {isUtil && (
          <UtilitySidebarInner
            node={node}
            columns={upstreamCols}
            allNodes={allNodes}
            onSave={cfg => onSave(node.id, cfg)}
          />
        )}
      </div>
    </div>
  );
}

// Utility configs rendered inline in sidebar (no popup)
function UtilitySidebarInner({ node, columns, allNodes, onSave }) {
  const type   = node.data?.type;
  const config = node.data?.config || {};

  // Use the same UtilityConfigModal but render its body inline
  // We wrap UtilityConfigModal content here directly
  return (
    <InlinedUtilityConfig
      type={type}
      config={config}
      columns={columns}
      allNodes={allNodes}
      onSave={onSave}
    />
  );
}

// Inline versions of all utility configs (no modal shell)
const DATA_TYPES  = ["TEXT","INTEGER","BIGINT","NUMERIC","BOOLEAN","DATE","TIMESTAMP","VARCHAR(255)"];
const JOIN_TYPES  = ["INNER JOIN","LEFT JOIN","RIGHT JOIN","FULL OUTER JOIN","CROSS JOIN"];
const AGG_FUNCS   = ["COUNT","SUM","AVG","MIN","MAX","COUNT DISTINCT"];
const CONDITIONS  = ["=","!=",">",">=","<","<=","LIKE","IS NULL","IS NOT NULL","IN","NOT IN"];
const ORDER_DIRS  = ["ASC","DESC"];

function ColPicker({ columns, selected, onAdd, onRemove, onAddAll, onRemoveAll }) {
  const [open, setOpen] = useState(false);
  const avail = columns.filter(c => !selected.includes(c));
  return (
    <div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 4, marginBottom: 8, minHeight: 24 }}>
        {selected.length === 0 && <span style={{ fontSize: 11, color: C.g300, fontStyle: "italic" }}>No columns selected</span>}
        {selected.map(c => (
          <span key={c} style={{ display: "inline-flex", alignItems: "center", gap: 3, padding: "2px 7px", borderRadius: 20, background: C.blueTint2, color: C.blue, fontSize: 10, fontWeight: 600 }}>
            {c}
            <button onClick={() => onRemove(c)} style={{ background: "none", border: "none", cursor: "pointer", color: C.blue, padding: 0, fontSize: 12, lineHeight: 1 }}>×</button>
          </span>
        ))}
      </div>
      <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
        <div style={{ position: "relative" }}>
          <button onClick={() => setOpen(v => !v)} style={{ padding: "4px 9px", borderRadius: 6, border: `1px solid ${C.blue}`, background: C.blueTint, color: C.blue, fontSize: 11, fontWeight: 700, cursor: "pointer" }}>
            + Column
          </button>
          {open && (
            <div style={{ position: "absolute", top: "100%", left: 0, zIndex: 200, background: C.white, border: `1px solid ${C.g200}`, borderRadius: 8, boxShadow: "0 4px 16px rgba(0,0,0,.12)", maxHeight: 160, overflowY: "auto", minWidth: 160, marginTop: 3 }}>
              {avail.length === 0
                ? <div style={{ padding: "8px 12px", fontSize: 11, color: C.g400 }}>All selected</div>
                : avail.map(c => (
                  <button key={c} onClick={() => { onAdd(c); setOpen(false); }}
                    style={{ width: "100%", padding: "7px 12px", background: "none", border: "none", textAlign: "left", fontSize: 11, cursor: "pointer", color: C.g700, fontFamily: "monospace" }}
                    onMouseEnter={e => e.currentTarget.style.background = C.g50}
                    onMouseLeave={e => e.currentTarget.style.background = "none"}>
                    {c}
                  </button>
                ))}
            </div>
          )}
        </div>
        {onAddAll && <button onClick={() => avail.forEach(c => onAdd(c))} style={{ padding: "4px 9px", borderRadius: 6, border: `1px solid ${C.g200}`, background: C.g50, color: C.g600, fontSize: 11, cursor: "pointer" }}>All</button>}
        {onRemoveAll && selected.length > 0 && <button onClick={onRemoveAll} style={{ padding: "4px 9px", borderRadius: 6, border: `1px solid ${C.redTint}`, background: C.redTint, color: C.red, fontSize: 11, cursor: "pointer" }}>Clear</button>}
      </div>
    </div>
  );
}

function SaveBtn({ onClick, disabled }) {
  return (
    <button onClick={onClick} disabled={disabled} style={{
      width: "100%", marginTop: 14, padding: "9px 0", borderRadius: 8, border: "none",
      background: disabled ? C.g300 : `linear-gradient(90deg,${C.blue},${C.blueMid})`,
      color: C.white, fontWeight: 700, fontSize: 13,
      cursor: disabled ? "not-allowed" : "pointer",
    }}>
      Save Configuration
    </button>
  );
}

function InlinedUtilityConfig({ type, config, columns, allNodes, onSave }) {
  // State for each config type
  const [selCols,    setSelCols]    = useState(config?.columns     || []);
  const [renames,    setRenames]    = useState(config?.renames      || {});
  const [constName,  setConstName]  = useState(config?.name         || "");
  const [constVal,   setConstVal]   = useState(config?.value        || "");
  const [constType,  setConstType]  = useState(config?.dtype        || "TEXT");
  const [orders,     setOrders]     = useState(config?.orders       || []);
  const [typeMap,    setTypeMap]    = useState(config?.types         || {});
  const [targetCol,  setTargetCol]  = useState(config?.targetCol    || "");
  const [sourceCol,  setSourceCol]  = useState(config?.sourceCol    || "");
  const [useExpr,    setUseExpr]    = useState(config?.useExpr      || false);
  const [expr,       setExpr]       = useState(config?.expr         || "");
  const [srcCol,     setSrcCol]     = useState(config?.sourceCol    || "");
  const [newColName, setNewColName] = useState(config?.newColName   || "");
  const [elseVal,    setElseVal]    = useState(config?.elseValue    || "");
  const [whens,      setWhens]      = useState(config?.whens        || [{ condition:"=", value:"", result:"" }]);
  const [fillType,   setFillType]   = useState(config?.fillType     || "value");
  const [fillVal,    setFillVal]    = useState(config?.fillValue    || "");
  const [formula,    setFormula]    = useState(config?.formula      || "");
  const [groupCols,  setGroupCols]  = useState(config?.groupCols    || []);
  const [aggCols,    setAggCols]    = useState(config?.aggCols      || []);
  const [joinType,   setJoinType]   = useState(config?.joinType     || "INNER JOIN");
  const [leftCol,    setLeftCol]    = useState(config?.leftCol      || "");
  const [rightCol,   setRightCol]   = useState(config?.rightCol     || "");
  const [rightNodeId,setRightNodeId]= useState(config?.rightNodeId  || "");
  const [pyCode,     setPyCode]     = useState(config?.code         || "# df is your input DataFrame\ndf = df.filter(df['age'] > 18)\n");
  const [orderOpen,  setOrderOpen]  = useState(false);
  const [aggOpen,    setAggOpen]    = useState(false);

  const inputNodes = (allNodes || []).filter(n => n.data?.type === "input_dataset" && n.data?.config?.dataset);
  const rightDS    = inputNodes.find(n => n.id === rightNodeId)?.data?.config?.dataset;
  const rightCols  = rightDS?.columns || [];

  const fs = { fontSize: 11, fontWeight: 700, color: C.g600, marginBottom: 4 };

  if (type === "select_col") return (
    <div>
      <div style={fs}>Select columns to keep</div>
      <ColPicker columns={columns} selected={selCols}
        onAdd={c => !selCols.includes(c) && setSelCols(s=>[...s,c])}
        onRemove={c => setSelCols(s=>s.filter(x=>x!==c))}
        onAddAll={() => setSelCols(columns)} onRemoveAll={() => setSelCols([])} />
      <SaveBtn onClick={() => onSave({ columns: selCols })} disabled={selCols.length===0} />
    </div>
  );

  if (type === "drop_col") return (
    <div>
      <div style={fs}>Select columns to DROP</div>
      <ColPicker columns={columns} selected={selCols}
        onAdd={c => !selCols.includes(c) && setSelCols(s=>[...s,c])}
        onRemove={c => setSelCols(s=>s.filter(x=>x!==c))}
        onAddAll={() => setSelCols(columns)} onRemoveAll={() => setSelCols([])} />
      {selCols.length > 0 && <div style={{ marginTop: 8, background: C.redTint, borderRadius: 6, padding: "6px 10px", fontSize: 10, color: C.red }}>⚠ Will remove: {selCols.join(", ")}</div>}
      <SaveBtn onClick={() => onSave({ columns: selCols })} disabled={selCols.length===0} />
    </div>
  );

  if (type === "rename_col") return (
    <div>
      <div style={fs}>Select columns to rename</div>
      <ColPicker columns={columns}
        selected={Object.keys(renames)}
        onAdd={c => setRenames(r => ({...r,[c]:r[c]||""}))}
        onRemove={c => setRenames(r => { const n={...r}; delete n[c]; return n; })}
        onAddAll={() => { const r={}; columns.forEach(c=>r[c]=""); setRenames(r); }}
        onRemoveAll={() => setRenames({})} />
      {Object.keys(renames).length > 0 && (
        <div style={{ marginTop: 10, display: "flex", flexDirection: "column", gap: 6 }}>
          {Object.keys(renames).map(c => (
            <div key={c} style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <div style={{ width: 80, padding: "5px 7px", borderRadius: 5, background: C.g100, fontSize: 10, color: C.g600, fontFamily: "monospace", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{c}</div>
              <span style={{ color: C.g400, fontSize: 11 }}>→</span>
              <input value={renames[c]} onChange={e => setRenames(r => ({...r,[c]:e.target.value}))} placeholder="new name"
                style={{ flex:1, padding:"5px 7px", borderRadius:5, border:`1px solid ${C.g200}`, fontSize:10, outline:"none", color:C.g700, fontFamily:"monospace" }} />
            </div>
          ))}
        </div>
      )}
      <SaveBtn onClick={() => onSave({ renames })} disabled={Object.keys(renames).length===0 || Object.values(renames).some(v=>!v)} />
    </div>
  );

  if (type === "add_const") return (
    <div style={{ display:"flex",flexDirection:"column",gap:10 }}>
      <div><div style={fs}>Column Name *</div><SidebarInput value={constName} onChange={setConstName} placeholder="e.g. country" /></div>
      <div><div style={fs}>Constant Value *</div><SidebarInput value={constVal} onChange={setConstVal} placeholder='e.g. "Indonesia" or 42' /></div>
      <div><div style={fs}>Data Type</div>
        <select value={constType} onChange={e=>setConstType(e.target.value)} style={{ width:"100%",padding:"7px 9px",borderRadius:7,border:`1px solid ${C.g200}`,fontSize:11,outline:"none",color:C.g700 }}>
          {DATA_TYPES.map(d=><option key={d}>{d}</option>)}
        </select>
      </div>
      {constName&&constVal && <div style={{ background:C.blueTint,borderRadius:6,padding:"6px 10px",fontSize:10,color:C.blue,fontFamily:"monospace" }}>{constName} = {constVal} ({constType})</div>}
      <SaveBtn onClick={() => onSave({ name:constName,value:constVal,dtype:constType })} disabled={!constName||!constVal} />
    </div>
  );

  if (type === "set_val") return (
    <div style={{ display:"flex",flexDirection:"column",gap:10 }}>
      <div><div style={fs}>Target Column *</div><SidebarSelect value={targetCol} onChange={setTargetCol} options={columns} placeholder="— column to update —" /></div>
      <div style={{ display:"flex",gap:6 }}>
        <button onClick={()=>setUseExpr(false)} style={{ flex:1,padding:"6px",borderRadius:6,border:`1px solid ${!useExpr?C.blue:C.g200}`,background:!useExpr?C.blueTint:C.white,color:!useExpr?C.blue:C.g500,fontSize:11,cursor:"pointer" }}>From Column</button>
        <button onClick={()=>setUseExpr(true)} style={{ flex:1,padding:"6px",borderRadius:6,border:`1px solid ${useExpr?C.blue:C.g200}`,background:useExpr?C.blueTint:C.white,color:useExpr?C.blue:C.g500,fontSize:11,cursor:"pointer" }}>Expression</button>
      </div>
      {!useExpr
        ? <div><div style={fs}>Source Column *</div><SidebarSelect value={sourceCol} onChange={setSourceCol} options={columns.filter(c=>c!==targetCol)} placeholder="— value from —" /></div>
        : <div><div style={fs}>SQL Expression *</div><SidebarInput value={expr} onChange={setExpr} placeholder="e.g. UPPER(name)" mono /></div>
      }
      <SaveBtn onClick={() => onSave({ targetCol,sourceCol,useExpr,expr })} disabled={!targetCol||(!sourceCol&&!expr)} />
    </div>
  );

  if (type === "fill_null") return (
    <div style={{ display:"flex",flexDirection:"column",gap:10 }}>
      <div><div style={fs}>Select Columns *</div>
        <ColPicker columns={columns} selected={selCols}
          onAdd={c => !selCols.includes(c)&&setSelCols(s=>[...s,c])}
          onRemove={c => setSelCols(s=>s.filter(x=>x!==c))}
          onAddAll={() => setSelCols(columns)} onRemoveAll={() => setSelCols([])} />
      </div>
      <div><div style={fs}>Fill Method</div>
        <div style={{ display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:5 }}>
          {[["value","Custom"],["mean","Mean"],["median","Median"],["mode","Mode"],["forward","Forward"],["backward","Backward"]].map(([v,l])=>(
            <button key={v} onClick={()=>setFillType(v)} style={{ padding:"5px",borderRadius:6,border:`1px solid ${fillType===v?C.blue:C.g200}`,background:fillType===v?C.blueTint:C.white,color:fillType===v?C.blue:C.g500,fontSize:10,cursor:"pointer" }}>{l}</button>
          ))}
        </div>
      </div>
      {fillType==="value" && <div><div style={fs}>Fill Value *</div><SidebarInput value={fillVal} onChange={setFillVal} placeholder='e.g. 0 or "Unknown"' /></div>}
      <SaveBtn onClick={() => onSave({ columns:selCols,fillType,fillValue:fillVal })} disabled={selCols.length===0||(fillType==="value"&&!fillVal)} />
    </div>
  );

  if (type === "filter_rows") return (
    <div>
      <div style={fs}>SQL WHERE condition *</div>
      <textarea value={formula} onChange={e=>setFormula(e.target.value)} rows={4} placeholder={"e.g. age > 18\ncity = 'Jakarta'\nchild_no IS NULL"}
        style={{ width:"100%",padding:"8px 10px",borderRadius:7,border:`1px solid ${C.g200}`,fontSize:11,boxSizing:"border-box",outline:"none",color:C.g700,resize:"vertical",fontFamily:"monospace",lineHeight:1.7 }} />
      <div style={{ marginTop:8,marginBottom:6 }}>
        <div style={{ fontSize:9,color:C.g400,marginBottom:5,textTransform:"uppercase",letterSpacing:1 }}>Click to insert column</div>
        <div style={{ display:"flex",flexWrap:"wrap",gap:4 }}>
          {columns.map(c=>(
            <button key={c} onClick={()=>setFormula(f=>f+(f?" AND ":"")+c)} style={{ padding:"2px 7px",borderRadius:4,border:`1px solid ${C.g200}`,background:C.g50,color:C.g600,fontSize:10,cursor:"pointer",fontFamily:"monospace" }}>{c}</button>
          ))}
        </div>
      </div>
      <SaveBtn onClick={() => onSave({ formula })} disabled={!formula} />
    </div>
  );

  if (type === "order_table") return (
    <div>
      <div style={fs}>Sort Columns</div>
      {orders.length > 0 && (
        <div style={{ display:"flex",flexDirection:"column",gap:6,marginBottom:8 }}>
          {orders.map((o,i)=>(
            <div key={i} style={{ display:"flex",alignItems:"center",gap:6,background:C.g50,borderRadius:6,padding:6,border:`1px solid ${C.g100}` }}>
              <span style={{ width:16,height:16,borderRadius:"50%",background:C.blue,color:C.white,fontSize:9,fontWeight:700,display:"flex",alignItems:"center",justifyContent:"center",flexShrink:0 }}>{i+1}</span>
              <div style={{ flex:1,fontSize:10,fontFamily:"monospace",color:C.g700 }}>{o.col}</div>
              <select value={o.dir} onChange={e=>setOrders(ord=>ord.map((x,idx)=>idx===i?{...x,dir:e.target.value}:x))}
                style={{ padding:"3px 5px",borderRadius:5,border:`1px solid ${C.g200}`,fontSize:10,fontWeight:700,color:o.dir==="ASC"?C.green:C.orange,background:C.white,width:60 }}>
                {ORDER_DIRS.map(d=><option key={d}>{d}</option>)}
              </select>
              <button onClick={()=>setOrders(ord=>ord.filter((_,idx)=>idx!==i))} style={{ background:C.redTint,border:"none",borderRadius:4,width:20,height:20,cursor:"pointer",color:C.red,fontSize:12,display:"flex",alignItems:"center",justifyContent:"center" }}>×</button>
            </div>
          ))}
        </div>
      )}
      <div style={{ position:"relative",display:"inline-block" }}>
        <button onClick={()=>setOrderOpen(v=>!v)} style={{ padding:"4px 9px",borderRadius:6,border:`1px solid ${C.blue}`,background:C.blueTint,color:C.blue,fontSize:11,fontWeight:700,cursor:"pointer" }}>+ Add Column</button>
        {orderOpen && (
          <div style={{ position:"absolute",top:"100%",left:0,zIndex:200,background:C.white,border:`1px solid ${C.g200}`,borderRadius:8,boxShadow:"0 4px 16px rgba(0,0,0,.12)",maxHeight:140,overflowY:"auto",minWidth:150,marginTop:3 }}>
            {columns.filter(c=>!orders.find(o=>o.col===c)).map(c=>(
              <button key={c} onClick={()=>{setOrders(ord=>[...ord,{col:c,dir:"ASC"}]);setOrderOpen(false);}}
                style={{ width:"100%",padding:"6px 12px",background:"none",border:"none",textAlign:"left",fontSize:11,cursor:"pointer",color:C.g700,fontFamily:"monospace" }}
                onMouseEnter={e=>e.currentTarget.style.background=C.g50} onMouseLeave={e=>e.currentTarget.style.background="none"}>{c}</button>
            ))}
          </div>
        )}
      </div>
      {orders.length>0 && <div style={{ marginTop:8,background:C.blueTint,borderRadius:6,padding:"5px 9px",fontSize:10,color:C.blue,fontFamily:"monospace" }}>ORDER BY {orders.map(o=>`${o.col} ${o.dir}`).join(", ")}</div>}
      <SaveBtn onClick={() => onSave({ orders })} disabled={orders.length===0} />
    </div>
  );

  if (type === "change_type") return (
    <div>
      <div style={fs}>Select columns to retype</div>
      <ColPicker columns={columns}
        selected={Object.keys(typeMap)}
        onAdd={c => setTypeMap(m=>({...m,[c]:m[c]||"TEXT"}))}
        onRemove={c => setTypeMap(m=>{ const n={...m}; delete n[c]; return n; })}
        onAddAll={() => { const m={}; columns.forEach(c=>m[c]="TEXT"); setTypeMap(m); }}
        onRemoveAll={() => setTypeMap({})} />
      {Object.keys(typeMap).length > 0 && (
        <div style={{ marginTop:10,display:"flex",flexDirection:"column",gap:5 }}>
          {Object.keys(typeMap).map(c=>(
            <div key={c} style={{ display:"flex",alignItems:"center",gap:6 }}>
              <div style={{ width:80,padding:"5px 7px",borderRadius:5,background:C.g100,fontSize:10,color:C.g600,fontFamily:"monospace",overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{c}</div>
              <span style={{ color:C.g400 }}>→</span>
              <select value={typeMap[c]} onChange={e=>setTypeMap(m=>({...m,[c]:e.target.value}))}
                style={{ flex:1,padding:"5px",borderRadius:5,border:`1px solid ${C.g200}`,fontSize:10,fontWeight:700,color:C.blue,background:C.white }}>
                {DATA_TYPES.map(d=><option key={d}>{d}</option>)}
              </select>
            </div>
          ))}
        </div>
      )}
      <SaveBtn onClick={() => onSave({ types:typeMap })} disabled={Object.keys(typeMap).length===0} />
    </div>
  );

  if (type === "val_mapper") return (
    <div style={{ display:"flex",flexDirection:"column",gap:10 }}>
      <div style={{ display:"flex",gap:8 }}>
        <div style={{ flex:1 }}><div style={fs}>Source Column *</div><SidebarSelect value={srcCol} onChange={setSrcCol} options={columns} placeholder="— column —" /></div>
        <div style={{ flex:1 }}><div style={fs}>New Column Name *</div><SidebarInput value={newColName} onChange={setNewColName} placeholder="member_level" /></div>
      </div>
      <div>
        <div style={fs}>Conditions (WHEN) *</div>
        <div style={{ display:"flex",flexDirection:"column",gap:6 }}>
          {whens.map((w,i)=>(
            <div key={i} style={{ background:C.g50,borderRadius:7,padding:8,border:`1px solid ${C.g100}` }}>
              <div style={{ display:"flex",alignItems:"center",gap:4,marginBottom:6 }}>
                <span style={{ fontSize:10,fontWeight:700,color:C.blue }}>WHEN #{i+1}</span>
                {i>0 && <button onClick={()=>setWhens(w=>w.filter((_,idx)=>idx!==i))} style={{ marginLeft:"auto",background:C.redTint,border:"none",borderRadius:4,padding:"1px 6px",cursor:"pointer",color:C.red,fontSize:10 }}>Remove</button>}
              </div>
              <div style={{ display:"grid",gridTemplateColumns:"1fr 1fr",gap:5 }}>
                <div>
                  <div style={{ fontSize:9,color:C.g400,marginBottom:2 }}>Condition</div>
                  <select value={w.condition} onChange={e=>setWhens(ws=>ws.map((x,idx)=>idx===i?{...x,condition:e.target.value}:x))}
                    style={{ width:"100%",padding:"5px",borderRadius:5,border:`1px solid ${C.g200}`,fontSize:10,background:C.white }}>
                    {CONDITIONS.map(c=><option key={c}>{c}</option>)}
                  </select>
                </div>
                <div>
                  <div style={{ fontSize:9,color:C.g400,marginBottom:2 }}>Compare Value</div>
                  <input value={w.value} onChange={e=>setWhens(ws=>ws.map((x,idx)=>idx===i?{...x,value:e.target.value}:x))} placeholder="1000"
                    style={{ width:"100%",padding:"5px",borderRadius:5,border:`1px solid ${C.g200}`,fontSize:10,boxSizing:"border-box",outline:"none",color:C.g700 }} />
                </div>
                <div style={{ gridColumn:"1/-1" }}>
                  <div style={{ fontSize:9,color:C.g400,marginBottom:2 }}>Then (Result)</div>
                  <input value={w.result} onChange={e=>setWhens(ws=>ws.map((x,idx)=>idx===i?{...x,result:e.target.value}:x))} placeholder="silver"
                    style={{ width:"100%",padding:"5px",borderRadius:5,border:`1px solid ${C.g200}`,fontSize:10,boxSizing:"border-box",outline:"none",color:C.g700 }} />
                </div>
              </div>
            </div>
          ))}
        </div>
        <button onClick={()=>setWhens(w=>[...w,{condition:"=",value:"",result:""}])} style={{ marginTop:6,padding:"4px 10px",borderRadius:5,border:`1px dashed ${C.blue}`,background:C.blueTint,color:C.blue,fontSize:10,cursor:"pointer" }}>+ Add WHEN</button>
      </div>
      <div><div style={fs}>ELSE Value *</div><SidebarInput value={elseVal} onChange={setElseVal} placeholder="bronze (default)" /></div>
      <SaveBtn onClick={() => onSave({ sourceCol:srcCol,newColName,elseValue:elseVal,whens })} disabled={!srcCol||!newColName||!elseVal||whens.some(w=>!w.value||!w.result)} />
    </div>
  );

  if (type === "group_agg") return (
    <div style={{ display:"flex",flexDirection:"column",gap:10 }}>
      <div><div style={fs}>Group By Columns *</div>
        <ColPicker columns={columns} selected={groupCols}
          onAdd={c=>!groupCols.includes(c)&&setGroupCols(g=>[...g,c])}
          onRemove={c=>setGroupCols(g=>g.filter(x=>x!==c))}
          onAddAll={()=>setGroupCols(columns)} onRemoveAll={()=>setGroupCols([])} />
      </div>
      <div>
        <div style={fs}>Aggregate Columns *</div>
        {aggCols.length>0 && (
          <div style={{ display:"flex",flexDirection:"column",gap:5,marginBottom:6 }}>
            {aggCols.map((a,i)=>(
              <div key={i} style={{ display:"grid",gridTemplateColumns:"1fr 1fr 1fr auto",gap:4,alignItems:"center",background:C.g50,borderRadius:6,padding:6,border:`1px solid ${C.g100}` }}>
                <div style={{ fontSize:9,fontFamily:"monospace",color:C.g600,padding:"3px 5px",background:C.g100,borderRadius:4,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{a.col}</div>
                <select value={a.func} onChange={e=>setAggCols(ac=>ac.map((x,idx)=>idx===i?{...x,func:e.target.value}:x))}
                  style={{ padding:"4px",borderRadius:5,border:`1px solid ${C.g200}`,fontSize:10,fontWeight:700,color:C.blue,background:C.white }}>
                  {AGG_FUNCS.map(f=><option key={f}>{f}</option>)}
                </select>
                <input value={a.alias} onChange={e=>setAggCols(ac=>ac.map((x,idx)=>idx===i?{...x,alias:e.target.value}:x))} placeholder="alias"
                  style={{ padding:"4px",borderRadius:5,border:`1px solid ${C.g200}`,fontSize:10,outline:"none",color:C.g700,fontFamily:"monospace" }} />
                <button onClick={()=>setAggCols(ac=>ac.filter((_,idx)=>idx!==i))} style={{ background:C.redTint,border:"none",borderRadius:4,width:20,height:20,cursor:"pointer",color:C.red,fontSize:12,display:"flex",alignItems:"center",justifyContent:"center" }}>×</button>
              </div>
            ))}
          </div>
        )}
        <div style={{ position:"relative",display:"inline-block" }}>
          <button onClick={()=>setAggOpen(v=>!v)} style={{ padding:"4px 9px",borderRadius:6,border:`1px solid ${C.gold}`,background:C.goldTint,color:C.gold,fontSize:11,fontWeight:700,cursor:"pointer" }}>+ Aggregate</button>
          {aggOpen && (
            <div style={{ position:"absolute",top:"100%",left:0,zIndex:200,background:C.white,border:`1px solid ${C.g200}`,borderRadius:8,boxShadow:"0 4px 16px rgba(0,0,0,.12)",maxHeight:140,overflowY:"auto",minWidth:150,marginTop:3 }}>
              {columns.map(c=>(
                <button key={c} onClick={()=>{setAggCols(ac=>[...ac,{col:c,func:"COUNT",alias:`${c}_count`}]);setAggOpen(false);}}
                  style={{ width:"100%",padding:"6px 12px",background:"none",border:"none",textAlign:"left",fontSize:11,cursor:"pointer",color:C.g700,fontFamily:"monospace" }}
                  onMouseEnter={e=>e.currentTarget.style.background=C.g50} onMouseLeave={e=>e.currentTarget.style.background="none"}>{c}</button>
              ))}
            </div>
          )}
        </div>
      </div>
      <SaveBtn onClick={() => onSave({ groupCols,aggCols })} disabled={groupCols.length===0||aggCols.length===0} />
    </div>
  );

  if (type === "join_data") return (
    <div style={{ display:"flex",flexDirection:"column",gap:10 }}>
      <div><div style={fs}>Join Type *</div>
        <div style={{ display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:5 }}>
          {JOIN_TYPES.map(jt=>(
            <button key={jt} onClick={()=>setJoinType(jt)} style={{ padding:"5px 2px",borderRadius:6,border:`1px solid ${joinType===jt?C.blue:C.g200}`,background:joinType===jt?C.blueTint:C.white,color:joinType===jt?C.blue:C.g500,fontSize:9,fontWeight:700,cursor:"pointer" }}>
              {jt.replace(" JOIN","")}
            </button>
          ))}
        </div>
      </div>
      {inputNodes.length>0 && (
        <div><div style={fs}>Right Table *</div>
          <SidebarSelect value={rightNodeId} onChange={setRightNodeId}
            options={inputNodes.map(n=>({ value:n.id, label:n.data.config.dataset.name }))}
            placeholder="— second dataset —" />
        </div>
      )}
      <div style={{ display:"grid",gridTemplateColumns:"1fr auto 1fr",gap:8,alignItems:"end" }}>
        <div><div style={fs}>Left Column *</div><SidebarSelect value={leftCol} onChange={setLeftCol} options={columns} placeholder="— left —" /></div>
        <div style={{ paddingBottom:8,color:C.g400,fontSize:13 }}>=</div>
        <div><div style={fs}>Right Column *</div><SidebarSelect value={rightCol} onChange={setRightCol} options={rightCols} placeholder="— right —" /></div>
      </div>
      <SaveBtn onClick={() => onSave({ joinType,leftCol,rightCol,rightNodeId })} disabled={!leftCol||!rightCol} />
    </div>
  );

  if (type === "pyspark") return (
    <div>
      <div style={fs}>Python / PySpark Code *</div>
      <div style={{ background:"#0f172a",borderRadius:8,overflow:"hidden",border:`1px solid ${C.g700}`,marginBottom:10 }}>
        <div style={{ padding:"4px 10px",background:"#1e293b",display:"flex",gap:4,alignItems:"center" }}>
          {["#f87171","#fbbf24","#34d399"].map((col,i)=><span key={i} style={{ width:9,height:9,borderRadius:"50%",background:col,display:"inline-block" }} />)}
          <span style={{ marginLeft:6,fontSize:9,color:C.g400 }}>python</span>
        </div>
        <textarea value={pyCode} onChange={e=>setPyCode(e.target.value)} rows={10} spellCheck={false}
          style={{ width:"100%",padding:"10px 12px",background:"transparent",border:"none",outline:"none",color:"#93c5fd",fontSize:11,fontFamily:"monospace",resize:"vertical",lineHeight:1.7,boxSizing:"border-box" }} />
      </div>
      <SaveBtn onClick={() => onSave({ code:pyCode })} disabled={!pyCode.trim()} />
    </div>
  );

  return <div style={{ color:C.g400,fontSize:12,textAlign:"center",padding:20 }}>No config needed for this node.</div>;
}

// ── Main WorkflowEditor ───────────────────────────────────────────────────────
export default function WorkflowEditor({ datasets = [], onSave, toast }) {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [selectedId, setSelectedId] = useState(null);
  const [previewBox, setPreviewBox] = useState({ show: false, loading: false, data: null });

  // Update status "Connected" pada Node secara Realtime
  useEffect(() => {
    setNodes(nds => nds.map(n => {
      const upstream = getUpstreamData(n.id, nds, edges);
      if (n.data.isConnected !== upstream.connected || JSON.stringify(n.data.columns) !== JSON.stringify(upstream.columns)) {
        return { ...n, data: { ...n.data, isConnected: upstream.connected, columns: upstream.columns } };
      }
      return n;
    }));
  }, [edges, setNodes]);
  
  // Sidebar State
  const [selectedNodeId, setSelectedNodeId] = useState(null);
  const [sidebarTab, setSidebarTab] = useState("config");
  
  // Bottom Preview Panel State
  const [previewData, setPreviewData] = useState(null);
  const [showPreview, setShowPreview] = useState(false);
  const [loadingPreview, setLoadingPreview] = useState(false);

  // Hitung kolom yang merambat (propagated) berdasarkan edges
  const columnMap = useMemo(() => computeNodeColumns(nodes, edges), [nodes, edges]);

  const makeCallbacks = useCallback((nid) => ({
    onDelete: (id) => { 
      setNodes(ns => ns.filter(n => n.id !== id)); 
      setEdges(es => es.filter(e => e.source !== id && e.target !== id));
      if (selectedNodeId === id) setSelectedNodeId(null); 
    },
    onSelect: (id) => setSelectedNodeId(id),
  }), [setNodes, setEdges, selectedNodeId]);

  // Sync columns ke node data
  useEffect(() => {
    setNodes(ns => ns.map(n => {
      const srcEdge  = edges.find(e => e.target === n.id);
      const upCols   = srcEdge ? (columnMap[srcEdge.source] || []) : [];
      if (JSON.stringify(n.data.upstreamColumns) !== JSON.stringify(upCols)) {
        return { ...n, data: { ...n.data, upstreamColumns: upCols } };
      }
      return n;
    }));
  }, [columnMap, edges, setNodes]);

  const addNode = (type) => {
    const id = `node_${Date.now()}`;
    setNodes((prev) => [
      ...prev,
      {
        id, type: "etlNode", position: { x: 250, y: 150 },
        data: { id, label: type, type, config: {}, ...makeCallbacks(id) }
      }
    ]);
  };

  const onConnect = (params) => setEdges((eds) => addEdge(params, eds));

  // --- RE-EDITABLE CONFIG HANDLER ---
  const handleConfigChange = (key, value) => {
    setNodes((prev) =>
      prev.map(n => n.id === selectedNodeId
        ? { ...n, data: { ...n.data, config: { ...n.data.config, [key]: value } } }
        : n
      )
    );
  };

  const selectedNode = nodes.find(n => n.id === selectedNodeId);
  const upstreamCols = selectedNode?.data?.upstreamColumns || [];

    // CEK APAKAH NODE TERSAMBUNG (Validasi Edge)
  const isUtilityNode = selectedNode && !["input_dataset"].includes(selectedNode.data.type);
  const isDataNotConnected = isUtilityNode && upstreamCols.length === 0;

    // --- MULTI-BRANCH EXECUTION (1 WORKFLOW = BANYAK OUTPUT) ---
  const handleRunPipeline = async () => {
    const outNodes = nodes.filter(n => n.data.type === "output_dataset");
    if (outNodes.length === 0) return toast("Tambahkan minimal 1 Output Dataset!", "error");

    // Rekonstruksi graf menjadi Tasks untuk Backend
    const tasks = outNodes.map((outNode) => {
      // Telusuri mundur dari Output ke Input untuk mendapatkan daftar transform
      let currentId = outNode.id;
      let branchTransforms = [];
      let inputConfig = null;

      while (currentId) {
        const edge = edges.find(e => e.target === currentId);
        if (!edge) break;
        
        const parentNode = nodes.find(n => n.id === edge.source);
        if (!parentNode) break;

        if (parentNode.data.type === "input_dataset") {
          inputConfig = parentNode.data.config.dataset;
          break;
        } else {
          // Masukkan ke array awal (karena kita jalan mundur)
          branchTransforms.unshift({
            type: parentNode.data.type,
            config: parentNode.data.config
          });
        }
        currentId = parentNode.id;
      }

      return {
        task_id: outNode.id,
        output_name: outNode.data.config.outputName || `output_${outNode.id}`,
        format: outNode.data.config.format || "PARQUET", // <--- Save as CSV/Parquet
        inputs: inputConfig ? [{ name: inputConfig.table_name || inputConfig.name, type: "raw" }] : [],
        transforms: branchTransforms
      };
    });

    try {
      const payload = {
        workflow_name: workflow?.name || "Multi_Branch_Pipeline",
        tasks: tasks
      };
      
      const res = await api.post("/pipelines/run", payload);
      toast(`Pipeline Triggered! DAG ID: ${res.data.dag_id}`, "success");
    } catch (error) {
      toast("Gagal menjalankan pipeline: " + error.message, "error");
    }
  };

  // --- FETCH PREVIEW DATA UNTUK PANEL BAWAH ---
  const fetchPreviewData = async () => {
    if (!selectedNode) return;
    setShowPreview(true);
    setLoadingPreview(true);

    try {
      // Mockup API Call: Pada realita, panggil /api/pipelines/preview-node
      // yang mensimulasikan Spark pada node tertentu.
      // await api.post("/pipelines/preview-node", { nodeConfig: selectedNode.data.config, upstream... });
      
      // Simulasi delay (Hapus jika API backend sudah siap)
      setTimeout(() => {
        setPreviewData({
          columns: upstreamCols.length > 0 ? upstreamCols : ["col1", "col2", "col3"],
          rows: [
            { col1: "Data 1", col2: 100, col3: true },
            { col1: "Data 2", col2: 200, col3: false },
            { col1: "Preview Simulasi", col2: 999, col3: true },
          ]
        });
        setLoadingPreview(false);
      }, 1500);

    } catch (err) {
      toast("Gagal mengambil preview", "error");
      setLoadingPreview(false);
    }
  };

    // DELETE NODE
  const handleDeleteNode = (id) => {
    setNodes((prev) => prev.filter(n => n.id !== id));
    setEdges((prev) => prev.filter(e => e.source !== id && e.target !== id));
    if (selectedNodeId === id) setSelectedNodeId(null);
  };

  useEffect(() => {
    setNodes(ns => ns.map(n => ({ ...n, data:{ ...n.data, ...makeCallbacks(n.id, n.data.type) } })));
  }, []);

  // CONNECT EDGE
  const onConnect = (params) => setEdges((eds) => addEdge(params, eds));

    // UPDATE CONFIG NODE (Dari Sidebar)
  const handleConfigChange = (key, value) => {
    setNodes((prev) =>
      prev.map(n => n.id === selectedNodeId
        ? { ...n, data: { ...n.data, config: { ...n.data.config, [key]: value } } }
        : n
      )
    );
  };

  const handleSaveConfig = (nodeId, cfg) => {
    setNodes(ns => ns.map(n => n.id === nodeId ? { ...n, data:{ ...n.data, config:cfg } } : n));
    toast("Configuration saved!", "success");
  };

  const handleSave = () => {
    const s = {
      ...workflow,
      nodes: nodes.map(n => ({ id:n.id, type:n.type, position:n.position, data:{ label:n.data.label, type:n.data.type, config:n.data.config, columns:n.data.columns } })),
      edges: edges.map(e => ({ id:e.id, source:e.source, target:e.target })),
      updatedAt: new Date().toISOString(),
    };
    onSave(s); toast("Workflow saved!", "success");
  };

  const handleRun = async () => {
    const inNodes  = nodes.filter(n=>n.data.type==="input_dataset");
    const outNodes = nodes.filter(n=>n.data.type==="output_dataset");
    if (!inNodes.length)  return toast("Add an Input Dataset node first", "error");
    if (!outNodes.length) return toast("Add an Output Dataset node first", "error");
    if (!inNodes[0].data.config?.dataset)    return toast("Configure Input Dataset node", "error");
    if (!outNodes[0].data.config?.outputName) return toast("Configure Output Dataset node", "error");

    const input  = inNodes[0].data.config.dataset;
    const output = outNodes[0].data.config;
    const transforms = nodes
      .filter(n=>!["input_dataset","output_dataset"].includes(n.data.type))
      .map(n=>({ type:n.data.type, config:n.data.config }));

    const inputTable = input.table_name
      ? `staging.${input.table_name}`
      : `staging.${input.name.replace(/\.[^.]+$/,'').toLowerCase().replace(/[\s-]+/g,'_')}`;

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
      const { run_ids, dag_id } = r.data;
      const firstRunId = Object.values(run_ids || {})[0];
      toast(`Pipeline triggered! DAG: ${dag_id}`, "success");
      setDagStatus({ dag_id, state:"queued" });
      handleSave();

      const poll = setInterval(async () => {
        try {
          const s = await api.get(`/pipelines/runs/${firstRunId}/dag-status`);
          setDagStatus(s.data);
          setTaskStates(s.data.tasks || {});
          if (["success","failed"].includes(s.data.state)) {
            clearInterval(poll); setRunning(false);
            toast(s.data.state==="success" ? "Pipeline completed!" : "Pipeline failed", s.data.state==="success"?"success":"error");
          }
        } catch {}
      }, 6000);
    } catch(e) {
      setRunning(false);
      toast(e.response?.data?.detail || "Failed to trigger pipeline", "error");
    }
  };

  const STATUS_BG     = { success:C.greenTint, running:C.blueTint, failed:C.redTint, queued:C.goldTint, none:C.g50 };
  const STATUS_BORDER = { success:C.green, running:C.blue, failed:C.red, queued:C.gold, none:C.g300 };

  // Get upstream cols for selected node
  const selectedUpstreamCols = useMemo(() => {
    if (!selectedNode) return [];
    const srcEdge = edges.find(e => e.target === selectedNode.id);
    return srcEdge ? (columnMap[srcEdge.source] || []) : [];
  }, [selectedNode, edges, columnMap]);

  // Mark selected node in ReactFlow
  const nodesWithSelected = useMemo(() =>
    nodes.map(n => ({ ...n, selected: n.id === selectedNode?.id })),
  [nodes, selectedNode]);

    // -- Add Node --
  const handleAddNode = (typeObj) => {
    const id = `${typeObj.type}_${Date.now()}`;
    setNodes(nds => [...nds, {
      id,
      type: "etlNode",
      position: { x: 250, y: 150 },
      data: { label: typeObj.label, type: typeObj.type, config: {}, isConnected: false, columns: [] }
    }]);
  };

  // -- Edges Connection --
  const onConnect = useCallback((params) => {
    setEdges(eds => addEdge({ ...params, animated: true, markerEnd: { type: MarkerType.ArrowClosed } }, eds));
  }, [setEdges]);

  // -- Update Node Config --
  const updateConfig = (key, value) => {
    setNodes(nds => nds.map(n => {
      if (n.id === selectedId) {
        return { ...n, data: { ...n.data, config: { ...n.data.config, [key]: value } } };
      }
      return n;
    }));
  };

  // -- Delete Node --
  const deleteSelected = () => {
    setNodes(nds => nds.filter(n => n.id !== selectedId));
    setEdges(eds => eds.filter(e => e.source !== selectedId && e.target !== selectedId));
    setSelectedId(null);
  };

  // -- Trigger Multi-Branch Pipeline --
  const handleRunPipeline = () => {
    const outputs = nodes.filter(n => n.data.type === "output_dataset");
    if (outputs.length === 0) return alert("Peringatan: Tambahkan minimal 1 Output Dataset!");

    // Algoritma Membaca Graf Mundur (Multi-Branch)
    const tasks = outputs.map(outNode => {
      let currentId = outNode.id;
      let transforms = [];
      let inputConfig = null;

      while (currentId) {
        const edge = edges.find(e => e.target === currentId);
        if (!edge) break;

        const parent = nodes.find(n => n.id === edge.source);
        if (!parent) break;

        if (parent.data.type === "input_dataset") {
          inputConfig = parent.data.config.dataset;
          break;
        } else {
          // Push ke depan array agar urutannya dari Input -> Output
          transforms.unshift({ type: parent.data.type, config: parent.data.config });
        }
        currentId = parent.id;
      }

      return {
        task_id: outNode.id,
        output_name: outNode.data.config.outputName || `table_${outNode.id}`,
        save_format: outNode.data.config.format || "PARQUET",
        inputs: inputConfig ? [{ name: inputConfig.name }] : [],
        transforms: transforms
      };
    });

    console.log("PAYLOAD JSON UNTUK SPARK:", JSON.stringify({ workflow_name: "My_DAG", tasks }, null, 2));
    alert("Pipeline berhasi di-trigger! Cek console untuk melihat struktur JSON Multi-Branch yang dikirim ke Backend.");
  };

  // -- Generate Data Preview --
  const handlePreview = () => {
    const selNode = nodes.find(n => n.id === selectedId);
    if (!selNode || !selNode.data.isConnected) return;

    setPreviewBox({ show: true, loading: true, data: null });
    
    // Simulasi Loading dari Spark/Backend
    setTimeout(() => {
      setPreviewBox({
        show: true, loading: false,
        data: {
          columns: selNode.data.columns.length > 0 ? selNode.data.columns : ["col_1", "col_2"],
          rows: [
            { col_1: "Sample 1", col_2: "Result A", id: 101, name: "Alpha", amount: 5000 },
            { col_1: "Sample 2", col_2: "Result B", id: 102, name: "Beta", amount: 7500 },
            { col_1: "Sample 3", col_2: "Result C", id: 103, name: "Gamma", amount: 9000 },
          ]
        }
      });
    }, 1200);
  };

  // ==========================================
  // RENDER UI
  // ==========================================
  const selNode = nodes.find(n => n.id === selectedId);

  return (
    <div style={{ display: "flex", height: "100vh", fontFamily: "sans-serif", background: C.light }}>
      
      {/* KIRI: PALETTE */}
      <div style={{ width: "240px", background: "#fff", borderRight: `1px solid ${C.gray}`, display: "flex", flexDirection: "column" }}>
        <div style={{ padding: "15px", background: C.dark, color: "#fff", fontWeight: "bold" }}>ETL Nodes</div>
        <div style={{ padding: "10px", flex: 1, overflowY: "auto" }}>
          <div style={{ fontSize: "11px", fontWeight: "bold", color: C.gray, marginBottom: "8px" }}>DATASETS</div>
          {NODE_TYPES.filter(n => n.cat === "data").map(n => (
            <button key={n.type} onClick={() => handleAddNode(n)} style={btnStyle}>{n.label}</button>
          ))}
          
          <div style={{ fontSize: "11px", fontWeight: "bold", color: C.gray, margin: "20px 0 8px" }}>TRANSFORMATIONS</div>
          {NODE_TYPES.filter(n => n.cat === "util").map(n => (
            <button key={n.type} onClick={() => handleAddNode(n)} style={btnStyle}>{n.label}</button>
          ))}
        </div>
        <div style={{ padding: "15px", borderTop: `1px solid ${C.gray}` }}>
          <button onClick={handleRunPipeline} style={{ ...btnStyle, background: C.blue, color: "#fff", borderColor: C.blue, fontWeight: "bold" }}>
            ▶ Run Pipeline (DAG)
          </button>
        </div>
      </div>

      {/* TENGAH: REACT FLOW CANVAS */}
      <div style={{ flex: 1, position: "relative" }}>
        <ReactFlow
          nodes={nodes.map(n => ({ ...n, selected: n.id === selectedId }))}
          edges={edges}
          nodeTypes={customNodeTypes}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onPaneClick={() => setSelectedId(null)}
          onNodeClick={(_, node) => setSelectedId(node.id)}
          fitView
        >
          <Background color="#ccc" gap={16} />
          <Controls />
        </ReactFlow>

        {/* BAWAH: BOTTOM PREVIEW PANEL */}
        {previewBox.show && (
          <div style={{ position: "absolute", bottom: 0, left: 0, right: 0, height: "250px", background: "#fff", borderTop: `2px solid ${C.blue}`, zIndex: 100, display: "flex", flexDirection: "column", boxShadow: "0 -4px 12px rgba(0,0,0,0.1)" }}>
            <div style={{ padding: "10px 15px", background: C.dark, color: "#fff", display: "flex", justifyContent: "space-between" }}>
              <span style={{ fontSize: "12px", fontWeight: "bold" }}>Data Preview Table</span>
              <button onClick={() => setPreviewBox({ show: false })} style={{ background: "none", color: "#fff", border: "none", cursor: "pointer" }}>✖ Close</button>
            </div>
            <div style={{ flex: 1, overflow: "auto", padding: "15px" }}>
              {previewBox.loading ? (
                <div style={{ textAlign: "center", marginTop: "40px", color: C.gray }}>⏳ Processing Spark Preview...</div>
              ) : previewBox.data ? (
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "12px", textAlign: "left" }}>
                  <thead>
                    <tr style={{ background: C.light }}>
                      {previewBox.data.columns.map(c => <th key={c} style={{ padding: "8px", border: "1px solid #ddd" }}>{c}</th>)}
                    </tr>
                  </thead>
                  <tbody>
                    {previewBox.data.rows.map((row, i) => (
                      <tr key={i}>
                        {previewBox.data.columns.map(c => <td key={c} style={{ padding: "8px", border: "1px solid #ddd" }}>{row[c] || ""}</td>)}
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : null}
            </div>
          </div>
        )}
      </div>

      {/* KANAN: SIDEBAR KONFIGURASI */}
      <div style={{ width: "300px", background: "#fff", borderLeft: `1px solid ${C.gray}`, display: "flex", flexDirection: "column" }}>
        {selNode ? (
          <>
            <div style={{ padding: "15px", background: C.light, borderBottom: `1px solid ${C.gray}`, display: "flex", justifyContent: "space-between" }}>
              <h3 style={{ margin: 0, fontSize: "14px", color: C.dark }}>{selNode.data.label}</h3>
              <button onClick={deleteSelected} style={{ background: C.red, color: "#fff", border: "none", borderRadius: "4px", cursor: "pointer" }}>Delete Node</button>
            </div>

            <div style={{ padding: "15px", flex: 1, overflowY: "auto" }}>
              
              {/* VALIDASI: JIKA BELUM DISAMBUNG EDGES */}
              {!selNode.data.isConnected && selNode.data.type !== "input_dataset" ? (
                <div style={{ padding: "15px", background: "#FEF2F2", border: `1px solid ${C.red}`, borderRadius: "6px", color: C.red, fontSize: "12px" }}>
                  <strong>⚠ Data Belum Masuk!</strong><br/><br/>
                  Tarik garis dari node sebelumnya ke node ini agar dapat melakukan konfigurasi.
                </div>
              ) : (
                <div style={{ display: "flex", flexDirection: "column", gap: "15px" }}>
                  
                  {/* FORM: INPUT DATASET */}
                  {selNode.data.type === "input_dataset" && (
                    <div>
                      <label style={lblStyle}>Pilih Dataset</label>
                      <select value={selNode.data.config.dataset?.name || ""} onChange={(e) => updateConfig("dataset", { name: e.target.value })} style={inpStyle}>
                        <option value="">-- Select --</option>
                        <option value="raw_sales_2024">raw_sales_2024 (CSV)</option>
                        <option value="customers_db">customers_db (Postgres)</option>
                      </select>
                    </div>
                  )}

                  {/* FORM: SELECT COLUMNS */}
                  {selNode.data.type === "select_col" && (
                    <div>
                      <label style={lblStyle}>Pilih Kolom yang Dipertahankan</label>
                      <div style={{ border: "1px solid #ccc", padding: "10px", borderRadius: "4px", maxHeight: "150px", overflow: "auto" }}>
                        {selNode.data.columns.map(c => {
                          const isChecked = (selNode.data.config.selectedCols || []).includes(c);
                          return (
                            <label key={c} style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "12px", marginBottom: "5px" }}>
                              <input type="checkbox" checked={isChecked} onChange={(e) => {
                                const current = selNode.data.config.selectedCols || [];
                                const next = e.target.checked ? [...current, c] : current.filter(x => x !== c);
                                updateConfig("selectedCols", next);
                              }} /> {c}
                            </label>
                          )
                        })}
                      </div>
                    </div>
                  )}

                  {/* FORM: FILTER ROWS */}
                  {selNode.data.type === "filter_rows" && (
                    <div>
                      <label style={lblStyle}>Formula SQL (WHERE)</label>
                      <textarea value={selNode.data.config.formula || ""} onChange={(e) => updateConfig("formula", e.target.value)} placeholder="amount > 1000 AND status = 'OK'" style={{...inpStyle, height: "80px"}} />
                    </div>
                  )}

                  {/* FORM: OUTPUT DATASET */}
                  {selNode.data.type === "output_dataset" && (
                    <>
                      <div>
                        <label style={lblStyle}>Nama Table Output</label>
                        <input type="text" value={selNode.data.config.outputName || ""} onChange={(e) => updateConfig("outputName", e.target.value)} placeholder="clean_table" style={inpStyle} />
                      </div>
                      <div>
                        <label style={lblStyle}>Save As Format</label>
                        <select value={selNode.data.config.format || "PARQUET"} onChange={(e) => updateConfig("format", e.target.value)} style={inpStyle}>
                          <option value="PARQUET">Parquet (.parquet)</option>
                          <option value="CSV">CSV (.csv)</option>
                        </select>
                      </div>
                    </>
                  )}

                  {/* PREVIEW BUTTON */}
                  {selNode.data.type !== "output_dataset" && (
                    <button onClick={handlePreview} style={{ padding: "10px", marginTop: "20px", background: C.gold, border: "none", borderRadius: "6px", color: "#fff", fontWeight: "bold", cursor: "pointer" }}>
                      👁 Preview Data Hasil Transform
                    </button>
                  )}

                </div>
              )}
            </div>
          </>
        ) : (
          <div style={{ padding: "30px", textAlign: "center", color: C.gray, fontSize: "12px" }}>
            Klik salah satu Node untuk melihat Konfigurasi
          </div>
        )}
      </div>

    </div>
  );
}

// --- STYLING VARS ---
const btnStyle = { width: "100%", padding: "10px", marginBottom: "8px", background: "#fff", border: "1px solid #ddd", borderRadius: "6px", cursor: "pointer", textAlign: "left", fontSize: "12px" };
const lblStyle = { display: "block", fontSize: "11px", fontWeight: "bold", marginBottom: "6px", color: C.dark };
const inpStyle = { width: "100%", padding: "8px", border: "1px solid #ccc", borderRadius: "4px", fontSize: "12px", boxSizing: "border-box" };   
// --- STYLING OBJECTS ---
const paletteBtnStyle = { display: "block", width: "100%", padding: "8px", marginBottom: "8px", background: "#fff", border: "1px solid #ddd", borderRadius: "4px", cursor: "pointer", textAlign: "left", fontSize: "11px", fontWeight: "bold" };
const actionBtnStyle = { display: "block", width: "100%", padding: "10px", marginBottom: "8px", background: "#52c41a", color: "#fff", border: "none", borderRadius: "4px", cursor: "pointer", fontWeight: "bold" };
const activeTabStyle = { flex: 1, padding: "10px", border: "none", borderBottom: "3px solid #1890ff", background: "#fff", fontWeight: "bold", color: "#1890ff", cursor: "pointer", fontSize: "12px" };
const inactiveTabStyle = { flex: 1, padding: "10px", border: "none", borderBottom: "3px solid transparent", background: "#f9f9f9", color: "#666", cursor: "pointer", fontSize: "12px" };
const labelStyle = { display: "block", fontSize: "11px", fontWeight: "bold", marginBottom: "5px", color: "#333", textTransform: "uppercase" };
const inputStyle = { width: "100%", padding: "8px", border: "1px solid #ccc", borderRadius: "4px", marginBottom: "15px", boxSizing: "border-box", fontSize: "12px" };