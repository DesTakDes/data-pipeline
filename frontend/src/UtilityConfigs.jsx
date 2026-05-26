// UtilityConfigs.jsx
// Drop-in replacement for utility node configuration sidebar

import { useState } from "react";

const C = {
  navy:"#0B1E3D", blue:"#1D6FEB", blueMid:"#3B82F6",
  blueTint:"#EFF6FF", blueTint2:"#DBEAFE",
  gold:"#F59E0B", goldTint:"#FFFBEB",
  white:"#FFFFFF", g50:"#F8FAFC", g100:"#F1F5F9", g200:"#E2E8F0",
  g300:"#CBD5E1", g400:"#94A3B8", g500:"#64748B", g600:"#475569", g700:"#334155",
  green:"#16A34A", greenTint:"#DCFCE7",
  red:"#DC2626", redTint:"#FEE2E2",
  orange:"#EA580C", orangeTint:"#FFEDD5",
  violet:"#7C3AED", violetTint:"#EDE9FE", violetTint2:"#DDD6FE",
  indigo:"#4338CA", indigoTint:"#EEF2FF",
  teal:"#0D9488", tealTint:"#F0FDFA", tealTint2:"#CCFBF1",
};

const DATA_TYPES = ["TEXT","INTEGER","BIGINT","NUMERIC","BOOLEAN","DATE","TIMESTAMP","VARCHAR(255)"];
const JOIN_TYPES = ["INNER JOIN","LEFT JOIN","RIGHT JOIN","FULL OUTER JOIN","CROSS JOIN"];
const AGG_FUNCS  = ["COUNT","SUM","AVG","MIN","MAX","COUNT DISTINCT"];
const CONDITIONS = ["=","!=",">",">=","<","<=","LIKE","IS NULL","IS NOT NULL","IN","NOT IN"];
const LOGIC_OPS  = ["AND","OR"];
const ORDER_DIRS = ["ASC","DESC"];

// ── Shared sub-components ─────────────────────────────────────────────────────

function StepHeader({ n, label, color = C.blue }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 10, marginTop: 6 }}>
      <span style={{
        width: 24, height: 24, borderRadius: "50%",
        background: color, color: C.white,
        fontSize: 11, fontWeight: 800, flexShrink: 0,
        display: "flex", alignItems: "center", justifyContent: "center",
        boxShadow: `0 2px 6px ${color}44`,
      }}>{n}</span>
      <span style={{ fontSize: 12, fontWeight: 700, color: C.g700 }}>{label}</span>
    </div>
  );
}

function Label({ children, required }) {
  return (
    <div style={{ fontSize: 11, fontWeight: 700, color: C.g600, marginBottom: 5 }}>
      {children}{required && <span style={{ color: C.red, marginLeft: 3 }}>*</span>}
    </div>
  );
}

function Input({ value, onChange, placeholder, type = "text", mono = false }) {
  return (
    <input type={type} value={value} onChange={e => onChange(e.target.value)} placeholder={placeholder}
      style={{ width: "100%", padding: "8px 10px", borderRadius: 7, border: `1px solid ${C.g200}`, fontSize: 12, boxSizing: "border-box", outline: "none", color: C.g700, fontFamily: mono ? "monospace" : "inherit", background: C.white }} />
  );
}

function Select({ value, onChange, options, placeholder }) {
  return (
    <select value={value} onChange={e => onChange(e.target.value)}
      style={{ width: "100%", padding: "8px 10px", borderRadius: 7, border: `1px solid ${C.g200}`, fontSize: 12, outline: "none", color: value ? C.g700 : C.g400, background: C.white }}>
      {placeholder && <option value="">{placeholder}</option>}
      {options.map(o => typeof o === "string" ? <option key={o} value={o}>{o}</option> : <option key={o.value} value={o.value}>{o.label}</option>)}
    </select>
  );
}

function Tag({ label, onRemove }) {
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 4, padding: "3px 8px", borderRadius: 20, background: C.blueTint2, color: C.blue, fontSize: 11, fontWeight: 600 }}>
      {label}
      {onRemove && (
        <button onClick={onRemove} style={{ background: "none", border: "none", cursor: "pointer", color: C.blue, padding: 0, display: "flex", alignItems: "center", fontSize: 13, lineHeight: 1 }}>×</button>
      )}
    </span>
  );
}

function ColumnPicker({ columns, selected, onAdd, onRemove, onAddAll, onRemoveAll, singleSelect = false }) {
  const [open, setOpen] = useState(false);
  const available = columns.filter(c => !selected.includes(c));

  return (
    <div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 5, marginBottom: 8, minHeight: 28 }}>
        {selected.length === 0 && <span style={{ fontSize: 11, color: C.g400, fontStyle: "italic" }}>No columns selected</span>}
        {selected.map(c => <Tag key={c} label={c} onRemove={() => onRemove(c)} />)}
      </div>
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        <div style={{ position: "relative" }}>
          <button onClick={() => setOpen(v => !v)} style={{ padding: "5px 10px", borderRadius: 6, border: `1px solid ${C.blue}`, background: C.blueTint, color: C.blue, fontSize: 11, fontWeight: 700, cursor: "pointer" }}>
            + Add Column
          </button>
          {open && (
            <div style={{ position: "absolute", top: "100%", left: 0, zIndex: 100, background: C.white, border: `1px solid ${C.g200}`, borderRadius: 8, boxShadow: "0 4px 16px rgba(0,0,0,.12)", maxHeight: 180, overflowY: "auto", minWidth: 180, marginTop: 3 }}>
              {available.length === 0
                ? <div style={{ padding: "10px 14px", fontSize: 11, color: C.g400 }}>All columns selected</div>
                : available.map(c => (
                  <button key={c} onClick={() => { if (singleSelect) { selected.forEach(s => onRemove(s)); } onAdd(c); setOpen(false); }}
                    style={{ width: "100%", padding: "8px 14px", background: "none", border: "none", textAlign: "left", fontSize: 12, cursor: "pointer", color: C.g700 }}
                    onMouseEnter={e => e.currentTarget.style.background = C.g50}
                    onMouseLeave={e => e.currentTarget.style.background = "none"}>
                    {c}
                  </button>
                ))}
            </div>
          )}
        </div>
        {!singleSelect && onAddAll && (
          <button onClick={() => available.forEach(c => onAdd(c))} style={{ padding: "5px 10px", borderRadius: 6, border: `1px solid ${C.g300}`, background: C.g50, color: C.g600, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>
            Add All
          </button>
        )}
        {!singleSelect && onRemoveAll && selected.length > 0 && (
          <button onClick={onRemoveAll} style={{ padding: "5px 10px", borderRadius: 6, border: `1px solid ${C.redTint}`, background: C.redTint, color: C.red, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>
            Remove All
          </button>
        )}
      </div>
    </div>
  );
}

function SaveBar({ onSave, onClose, disabled = false, color = C.blue }) {
  return (
    <div style={{ display: "flex", gap: 8, marginTop: "auto", paddingTop: 16, borderTop: `1px solid ${C.g200}` }}>
      <button onClick={onClose} style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: `1px solid ${C.g200}`, background: C.white, color: C.g600, fontSize: 13, fontWeight: 600, cursor: "pointer" }}>Cancel</button>
      <button onClick={onSave} disabled={disabled} style={{ flex: 1, padding: "9px 0", borderRadius: 8, background: disabled ? C.g300 : color, border: "none", color: C.white, fontSize: 13, fontWeight: 700, cursor: disabled ? "not-allowed" : "pointer" }}>
        Save Configuration
      </button>
    </div>
  );
}

function Divider() {
  return <div style={{ height: 1, background: C.g100, margin: "16px 0" }} />;
}

// ── 1. Add Constant ───────────────────────────────────────────────────────────
function AddConstantConfig({ config, onSave, onClose }) {
  const [name,  setName]  = useState(config?.name  || "");
  const [value, setValue] = useState(config?.value || "");
  const [dtype, setDtype] = useState(config?.dtype || "TEXT");
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16, lineHeight: 1.6 }}>Add a new column with a constant value to your dataset.</p>
      <div style={{ display: "flex", flexDirection: "column", gap: 12, marginBottom: 16 }}>
        <div><Label required>Column Name</Label><Input value={name} onChange={setName} placeholder="e.g. country" /></div>
        <div><Label required>Constant Value</Label><Input value={value} onChange={setValue} placeholder='e.g. "Indonesia" or 1' /></div>
        <div><Label required>Data Type</Label><Select value={dtype} onChange={setDtype} options={DATA_TYPES} /></div>
      </div>
      {name && value && (
        <div style={{ marginTop: 12, background: C.blueTint, borderRadius: 7, padding: "8px 12px", fontSize: 11, color: C.blue }}>
          Preview: <code style={{ fontFamily: "monospace" }}>{name} = {value} ({dtype})</code>
        </div>
      )}
      <SaveBar onSave={() => onSave({ name, value, dtype })} onClose={onClose} disabled={!name || !value} />
    </div>
  );
}

// ── 2. Select Column ──────────────────────────────────────────────────────────
function SelectColumnConfig({ columns, config, onSave, onClose }) {
  const [selected, setSelected] = useState(config?.columns || []);
  const add = (c) => !selected.includes(c) && setSelected(s => [...s, c]);
  const remove = (c) => setSelected(s => s.filter(x => x !== c));
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Select columns to keep in the output.</p>
      <div style={{ marginBottom: 16 }}>
        <Label required>Columns to Select</Label>
        <ColumnPicker columns={columns} selected={selected} onAdd={add} onRemove={remove} onAddAll={() => columns.forEach(add)} onRemoveAll={() => setSelected([])} />
      </div>
      {selected.length > 0 && (
        <div style={{ marginTop: 10, background: C.greenTint, borderRadius: 7, padding: "8px 12px", fontSize: 11, color: C.green, marginBottom: 16 }}>
          ✓ {selected.length} column{selected.length > 1 ? "s" : ""} selected: {selected.join(", ")}
        </div>
      )}
      <SaveBar onSave={() => onSave({ columns: selected })} onClose={onClose} disabled={selected.length === 0} />
    </div>
  );
}

// ── 3. Rename Columns ─────────────────────────────────────────────────────────
function RenameColumnsConfig({ columns, config, onSave, onClose }) {
  const [selected, setSelected] = useState(Object.keys(config?.renames || {}));
  const [renames,  setRenames]  = useState(config?.renames || {});
  const add = (c) => { if (!selected.includes(c)) { setSelected(s => [...s, c]); setRenames(r => ({ ...r, [c]: r[c] || "" })); }};
  const remove = (c) => { setSelected(s => s.filter(x => x !== c)); setRenames(r => { const n = { ...r }; delete n[c]; return n; }); };
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Select columns and provide new names.</p>
      <Label required>Select Columns</Label>
      <ColumnPicker columns={columns} selected={selected} onAdd={add} onRemove={remove} onAddAll={() => columns.forEach(add)} onRemoveAll={() => { setSelected([]); setRenames({}); }} />
      {selected.length > 0 && (
        <div style={{ marginTop: 14, marginBottom: 16, overflowY: "auto", flex: 1 }}>
          <Label>New Column Names</Label>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {selected.map(c => (
              <div key={c} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <div style={{ flex: 1, padding: "7px 10px", borderRadius: 7, background: C.g100, fontSize: 12, color: C.g600, fontFamily: "monospace", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{c}</div>
                <span style={{ color: C.g400, fontSize: 12 }}>→</span>
                <div style={{ flex: 1 }}>
                  <input value={renames[c] || ""} onChange={e => setRenames(r => ({ ...r, [c]: e.target.value }))} placeholder={`new name for ${c}`}
                    style={{ width: "100%", padding: "7px 10px", borderRadius: 7, border: `1px solid ${C.g200}`, fontSize: 12, outline: "none", color: C.g700, boxSizing: "border-box", fontFamily: "monospace" }} />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
      <SaveBar onSave={() => onSave({ renames })} onClose={onClose} disabled={selected.length === 0 || selected.some(c => !renames[c])} />
    </div>
  );
}

// ── 4. Drop Columns ───────────────────────────────────────────────────────────
function DropColumnsConfig({ columns, config, onSave, onClose }) {
  const [selected, setSelected] = useState(config?.columns || []);
  const add = (c) => !selected.includes(c) && setSelected(s => [...s, c]);
  const remove = (c) => setSelected(s => s.filter(x => x !== c));
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Select columns to <strong>remove</strong> from the dataset.</p>
      <div style={{ marginBottom: 16 }}>
        <Label required>Columns to Drop</Label>
        <ColumnPicker columns={columns} selected={selected} onAdd={add} onRemove={remove} onAddAll={() => columns.forEach(add)} onRemoveAll={() => setSelected([])} />
      </div>
      {selected.length > 0 && (
        <div style={{ marginTop: 10, background: C.redTint, borderRadius: 7, padding: "8px 12px", fontSize: 11, color: C.red, marginBottom: 16 }}>
          ⚠ Will remove: {selected.join(", ")}
        </div>
      )}
      <SaveBar onSave={() => onSave({ columns: selected })} onClose={onClose} disabled={selected.length === 0} />
    </div>
  );
}

// ── 5. Order Table ────────────────────────────────────────────────────────────
function OrderTableConfig({ columns, config, onSave, onClose }) {
  const [orders, setOrders] = useState(config?.orders || []);
  const [open,   setOpen]   = useState(false);
  const usedCols = orders.map(o => o.col);
  const available = columns.filter(c => !usedCols.includes(c));
  const addCol = (c) => setOrders(o => [...o, { col: c, dir: "ASC" }]);
  const removeCol = (c) => setOrders(o => o.filter(x => x.col !== c));
  const setDir = (c, dir) => setOrders(o => o.map(x => x.col === c ? { ...x, dir } : x));
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Choose columns to sort by and set the order direction.</p>
      <Label required>Sort Columns</Label>
      {orders.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: 7, marginBottom: 10, overflowY: "auto", maxHeight: "250px" }}>
          {orders.map((o, i) => (
            <div key={o.col} style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <span style={{ width: 18, height: 18, borderRadius: "50%", background: C.blue, color: C.white, fontSize: 10, fontWeight: 700, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>{i+1}</span>
              <div style={{ flex: 1, padding: "6px 10px", borderRadius: 7, background: C.g100, fontSize: 12, color: C.g700, fontFamily: "monospace", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{o.col}</div>
              <select value={o.dir} onChange={e => setDir(o.col, e.target.value)}
                style={{ padding: "6px 8px", borderRadius: 7, border: `1px solid ${C.g200}`, fontSize: 11, fontWeight: 700, color: o.dir === "ASC" ? C.green : C.orange, background: C.white, width: 76 }}>
                {ORDER_DIRS.map(d => <option key={d}>{d}</option>)}
              </select>
              <button onClick={() => removeCol(o.col)} style={{ background: C.redTint, border: "none", borderRadius: 6, width: 24, height: 24, cursor: "pointer", color: C.red, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14 }}>×</button>
            </div>
          ))}
        </div>
      )}
      <div style={{ position: "relative", display: "inline-block", marginBottom: 16 }}>
        <button onClick={() => setOpen(v => !v)} style={{ padding: "5px 10px", borderRadius: 6, border: `1px solid ${C.blue}`, background: C.blueTint, color: C.blue, fontSize: 11, fontWeight: 700, cursor: "pointer" }}>+ Add Column</button>
        {open && (
          <div style={{ position: "absolute", top: "100%", left: 0, zIndex: 100, background: C.white, border: `1px solid ${C.g200}`, borderRadius: 8, boxShadow: "0 4px 16px rgba(0,0,0,.12)", maxHeight: 160, overflowY: "auto", minWidth: 180, marginTop: 3 }}>
            {available.length === 0 ? <div style={{ padding: "10px 14px", fontSize: 11, color: C.g400 }}>All columns added</div>
              : available.map(c => (
                <button key={c} onClick={() => { addCol(c); setOpen(false); }}
                  style={{ width: "100%", padding: "7px 14px", background: "none", border: "none", textAlign: "left", fontSize: 12, cursor: "pointer", color: C.g700 }}
                  onMouseEnter={e => e.currentTarget.style.background = C.g50}
                  onMouseLeave={e => e.currentTarget.style.background = "none"}>{c}</button>
              ))}
          </div>
        )}
      </div>
      {orders.length > 0 && (
        <div style={{ marginTop: 12, background: C.blueTint, borderRadius: 7, padding: "8px 12px", fontSize: 11, color: C.blue, marginBottom: 16 }}>
          ORDER BY {orders.map(o => `${o.col} ${o.dir}`).join(", ")}
        </div>
      )}
      <SaveBar onSave={() => onSave({ orders })} onClose={onClose} disabled={orders.length === 0} />
    </div>
  );
}

// ── 6. Change Data Type ───────────────────────────────────────────────────────
function ChangeDataTypeConfig({ columns, config, onSave, onClose }) {
  const [selected, setSelected] = useState(Object.keys(config?.types || {}));
  const [types, setTypes] = useState(config?.types || {});
  const add = (c) => { if (!selected.includes(c)) { setSelected(s => [...s, c]); setTypes(t => ({ ...t, [c]: t[c] || "TEXT" })); }};
  const remove = (c) => { setSelected(s => s.filter(x => x !== c)); setTypes(t => { const n = { ...t }; delete n[c]; return n; }); };
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Select columns and choose the new data type for each.</p>
      <Label required>Select Columns</Label>
      <ColumnPicker columns={columns} selected={selected} onAdd={add} onRemove={remove} onAddAll={() => columns.forEach(add)} onRemoveAll={() => { setSelected([]); setTypes({}); }} />
      {selected.length > 0 && (
        <div style={{ marginTop: 14, marginBottom: 16, overflowY: "auto", flex: 1 }}>
          <Label>New Data Types</Label>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {selected.map(c => (
              <div key={c} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <div style={{ flex: 1, padding: "7px 10px", borderRadius: 7, background: C.g100, fontSize: 12, color: C.g600, fontFamily: "monospace", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{c}</div>
                <span style={{ color: C.g400, fontSize: 12 }}>→</span>
                <select value={types[c] || "TEXT"} onChange={e => setTypes(t => ({ ...t, [c]: e.target.value }))}
                  style={{ flex: 1, padding: "7px 10px", borderRadius: 7, border: `1px solid ${C.g200}`, fontSize: 12, outline: "none", color: C.blue, fontWeight: 700, background: C.white }}>
                  {DATA_TYPES.map(d => <option key={d}>{d}</option>)}
                </select>
              </div>
            ))}
          </div>
        </div>
      )}
      <SaveBar onSave={() => onSave({ types })} onClose={onClose} disabled={selected.length === 0} />
    </div>
  );
}

// ── 7. Set Column Value ───────────────────────────────────────────────────────
function SetColumnValueConfig({ columns, config, onSave, onClose }) {
  const [targetCol, setTargetCol] = useState(config?.targetCol || "");
  const [sourceCol, setSourceCol] = useState(config?.sourceCol || "");
  const [useExpr, setUseExpr]     = useState(config?.useExpr || false);
  const [expr, setExpr]           = useState(config?.expr || "");
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Replace the value of a column with another column's value or an expression.</p>
      <div style={{ display: "flex", flexDirection: "column", gap: 12, marginBottom: 16 }}>
        <div><Label required>Target Column (to replace)</Label><Select value={targetCol} onChange={setTargetCol} options={columns} placeholder="— Select column —" /></div>
        <div style={{ display: "flex", gap: 8, marginBottom: 4 }}>
          <button onClick={() => setUseExpr(false)} style={{ flex: 1, padding: "7px 0", borderRadius: 7, border: `1px solid ${!useExpr ? C.blue : C.g200}`, background: !useExpr ? C.blueTint : C.white, color: !useExpr ? C.blue : C.g500, fontSize: 12, fontWeight: 600, cursor: "pointer" }}>From Column</button>
          <button onClick={() => setUseExpr(true)}  style={{ flex: 1, padding: "7px 0", borderRadius: 7, border: `1px solid ${useExpr ? C.blue : C.g200}`, background: useExpr ? C.blueTint : C.white, color: useExpr ? C.blue : C.g500, fontSize: 12, fontWeight: 600, cursor: "pointer" }}>Expression</button>
        </div>
        {!useExpr
          ? <div><Label required>Source Column</Label><Select value={sourceCol} onChange={setSourceCol} options={columns.filter(c => c !== targetCol)} placeholder="— Select column —" /></div>
          : <div><Label required>SQL Expression</Label><Input value={expr} onChange={setExpr} placeholder="e.g. UPPER(name) or price * 1.1" mono /></div>}
      </div>
      {targetCol && (sourceCol || expr) && (
        <div style={{ marginTop: 12, background: C.blueTint, borderRadius: 7, padding: "8px 12px", fontSize: 11, color: C.blue, fontFamily: "monospace", marginBottom: 16 }}>
          UPDATE {targetCol} = {useExpr ? expr : sourceCol}
        </div>
      )}
      <SaveBar onSave={() => onSave({ targetCol, sourceCol, useExpr, expr })} onClose={onClose} disabled={!targetCol || (!sourceCol && !expr)} />
    </div>
  );
}

// ── 8. Value Mapper ───────────────────────────────────────────────────────────
function ValueMapperConfig({ columns, config, onSave, onClose }) {
  const [sourceCol,  setSourceCol]  = useState(config?.sourceCol  || "");
  const [newColName, setNewColName] = useState(config?.newColName || "");
  const [elseValue,  setElseValue]  = useState(config?.elseValue  || "");
  const [whens, setWhens] = useState(config?.whens || [{ logic: "AND", condition: "=", value: "", result: "" }]);
  const addWhen = () => setWhens(w => [...w, { logic: "AND", condition: "=", value: "", result: "" }]);
  const removeWhen = (i) => setWhens(w => w.filter((_, idx) => idx !== i));
  const updateWhen = (i, field, val) => setWhens(w => w.map((x, idx) => idx === i ? { ...x, [field]: val } : x));
  const preview = whens.every(w => w.value && w.result) && newColName && elseValue;
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16, lineHeight: 1.6 }}>Map values to a new column using conditional logic (SQL CASE WHEN).</p>
      <div style={{ display: "flex", flexDirection: "column", gap: 12, marginBottom: 16, overflowY: "auto", flex: 1, paddingRight: 4 }}>
        <div style={{ display: "flex", gap: 10 }}>
          <div style={{ flex: 1 }}><Label required>Source Column</Label><Select value={sourceCol} onChange={setSourceCol} options={columns} placeholder="— Select —" /></div>
          <div style={{ flex: 1 }}><Label required>New Column Name</Label><Input value={newColName} onChange={setNewColName} placeholder="e.g. member_level" /></div>
        </div>
        <div>
          <Label required>Conditions</Label>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {whens.map((w, i) => (
              <div key={i} style={{ background: C.g50, borderRadius: 8, padding: 10, border: `1px solid ${C.g200}` }}>
                <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
                  {i > 0 && <select value={w.logic} onChange={e => updateWhen(i, "logic", e.target.value)} style={{ width: 64, padding: "4px 6px", borderRadius: 6, border: `1px solid ${C.g200}`, fontSize: 11, fontWeight: 700, color: C.orange, background: C.white }}>{LOGIC_OPS.map(l => <option key={l}>{l}</option>)}</select>}
                  <span style={{ fontSize: 11, fontWeight: 700, color: C.blue }}>WHEN #{i + 1}</span>
                  {i > 0 && <button onClick={() => removeWhen(i)} style={{ marginLeft: "auto", background: C.redTint, border: "none", borderRadius: 5, padding: "2px 7px", cursor: "pointer", color: C.red, fontSize: 11 }}>Remove</button>}
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 6 }}>
                  <div><div style={{ fontSize: 10, color: C.g400, marginBottom: 3 }}>Column</div><div style={{ padding: "6px 8px", borderRadius: 6, background: C.blueTint, fontSize: 11, color: C.blue, fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{sourceCol || "—"}</div></div>
                  <div><div style={{ fontSize: 10, color: C.g400, marginBottom: 3 }}>Condition</div><select value={w.condition} onChange={e => updateWhen(i, "condition", e.target.value)} style={{ width: "100%", padding: "6px 6px", borderRadius: 6, border: `1px solid ${C.g200}`, fontSize: 11, background: C.white, color: C.g700 }}>{CONDITIONS.map(c => <option key={c}>{c}</option>)}</select></div>
                  <div><div style={{ fontSize: 10, color: C.g400, marginBottom: 3 }}>Compare</div><input value={w.value} onChange={e => updateWhen(i, "value", e.target.value)} placeholder="1000" style={{ width: "100%", padding: "6px 7px", borderRadius: 6, border: `1px solid ${C.g200}`, fontSize: 11, boxSizing: "border-box", outline: "none", color: C.g700 }} /></div>
                  <div><div style={{ fontSize: 10, color: C.g400, marginBottom: 3 }}>Result</div><input value={w.result} onChange={e => updateWhen(i, "result", e.target.value)} placeholder="silver" style={{ width: "100%", padding: "6px 7px", borderRadius: 6, border: `1px solid ${C.g200}`, fontSize: 11, boxSizing: "border-box", outline: "none", color: C.g700 }} /></div>
                </div>
              </div>
            ))}
          </div>
          <button onClick={addWhen} style={{ marginTop: 8, padding: "5px 12px", borderRadius: 6, border: `1px dashed ${C.blue}`, background: C.blueTint, color: C.blue, fontSize: 11, fontWeight: 700, cursor: "pointer" }}>+ Add WHEN</button>
        </div>
        <div><Label required>ELSE Value</Label><Input value={elseValue} onChange={setElseValue} placeholder="e.g. bronze" /></div>
      </div>
      {preview && (
        <div style={{ background: "#1e293b", borderRadius: 8, padding: 12, fontFamily: "monospace", fontSize: 11, color: "#93c5fd", lineHeight: 1.7, marginBottom: 16 }}>
          CASE<br/>{whens.map((w, i) => <span key={i}>&nbsp;&nbsp;{i > 0 ? w.logic : "WHEN"} {sourceCol} {w.condition} '{w.value}' THEN '{w.result}'<br/></span>)}
          &nbsp;&nbsp;ELSE '{elseValue}'<br/>END AS {newColName}
        </div>
      )}
      <SaveBar onSave={() => onSave({ sourceCol, newColName, elseValue, whens })} onClose={onClose} disabled={!sourceCol || !newColName || !elseValue || whens.some(w => !w.value || !w.result)} />
    </div>
  );
}

// ── 9. Fill NULL ──────────────────────────────────────────────────────────────
function FillNullConfig({ columns, config, onSave, onClose }) {
  const [selected, setSelected] = useState(config?.columns || []);
  const [fillValue, setFillValue] = useState(config?.fillValue || "");
  const [fillType, setFillType]   = useState(config?.fillType || "value");
  const add = (c) => !selected.includes(c) && setSelected(s => [...s, c]);
  const remove = (c) => setSelected(s => s.filter(x => x !== c));
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Replace NULL values in selected columns.</p>
      <div style={{ display: "flex", flexDirection: "column", gap: 12, marginBottom: 16 }}>
        <div><Label required>Select Columns</Label><ColumnPicker columns={columns} selected={selected} onAdd={add} onRemove={remove} onAddAll={() => columns.forEach(add)} onRemoveAll={() => setSelected([])} /></div>
        <div>
          <Label required>Fill Method</Label>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3,1fr)", gap: 6, marginBottom: 10 }}>
            {[["value","Custom Value"],["mean","Mean"],["median","Median"],["mode","Mode"],["forward","Forward Fill"],["backward","Backward Fill"]].map(([v,l]) => (
              <button key={v} onClick={() => setFillType(v)} style={{ padding: "6px 0", borderRadius: 7, border: `1px solid ${fillType===v?C.blue:C.g200}`, background: fillType===v?C.blueTint:C.white, color: fillType===v?C.blue:C.g500, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>{l}</button>
            ))}
          </div>
        </div>
        {fillType === "value" && <div><Label required>Fill Value</Label><Input value={fillValue} onChange={setFillValue} placeholder='e.g. 0 or "Unknown"' /></div>}
      </div>
      {selected.length > 0 && <div style={{ marginTop: 12, background: C.blueTint, borderRadius: 7, padding: "8px 12px", fontSize: 11, color: C.blue, marginBottom: 16 }}>Fill NULL in [{selected.join(", ")}] with {fillType === "value" ? `"${fillValue}"` : fillType}</div>}
      <SaveBar onSave={() => onSave({ columns: selected, fillValue, fillType })} onClose={onClose} disabled={selected.length === 0 || (fillType === "value" && !fillValue)} />
    </div>
  );
}

// ── 10. Filter Rows ───────────────────────────────────────────────────────────
function FilterRowsConfig({ columns, config, onSave, onClose }) {
  const [formula, setFormula]   = useState(config?.formula || "");
  const [error, setError]       = useState("");
  const [validated, setValidated] = useState(false);
  const validate = () => {
    if (!formula.trim()) { setError("Formula cannot be empty"); setValidated(false); return; }
    const dangerous = ["DROP","DELETE","INSERT","UPDATE","TRUNCATE","ALTER","CREATE","EXEC","EXECUTE"];
    if (dangerous.some(d => formula.toUpperCase().includes(d))) { setError("Dangerous SQL keywords detected"); setValidated(false); return; }
    setError(""); setValidated(true);
  };
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 12, lineHeight: 1.6 }}>Filter rows using a SQL WHERE clause. Rows where the condition is TRUE will be kept.</p>
      <div style={{ marginBottom: 10 }}>
        <Label required>Filter Formula (SQL WHERE clause)</Label>
        <textarea value={formula} onChange={e => { setFormula(e.target.value); setValidated(false); setError(""); }} placeholder={"e.g. age > 18\nor: city = 'Jakarta' AND salary >= 5000000"} rows={4}
          style={{ width: "100%", padding: "10px 12px", borderRadius: 8, border: `1.5px solid ${error ? C.red : validated ? C.green : C.g200}`, fontSize: 12, boxSizing: "border-box", outline: "none", color: C.g700, resize: "vertical", fontFamily: "monospace", lineHeight: 1.7 }} />
        {error    && <div style={{ marginTop: 5, fontSize: 11, color: C.red   }}>❌ {error}</div>}
        {validated && <div style={{ marginTop: 5, fontSize: 11, color: C.green }}>✓ Formula looks valid</div>}
      </div>
      <div style={{ marginBottom: 12, overflowY: "auto", maxHeight: "120px" }}>
        <div style={{ fontSize: 11, color: C.g400, marginBottom: 6 }}>Available columns (click to insert):</div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 5 }}>
          {columns.map(c => <button key={c} onClick={() => setFormula(f => f + (f ? " AND " : "") + c)} style={{ padding: "3px 9px", borderRadius: 5, border: `1px solid ${C.g200}`, background: C.g50, color: C.g600, fontSize: 11, cursor: "pointer", fontFamily: "monospace" }}>{c}</button>)}
        </div>
      </div>
      <button onClick={validate} style={{ alignSelf: "flex-start", padding: "6px 14px", borderRadius: 7, border: `1px solid ${C.blue}`, background: C.blueTint, color: C.blue, fontSize: 12, fontWeight: 700, cursor: "pointer", marginBottom: 16 }}>Validate Formula</button>
      <SaveBar onSave={() => onSave({ formula })} onClose={onClose} disabled={!formula || !!error} />
    </div>
  );
}

// ── 11. Group By & Aggregate ──────────────────────────────────────────────────
function GroupByAggConfig({ columns, config, onSave, onClose }) {
  const [groupCols, setGroupCols] = useState(config?.groupCols || []);
  const [aggCols, setAggCols]     = useState(config?.aggCols   || []);
  const [openAgg, setOpenAgg]     = useState(false);
  const addGroup = (c) => !groupCols.includes(c) && setGroupCols(g => [...g, c]);
  const removeGroup = (c) => setGroupCols(g => g.filter(x => x !== c));
  const addAgg = (c) => setAggCols(a => [...a, { col: c, func: "COUNT", alias: `${c.toLowerCase()}_count` }]);
  const removeAgg = (i) => setAggCols(a => a.filter((_, idx) => idx !== i));
  const updateAgg = (i, field, val) => setAggCols(a => a.map((x, idx) => idx === i ? { ...x, [field]: val } : x));
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Group data by columns and compute aggregations.</p>
      <div style={{ display: "flex", flexDirection: "column", gap: 14, marginBottom: 16, overflowY: "auto", flex: 1, paddingRight: 4 }}>
        <div><Label required>Group By Columns</Label><ColumnPicker columns={columns} selected={groupCols} onAdd={addGroup} onRemove={removeGroup} onAddAll={() => columns.forEach(addGroup)} onRemoveAll={() => setGroupCols([])} /></div>
        <div>
          <Label required>Aggregate Columns</Label>
          {aggCols.length > 0 && (
            <div style={{ display: "flex", flexDirection: "column", gap: 7, marginBottom: 8 }}>
              {aggCols.map((a, i) => (
                <div key={i} style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr auto", gap: 6, alignItems: "center", background: C.g50, borderRadius: 7, padding: 8, border: `1px solid ${C.g200}` }}>
                  <div style={{ fontSize: 11, fontFamily: "monospace", color: C.g600, padding: "4px 6px", background: C.g100, borderRadius: 5, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{a.col}</div>
                  <select value={a.func} onChange={e => updateAgg(i, "func", e.target.value)} style={{ padding: "5px 7px", borderRadius: 6, border: `1px solid ${C.g200}`, fontSize: 11, fontWeight: 700, color: C.blue, background: C.white }}>{AGG_FUNCS.map(f => <option key={f}>{f}</option>)}</select>
                  <input value={a.alias} onChange={e => updateAgg(i, "alias", e.target.value)} placeholder="alias" style={{ padding: "5px 7px", borderRadius: 6, border: `1px solid ${C.g200}`, fontSize: 11, outline: "none", color: C.g700, fontFamily: "monospace", width: "100%", boxSizing: "border-box" }} />
                  <button onClick={() => removeAgg(i)} style={{ background: C.redTint, border: "none", borderRadius: 5, width: 24, height: 24, cursor: "pointer", color: C.red, fontSize: 14, display: "flex", alignItems: "center", justifyContent: "center" }}>×</button>
                </div>
              ))}
            </div>
          )}
          <div style={{ position: "relative", display: "inline-block" }}>
            <button onClick={() => setOpenAgg(v => !v)} style={{ padding: "5px 10px", borderRadius: 6, border: `1px solid ${C.gold}`, background: C.goldTint, color: C.gold, fontSize: 11, fontWeight: 700, cursor: "pointer" }}>+ Add Aggregate Column</button>
            {openAgg && (
              <div style={{ position: "absolute", top: "100%", left: 0, zIndex: 100, background: C.white, border: `1px solid ${C.g200}`, borderRadius: 8, boxShadow: "0 4px 16px rgba(0,0,0,.12)", maxHeight: 160, overflowY: "auto", minWidth: 180, marginTop: 3 }}>
                {columns.map(c => <button key={c} onClick={() => { addAgg(c); setOpenAgg(false); }} style={{ width: "100%", padding: "7px 14px", background: "none", border: "none", textAlign: "left", fontSize: 12, cursor: "pointer", color: C.g700 }} onMouseEnter={e => e.currentTarget.style.background = C.g50} onMouseLeave={e => e.currentTarget.style.background = "none"}>{c}</button>)}
              </div>
            )}
          </div>
        </div>
      </div>
      {groupCols.length > 0 && aggCols.length > 0 && (
        <div style={{ background: "#1e293b", borderRadius: 8, padding: 12, fontFamily: "monospace", fontSize: 11, color: "#93c5fd", lineHeight: 1.8, marginBottom: 16 }}>
          SELECT {groupCols.join(", ")}, {aggCols.map(a => `${a.func}(${a.col}) AS ${a.alias}`).join(", ")}<br/>
          FROM table GROUP BY {groupCols.join(", ")}
        </div>
      )}
      <SaveBar onSave={() => onSave({ groupCols, aggCols })} onClose={onClose} disabled={groupCols.length === 0 || aggCols.length === 0} />
    </div>
  );
}

// ── 12. Join Data ─────────────────────────────────────────────────────────────
function JoinDataConfig({ columns, config, onSave, onClose, allNodes }) {
  const [joinType, setJoinType]     = useState(config?.joinType    || "INNER JOIN");
  const [leftCol, setLeftCol]       = useState(config?.leftCol     || "");
  const [rightCol, setRightCol]     = useState(config?.rightCol    || "");
  const [rightNodeId, setRightNodeId] = useState(config?.rightNodeId || "");
  const inputNodes = (allNodes || []).filter(n => n.data?.type === "input_dataset" && n.data?.config?.dataset);
  const rightDataset = inputNodes.find(n => n.id === rightNodeId)?.data?.config?.dataset;
  const rightColumns = rightDataset?.columns || [];
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16, lineHeight: 1.6 }}>Join two datasets. The first connected node is the <strong>left table</strong>.</p>
      <div style={{ display: "flex", flexDirection: "column", gap: 12, marginBottom: 16, overflowY: "auto", flex: 1 }}>
        <div>
          <Label required>Join Type</Label>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3,1fr)", gap: 6 }}>
            {JOIN_TYPES.map(jt => <button key={jt} onClick={() => setJoinType(jt)} style={{ padding: "7px 4px", borderRadius: 7, border: `1px solid ${joinType===jt?C.blue:C.g200}`, background: joinType===jt?C.blueTint:C.white, color: joinType===jt?C.blue:C.g500, fontSize: 10, fontWeight: 700, cursor: "pointer" }}>{jt.replace(" JOIN","")}</button>)}
          </div>
        </div>
        {inputNodes.length > 0 && <div><Label required>Right Table (second dataset)</Label><Select value={rightNodeId} onChange={setRightNodeId} options={inputNodes.map(n => ({ value: n.id, label: n.data.config.dataset.name }))} placeholder="— Select second dataset —" /></div>}
        <div style={{ display: "grid", gridTemplateColumns: "1fr auto 1fr", gap: 10, alignItems: "end" }}>
          <div><Label required>Left Table Column</Label><Select value={leftCol} onChange={setLeftCol} options={columns} placeholder="— Select —" /></div>
          <div style={{ padding: "8px 0", color: C.g400, fontSize: 12, textAlign: "center" }}>=</div>
          <div><Label required>Right Table Column</Label><Select value={rightCol} onChange={setRightCol} options={rightColumns} placeholder="— Select —" /></div>
        </div>
      </div>
      {leftCol && rightCol && (
        <div style={{ background: "#1e293b", borderRadius: 8, padding: 12, fontFamily: "monospace", fontSize: 11, color: "#93c5fd", lineHeight: 1.8, marginBottom: 16 }}>
          FROM left_table<br/>{joinType} right_table<br/>ON left_table.{leftCol} = right_table.{rightCol}
        </div>
      )}
      <SaveBar onSave={() => onSave({ joinType, leftCol, rightCol, rightNodeId })} onClose={onClose} disabled={!leftCol || !rightCol} />
    </div>
  );
}

// ── 13. PySpark ───────────────────────────────────────────────────────────────
function PySparkConfig({ config, onSave, onClose }) {
  const [code, setCode]         = useState(config?.code || `# Input dataframe is available as 'df'\ndf = df.filter(df['age'] > 18)\n# The result 'df' will be passed to the next node`);
  const [nodeName, setNodeName] = useState(config?.nodeName || "PySpark Node");
  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 14, lineHeight: 1.6 }}>Write custom PySpark code. Input dataframe available as <code style={{ background: C.g100, padding: "1px 4px", borderRadius: 3 }}>df</code>.</p>
      <div style={{ marginBottom: 12 }}><Label>Node Label</Label><Input value={nodeName} onChange={setNodeName} placeholder="e.g. Clean Names" /></div>
      <div style={{ display: "flex", flexDirection: "column", flex: 1, marginBottom: 16 }}>
        <Label required>Python / PySpark Code</Label>
        <div style={{ background: "#0f172a", borderRadius: 8, padding: 4, border: `1px solid ${C.g700}`, display: "flex", flexDirection: "column", flex: 1 }}>
          <div style={{ padding: "4px 10px", background: "#1e293b", borderRadius: "6px 6px 0 0", display: "flex", gap: 5 }}>
            {["#f87171","#fbbf24","#34d399"].map((c, i) => <span key={i} style={{ width: 10, height: 10, borderRadius: "50%", background: c, display: "inline-block" }} />)}
            <span style={{ marginLeft: 8, fontSize: 10, color: C.g400 }}>python</span>
          </div>
          <textarea value={code} onChange={e => setCode(e.target.value)} spellCheck={false}
            style={{ width: "100%", padding: "12px 14px", background: "transparent", border: "none", outline: "none", color: "#93c5fd", fontSize: 12, fontFamily: "monospace", resize: "none", lineHeight: 1.7, boxSizing: "border-box", flex: 1 }} />
        </div>
      </div>
      <SaveBar onSave={() => onSave({ code, nodeName })} onClose={onClose} disabled={!code.trim()} />
    </div>
  );
}

// ── NEW NODE: Calculator ──────────────────────────────────────────────────────
function CalculatorNodeConfig({ columns, config, onSave, onClose }) {
  const [newColName, setNewColName] = useState(config?.newColName || "");
  const [operation, setOperation]   = useState(config?.operation  || "+");
  const [colA, setColA]             = useState(config?.colA       || "");
  const [colB, setColB]             = useState(config?.colB       || "");

  const OPERATIONS = [
    { sym: "+", label: "Add",      value: "+" },
    { sym: "−", label: "Subtract", value: "-" },
    { sym: "×", label: "Multiply", value: "*" },
    { sym: "÷", label: "Divide",   value: "/" },
  ];

  const opDisplay = { "+": "+", "-": "−", "*": "×", "/": "÷" }[operation] || operation;
  const isValid   = newColName.trim() && colA && colB && colA !== colB;
  const isReplace = newColName.trim() && columns.includes(newColName.trim());

  const sqlPreview = isValid
    ? `"${newColName}" = "${colA}" ${opDisplay} "${colB}"${operation === "/" ? "  (÷0 → NULL)" : ""}`
    : null;

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <div style={{ background: C.violetTint, borderRadius: 10, padding: "10px 14px", marginBottom: 18, fontSize: 11, color: C.violet, lineHeight: 1.7 }}>
        Performs arithmetic between two numeric columns and stores the result in a new (or existing) column.
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 12, marginBottom: 16, overflowY: "auto", flex: 1, paddingRight: 4 }}>
        {/* Step 1 */}
        <StepHeader n={1} label="Name your new calculated column" color={C.violet} />
        <div style={{ marginBottom: 6 }}>
          <Input value={newColName} onChange={setNewColName} placeholder="e.g. total_revenue, profit_margin" />
          {isReplace && (
            <div style={{ marginTop: 6, fontSize: 11, color: C.orange, background: C.orangeTint, borderRadius: 6, padding: "5px 10px" }}>
              ⚠ Column "<strong>{newColName}</strong>" already exists — its values will be replaced.
            </div>
          )}
          {newColName && !isReplace && (
            <div style={{ marginTop: 6, fontSize: 11, color: C.green, background: C.greenTint, borderRadius: 6, padding: "5px 10px" }}>
              ✓ New column "<strong>{newColName}</strong>" will be added to the table.
            </div>
          )}
        </div>

        <Divider />

        {/* Step 2 */}
        <StepHeader n={2} label="Choose operation" color={C.violet} />
        <div style={{ display: "flex", gap: 8 }}>
          {OPERATIONS.map(op => (
            <button key={op.value} onClick={() => setOperation(op.value)}
              style={{
                flex: 1, padding: "10px 4px", borderRadius: 10, cursor: "pointer",
                border: `2px solid ${operation === op.value ? C.violet : C.g200}`,
                background: operation === op.value ? C.violetTint : C.white,
                color: operation === op.value ? C.violet : C.g400,
                display: "flex", flexDirection: "column", alignItems: "center", gap: 5,
                transition: "all .12s",
              }}>
              <span style={{ fontSize: 20, fontWeight: 800, lineHeight: 1 }}>{op.sym}</span>
              <span style={{ fontSize: 9, fontWeight: 600 }}>{op.label}</span>
            </button>
          ))}
        </div>

        <Divider />

        {/* Step 3 */}
        <StepHeader n={3} label="Select columns to calculate" color={C.violet} />
        <div style={{ display: "grid", gridTemplateColumns: "1fr 44px 1fr", gap: 10, alignItems: "end", marginBottom: 14 }}>
          <div>
            <Label required>Column (A)</Label>
            <Select value={colA} onChange={setColA} options={columns} placeholder="— Select —" />
          </div>
          <div style={{ display: "flex", alignItems: "flex-end", paddingBottom: 2 }}>
            <div style={{
              width: 44, height: 36, borderRadius: 8,
              background: C.violetTint, border: `2px solid ${C.violetTint2}`,
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 20, fontWeight: 800, color: C.violet,
            }}>
              {opDisplay}
            </div>
          </div>
          <div>
            <Label required>Column (B)</Label>
            <Select value={colB} onChange={v => { if (v !== colA) setColB(v); }} options={columns.filter(c => c !== colA)} placeholder="— Select —" />
          </div>
        </div>

        <div style={{ fontSize: 11, color: C.g400, background: C.g50, borderRadius: 7, padding: "7px 12px" }}>
          💡 Ensure both columns contain <strong>numeric</strong> values.
        </div>
      </div>

      {sqlPreview && (
        <div style={{ background: "#1e293b", borderRadius: 8, padding: "10px 14px", fontFamily: "monospace", fontSize: 12, color: "#a5b4fc", marginBottom: 16 }}>
          <div style={{ color: C.g400, fontSize: 10, marginBottom: 4 }}>SQL Preview</div>
          {sqlPreview}
        </div>
      )}

      <SaveBar
        onSave={() => onSave({ newColName: newColName.trim(), operation, colA, colB })}
        onClose={onClose}
        disabled={!isValid}
        color={C.violet}
      />
    </div>
  );
}

// ── NEW NODE: Advance Calculator ──────────────────────────────────────────────
const SCI_OPS = [
  { value: "sin",     label: "sin",   hint: "sin(x)",       args: 1, color: "#0ea5e9" },
  { value: "cos",     label: "cos",   hint: "cos(x)",       args: 1, color: "#0ea5e9" },
  { value: "sqrt",    label: "√",     hint: "sqrt(x)",      args: 1, color: "#10b981" },
  { value: "radians", label: "rad",   hint: "radians(x)",   args: 1, color: "#10b981" },
  { value: "atan2",   label: "atan2", hint: "atan2(y, x)",  args: 2, color: "#f59e0b" },
  { value: "power",   label: "xⁿ",   hint: "power(x, exp)", args: 2, color: "#f59e0b" },
];

function AdvCalcRow({ row, index, columns, onChange, onRemove, isOnly }) {
  const op    = SCI_OPS.find(o => o.value === row.operation) || SCI_OPS[0];
  const needs2 = op.args === 2;

  return (
    <div style={{ background: C.g50, borderRadius: 10, padding: 14, border: `1px solid ${C.g200}`, marginBottom: 10, position: "relative" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ width: 22, height: 22, borderRadius: "50%", background: C.indigo, color: C.white, fontSize: 11, fontWeight: 800, display: "flex", alignItems: "center", justifyContent: "center" }}>{index + 1}</span>
          <span style={{ fontSize: 11, fontWeight: 700, color: C.g600 }}>Calculation #{index + 1}</span>
        </div>
        {!isOnly && (
          <button onClick={onRemove} style={{ width: 26, height: 26, borderRadius: 6, background: C.redTint, border: "none", color: C.red, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 15, fontWeight: 700 }}>×</button>
        )}
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        <div>
          <Label required>Name your new calculated column</Label>
          <Input value={row.newColName} onChange={v => onChange("newColName", v)} placeholder="e.g. sin_angle, distance_km" />
        </div>

        <div>
          <Label required>Choose operation</Label>
          <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
            {SCI_OPS.map(o => (
              <button key={o.value} onClick={() => { onChange("operation", o.value); if (o.args === 1) onChange("colB", ""); }}
                style={{
                  padding: "6px 10px", borderRadius: 20, cursor: "pointer",
                  border: `2px solid ${row.operation === o.value ? o.color : C.g200}`,
                  background: row.operation === o.value ? `${o.color}18` : C.white,
                  color: row.operation === o.value ? o.color : C.g500,
                  fontSize: 11, fontWeight: 700, transition: "all .1s",
                }}>
                {o.label}
                <span style={{ fontSize: 9, fontWeight: 400, marginLeft: 5, opacity: 0.7 }}>{o.hint}</span>
              </button>
            ))}
          </div>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: needs2 ? "1fr 1fr" : "1fr", gap: 10 }}>
          <div>
            <Label required>{needs2 ? `Column A (${op.value === "atan2" ? "y" : "base"})` : "Select column"}</Label>
            <Select value={row.colA} onChange={v => onChange("colA", v)} options={columns} placeholder="— Select column —" />
          </div>
          {needs2 && (
            <div>
              <Label required>{op.value === "atan2" ? "Column B (x)" : "Column B (exp)"}</Label>
              <Select value={row.colB} onChange={v => onChange("colB", v)} options={columns.filter(c => c !== row.colA)} placeholder="— Select column —" />
            </div>
          )}
        </div>

        {row.newColName && row.colA && (
          <div style={{ background: "#1e293b", borderRadius: 7, padding: "7px 12px", fontFamily: "monospace", fontSize: 11, color: "#a5b4fc" }}>
            {`"${row.newColName}" = ${op.value}("${row.colA}"${needs2 && row.colB ? `, "${row.colB}"` : ""})`}
          </div>
        )}
      </div>
    </div>
  );
}

function AdvanceCalculatorConfig({ columns, config, onSave, onClose }) {
  const [calculations, setCalculations] = useState(
    config?.calculations?.length
      ? config.calculations
      : [{ newColName: "", operation: "sin", colA: "", colB: "" }]
  );

  const addCalc = () => setCalculations(c => [...c, { newColName: "", operation: "sin", colA: "", colB: "" }]);
  const removeCalc = (i) => setCalculations(c => c.filter((_, idx) => idx !== i));
  const updateCalc = (i, field, val) => setCalculations(c => c.map((x, idx) => idx === i ? { ...x, [field]: val } : x));

  const isRowValid = (row) => {
    const op = SCI_OPS.find(o => o.value === row.operation);
    if (!row.newColName || !row.colA) return false;
    if (op?.args === 2 && !row.colB) return false;
    return true;
  };
  const allValid = calculations.length > 0 && calculations.every(isRowValid);

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <div style={{ background: C.indigoTint, borderRadius: 10, padding: "10px 14px", marginBottom: 14, fontSize: 11, color: C.indigo, lineHeight: 1.7 }}>
        Apply scientific math functions. Each calculation produces a new column.
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 4, marginBottom: 16, overflowY: "auto", flex: 1, paddingRight: 4 }}>
        {calculations.map((row, i) => (
          <AdvCalcRow
            key={i} row={row} index={i} columns={columns}
            onChange={(field, val) => updateCalc(i, field, val)}
            onRemove={() => removeCalc(i)}
            isOnly={calculations.length === 1}
          />
        ))}

        <button onClick={addCalc}
          style={{ width: "100%", padding: "10px 0", borderRadius: 8, border: `2px dashed ${C.indigo}`, background: C.indigoTint, color: C.indigo, fontSize: 12, fontWeight: 700, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 6, marginBottom: 4 }}>
          <span style={{ fontSize: 18, lineHeight: 1 }}>+</span> Add Another Calculation
        </button>
      </div>

      <SaveBar
        onSave={() => onSave({ calculations })}
        onClose={onClose}
        disabled={!allValid}
        color={C.indigo}
      />
    </div>
  );
}

// ── NEW NODE: Combine Columns ─────────────────────────────────────────────────
const SEPARATOR_PRESETS = [
  { label: "Space",      value: " ",   display: '" "' },
  { label: "Comma",      value: ", ",  display: '", "' },
  { label: "Dash",       value: " - ", display: '" - "' },
  { label: "Underscore", value: "_",   display: '"_"' },
  { label: "Pipe",       value: " | ", display: '" | "' },
  { label: "None",       value: "",    display: '""' },
];

function CombineColumnsConfig({ columns, config, onSave, onClose }) {
  const [newColName,      setNewColName]      = useState(config?.newColName      || "");
  const [separatorPreset, setSeparatorPreset] = useState(config?.separatorPreset || " ");
  const [customSep,       setCustomSep]       = useState(config?.customSep       || "");
  const [useCustom,       setUseCustom]       = useState(config?.useCustom       || false);
  const [removeOriginal,  setRemoveOriginal]  = useState(config?.removeOriginal  || false);
  const [selectedCols,    setSelectedCols]    = useState(config?.selectedCols    || []);

  const separator  = useCustom ? customSep : separatorPreset;
  const isValid    = newColName.trim() && selectedCols.length >= 2;
  const isReplace  = newColName.trim() && columns.includes(newColName.trim());

  const addCol = (c) => { if (!selectedCols.includes(c)) setSelectedCols(s => [...s, c]); };
  const removeCol = (c) => setSelectedCols(s => s.filter(x => x !== c));
  const moveUp   = (i) => { if (i === 0) return; const a = [...selectedCols]; [a[i-1], a[i]] = [a[i], a[i-1]]; setSelectedCols(a); };
  const moveDown = (i) => { if (i === selectedCols.length - 1) return; const a = [...selectedCols]; [a[i], a[i+1]] = [a[i+1], a[i]]; setSelectedCols(a); };

  const previewStr = selectedCols.length > 0
    ? selectedCols.join(` ${separator ? `"${separator}"` : "||"} `)
    : null;

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <div style={{ background: C.tealTint, borderRadius: 10, padding: "10px 14px", marginBottom: 14, fontSize: 11, color: C.teal, lineHeight: 1.7 }}>
        Combine multiple string columns into one column using a separator character.
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 10, marginBottom: 16, overflowY: "auto", flex: 1, paddingRight: 4 }}>
        {/* Step 1 */}
        <StepHeader n={1} label="Name the new combined column" color={C.teal} />
        <div>
          <Input value={newColName} onChange={setNewColName} placeholder="e.g. full_name, full_address" />
          {isReplace && <div style={{ marginTop: 6, fontSize: 11, color: C.orange, background: C.orangeTint, borderRadius: 6, padding: "5px 10px" }}>⚠ Column "<strong>{newColName}</strong>" exists — values will be replaced.</div>}
          {newColName && !isReplace && <div style={{ marginTop: 6, fontSize: 11, color: C.teal, background: C.tealTint, borderRadius: 6, padding: "5px 10px" }}>✓ New column "<strong>{newColName}</strong>" will be added.</div>}
        </div>

        <Divider />

        {/* Step 2 */}
        <StepHeader n={2} label="Separator character" color={C.teal} />
        <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
          {SEPARATOR_PRESETS.map(s => (
            <button key={s.value} onClick={() => { setSeparatorPreset(s.value); setUseCustom(false); }}
              style={{
                padding: "6px 12px", borderRadius: 20, cursor: "pointer",
                border: `2px solid ${!useCustom && separatorPreset === s.value ? C.teal : C.g200}`,
                background: !useCustom && separatorPreset === s.value ? C.tealTint2 : C.white,
                color: !useCustom && separatorPreset === s.value ? C.teal : C.g500,
                fontSize: 11, fontWeight: 700,
              }}>
              {s.label}
              <span style={{ fontFamily: "monospace", fontSize: 10, marginLeft: 5, opacity: 0.7 }}>{s.display}</span>
            </button>
          ))}
          <button onClick={() => setUseCustom(true)}
            style={{ padding: "6px 12px", borderRadius: 20, cursor: "pointer", border: `2px solid ${useCustom ? C.teal : C.g200}`, background: useCustom ? C.tealTint2 : C.white, color: useCustom ? C.teal : C.g500, fontSize: 11, fontWeight: 700 }}>
            Custom
          </button>
        </div>
        {useCustom && (
          <div>
            <Input value={customSep} onChange={setCustomSep} placeholder='e.g. " / " or " and "' />
          </div>
        )}

        <Divider />

        {/* Step 3 */}
        <StepHeader n={3} label="Remove original columns?" color={C.teal} />
        <div style={{ display: "flex", gap: 8 }}>
          <button onClick={() => setRemoveOriginal(false)}
            style={{ flex: 1, padding: "8px 0", borderRadius: 8, cursor: "pointer", border: `2px solid ${!removeOriginal ? C.teal : C.g200}`, background: !removeOriginal ? C.tealTint : C.white, color: !removeOriginal ? C.teal : C.g500, fontSize: 12, fontWeight: 700 }}>
            Keep originals
          </button>
          <button onClick={() => setRemoveOriginal(true)}
            style={{ flex: 1, padding: "8px 0", borderRadius: 8, cursor: "pointer", border: `2px solid ${removeOriginal ? C.red : C.g200}`, background: removeOriginal ? C.redTint : C.white, color: removeOriginal ? C.red : C.g500, fontSize: 12, fontWeight: 700 }}>
            Remove originals
          </button>
        </div>

        <Divider />

        {/* Step 4 */}
        <StepHeader n={4} label="Select columns (order matters)" color={C.teal} />

        {selectedCols.length > 0 && (
          <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
            {selectedCols.map((c, i) => (
              <div key={c} style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <span style={{ width: 20, height: 20, borderRadius: "50%", background: C.teal, color: C.white, fontSize: 10, fontWeight: 800, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>{i + 1}</span>
                <div style={{ flex: 1, padding: "6px 10px", borderRadius: 7, background: C.tealTint, border: `1px solid ${C.tealTint2}`, fontSize: 12, color: C.teal, fontWeight: 600, fontFamily: "monospace", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{c}</div>
                {i > 0 && (
                  <button onClick={() => moveUp(i)} style={{ width: 24, height: 24, borderRadius: 5, border: `1px solid ${C.g200}`, background: C.white, cursor: "pointer", color: C.g500, fontSize: 12, display: "flex", alignItems: "center", justifyContent: "center" }}>↑</button>
                )}
                {i < selectedCols.length - 1 && (
                  <button onClick={() => moveDown(i)} style={{ width: 24, height: 24, borderRadius: 5, border: `1px solid ${C.g200}`, background: C.white, cursor: "pointer", color: C.g500, fontSize: 12, display: "flex", alignItems: "center", justifyContent: "center" }}>↓</button>
                )}
                <button onClick={() => removeCol(c)} style={{ width: 24, height: 24, borderRadius: 5, background: C.redTint, border: "none", color: C.red, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14 }}>×</button>
              </div>
            ))}
          </div>
        )}

        <ColumnPicker columns={columns} selected={selectedCols} onAdd={addCol} onRemove={removeCol} />
      </div>

      {isValid && (
        <div style={{ background: "#1e293b", borderRadius: 8, padding: "10px 14px", fontFamily: "monospace", fontSize: 11, color: "#5eead4", lineHeight: 1.8, marginBottom: 16 }}>
          <div style={{ color: C.g400, fontSize: 10, marginBottom: 4 }}>SQL Preview</div>
          {`"${newColName}" = `}
          {selectedCols.map((c, i) => (
            <span key={c}>
              {`COALESCE("${c}"::TEXT, '')`}
              {i < selectedCols.length - 1 && <span style={{ color: "#fbbf24" }}>{separator ? ` || '${separator}' || ` : " || "}</span>}
            </span>
          ))}
          {removeOriginal && <div style={{ color: "#f87171", marginTop: 4, fontSize: 10 }}>— removes: {selectedCols.join(", ")}</div>}
        </div>
      )}

      <SaveBar
        onSave={() => onSave({
          newColName: newColName.trim(), separator, separatorPreset, customSep, useCustom,
          removeOriginal, selectedCols,
        })}
        onClose={onClose}
        disabled={!isValid}
        color={C.teal}
      />
    </div>
  );
}

// ── TITLE & WIDTH MAP ─────────────────────────────────────────────────────────
const CONFIG_TITLE = {
  add_const:       "Add Constant",
  select_col:      "Select Column",
  rename_col:      "Rename Columns",
  drop_col:        "Drop Columns",
  order_table:     "Order Table",
  change_type:     "Change Column Data Type",
  set_val:         "Set Column Value",
  val_mapper:      "Value Mapper",
  fill_null:       "Fill NULL",
  filter_rows:     "Filter Rows",
  group_agg:       "Group By & Aggregate",
  join_data:       "Join Data",
  pyspark:         "PySpark Node",
  calc:            "Calculator Node",
  adv_calculator:  "Advance Calculator Node",
  combine_cols:    "Combine Columns Node",
};

const CONFIG_WIDTH = {
  val_mapper: 460, group_agg: 460, join_data: 460, pyspark: 520,
  filter_rows: 440, rename_col: 420, change_type: 420,
  calc:           420,
  adv_calculator: 480,
  combine_cols:   440,
};

const CONFIG_ACCENT = {
  calc:           C.violet,
  adv_calculator: C.indigo,
  combine_cols:   C.teal,
};

// ── MAIN EXPORT (Rendered as Right Sidebar Drawer) ────────────────────────────
export function UtilityConfigModal({ node, columns, onSave, onClose, allNodes }) {
  if (!node) return null;
  const type    = node.data?.type;
  const config  = node.data?.config || {};
  const width   = CONFIG_WIDTH[type]  || 400;
  const title   = CONFIG_TITLE[type]  || "Configure Node";
  const accent  = CONFIG_ACCENT[type] || C.blue;

  const handleSave = (cfg) => { onSave(node.id, cfg); onClose(); };
  const props = { columns: columns || [], config, onSave: handleSave, onClose, allNodes };

  return (
    <div 
      style={{ 
        position: "fixed", 
        inset: 0, 
        background: "rgba(11, 30, 61, 0.15)", // Overlay latar belakang transparan/tipis
        display: "flex", 
        justifyContent: "flex-end", // Menyebelahkan ke kanan (Sidebar)
        zIndex: 2000 
      }} 
      onClick={onClose}
    >
      <div 
        style={{ 
          background: C.white, 
          borderRadius: "16px 0 0 16px", // Melengkung hanya di sudut kiri panel sidebar
          width, 
          maxWidth: "100vw", 
          height: "100vh", // Mengisi tinggi layar penuh
          boxShadow: "-8px 0 32px rgba(0,0,0,0.15)", 
          display: "flex", 
          flexDirection: "column", 
          overflow: "hidden" 
        }} 
        onClick={e => e.stopPropagation()}
      >
        {/* Sidebar Header */}
        <div style={{ background: `linear-gradient(90deg,${C.navy},${accent}cc)`, padding: "15px 20px", display: "flex", justifyContent: "space-between", alignItems: "center", flexShrink: 0 }}>
          <div>
            <div style={{ color: C.white, fontWeight: 800, fontSize: 14 }}>{title}</div>
            <div style={{ color: "rgba(255,255,255,0.6)", fontSize: 10, marginTop: 1 }}>Configure node transformation</div>
          </div>
          <button onClick={onClose} style={{ background: "rgba(255,255,255,.1)", border: "none", borderRadius: 6, padding: "4px 10px", cursor: "pointer", color: C.white, fontSize: 18, lineHeight: 1 }}>×</button>
        </div>

        {/* Scrollable Sidebar Body */}
        <div style={{ padding: "20px 22px", overflowY: "auto", flex: 1, display: "flex", flexDirection: "column" }}>
          {type === "add_const"      && <AddConstantConfig    {...props} />}
          {type === "select_col"     && <SelectColumnConfig   {...props} />}
          {type === "rename_col"     && <RenameColumnsConfig  {...props} />}
          {type === "drop_col"       && <DropColumnsConfig    {...props} />}
          {type === "order_table"    && <OrderTableConfig     {...props} />}
          {type === "change_type"    && <ChangeDataTypeConfig {...props} />}
          {type === "set_val"        && <SetColumnValueConfig {...props} />}
          {type === "val_mapper"     && <ValueMapperConfig    {...props} />}
          {type === "fill_null"      && <FillNullConfig       {...props} />}
          {type === "filter_rows"    && <FilterRowsConfig     {...props} />}
          {type === "group_agg"      && <GroupByAggConfig     {...props} />}
          {type === "join_data"      && <JoinDataConfig       {...props} allNodes={allNodes} />}
          {type === "pyspark"        && <PySparkConfig        {...props} />}
          {/* New nodes */}
          {type === "calc"           && <CalculatorNodeConfig  {...props} />}
          {type === "adv_calculator" && <AdvanceCalculatorConfig {...props} />}
          {type === "combine_cols"   && <CombineColumnsConfig  {...props} />}
          {!CONFIG_TITLE[type]       && <div style={{ color: C.g400, padding: 20, textAlign: "center" }}>No configuration available for this node type.</div>}
        </div>
      </div>
    </div>
  );
}