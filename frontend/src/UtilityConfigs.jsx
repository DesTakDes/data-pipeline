// UtilityConfigs.jsx
// Drop-in replacement for utility node configuration sidebars
// Import and use: <UtilityConfigSidebar node={node} columns={columns} onSave={fn} onClose={fn} />

import { useState, useEffect } from "react";

const C = {
  navy:"#0B1E3D", blue:"#1D6FEB", blueMid:"#3B82F6",
  blueTint:"#EFF6FF", blueTint2:"#DBEAFE",
  gold:"#F59E0B", goldTint:"#FFFBEB",
  white:"#FFFFFF", g50:"#F8FAFC", g100:"#F1F5F9", g200:"#E2E8F0",
  g300:"#CBD5E1", g400:"#94A3B8", g500:"#64748B", g600:"#475569", g700:"#334155",
  green:"#16A34A", greenTint:"#DCFCE7",
  red:"#DC2626", redTint:"#FEE2E2",
  orange:"#EA580C", orangeTint:"#FFEDD5",
};

const DATA_TYPES = ["TEXT","INTEGER","BIGINT","NUMERIC","BOOLEAN","DATE","TIMESTAMP","VARCHAR(255)"];
const JOIN_TYPES = ["INNER JOIN","LEFT JOIN","RIGHT JOIN","FULL OUTER JOIN","CROSS JOIN"];
const AGG_FUNCS  = ["COUNT","SUM","AVG","MIN","MAX","COUNT DISTINCT"];
const CONDITIONS = ["=","!=",">",">=","<","<=","LIKE","IS NULL","IS NOT NULL","IN","NOT IN"];
const LOGIC_OPS  = ["AND","OR"];
const ORDER_DIRS = ["ASC","DESC"];

// ── Shared sub-components ─────────────────────────────────────────────────────

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
          <button onClick={() => { available.forEach(c => onAdd(c)); }} style={{ padding: "5px 10px", borderRadius: 6, border: `1px solid ${C.g300}`, background: C.g50, color: C.g600, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>
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

function SaveBar({ onSave, onClose, disabled = false }) {
  return (
    <div style={{ display: "flex", gap: 8, marginTop: "auto", paddingTop: 16, borderTop: `1px solid ${C.g200}` }}>
      <button onClick={onClose} style={{ flex: 1, padding: "9px 0", borderRadius: 8, border: `1px solid ${C.g200}`, background: C.white, color: C.g600, fontSize: 13, fontWeight: 600, cursor: "pointer" }}>Cancel</button>
      <button onClick={onSave} disabled={disabled} style={{ flex: 1, padding: "9px 0", borderRadius: 8, background: disabled ? C.g300 : `linear-gradient(90deg,${C.blue},${C.blueMid})`, border: "none", color: C.white, fontSize: 13, fontWeight: 700, cursor: disabled ? "not-allowed" : "pointer" }}>
        Save Configuration
      </button>
    </div>
  );
}

// ── Configuration Components ──────────────────────────────────────────────────
// (Kode komponen configurasi di bawah ini sama seperti sebelumnya)
function AddConstantConfig({ config, onSave, onClose }) {
  const [name,  setName]  = useState(config?.name  || "");
  const [value, setValue] = useState(config?.value || "");
  const [dtype, setDtype] = useState(config?.dtype || "TEXT");

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16, lineHeight: 1.6 }}>Add a new column with a constant value to your dataset.</p>
      <div style={{ display: "flex", flexDirection: "column", gap: 12, flex: 1 }}>
        <div><Label required>Column Name</Label><Input value={name} onChange={setName} placeholder="e.g. country" /></div>
        <div><Label required>Constant Value</Label><Input value={value} onChange={setValue} placeholder='e.g. "Indonesia" or 1' /></div>
        <div><Label required>Data Type</Label><Select value={dtype} onChange={setDtype} options={DATA_TYPES} /></div>
        {name && value && (
          <div style={{ marginTop: 12, background: C.blueTint, borderRadius: 7, padding: "8px 12px", fontSize: 11, color: C.blue }}>
            Preview: <code style={{ fontFamily: "monospace" }}>{name} = {value} ({dtype})</code>
          </div>
        )}
      </div>
      <SaveBar onSave={() => onSave({ name, value, dtype })} onClose={onClose} disabled={!name || !value} />
    </div>
  );
}

function SelectColumnConfig({ columns, config, onSave, onClose }) {
  const [selected, setSelected] = useState(config?.columns || []);
  const add = (c) => !selected.includes(c) && setSelected(s => [...s, c]);
  const remove = (c) => setSelected(s => s.filter(x => x !== c));

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Select columns to keep in the output.</p>
      <div style={{ flex: 1 }}>
        <Label required>Columns to Select</Label>
        <ColumnPicker columns={columns} selected={selected} onAdd={add} onRemove={remove} onAddAll={() => columns.forEach(add)} onRemoveAll={() => setSelected([])} />
        {selected.length > 0 && (
          <div style={{ marginTop: 10, background: C.greenTint, borderRadius: 7, padding: "8px 12px", fontSize: 11, color: C.green }}>
            ✓ {selected.length} column{selected.length > 1 ? "s" : ""} selected: {selected.join(", ")}
          </div>
        )}
      </div>
      <SaveBar onSave={() => onSave({ columns: selected })} onClose={onClose} disabled={selected.length === 0} />
    </div>
  );
}

function RenameColumnsConfig({ columns, config, onSave, onClose }) {
  const [selected, setSelected] = useState(Object.keys(config?.renames || {}));
  const [renames,  setRenames]  = useState(config?.renames || {});

  const add = (c) => { if (!selected.includes(c)) { setSelected(s => [...s, c]); setRenames(r => ({ ...r, [c]: r[c] || "" })); }};
  const remove = (c) => { setSelected(s => s.filter(x => x !== c)); setRenames(r => { const n = { ...r }; delete n[c]; return n; }); };

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Select columns and provide new names.</p>
      <div style={{ flex: 1 }}>
        <Label required>Select Columns</Label>
        <ColumnPicker columns={columns} selected={selected} onAdd={add} onRemove={remove} onAddAll={() => columns.forEach(add)} onRemoveAll={() => { setSelected([]); setRenames({}); }} />
        {selected.length > 0 && (
          <div style={{ marginTop: 14 }}>
            <Label>New Column Names</Label>
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {selected.map(c => (
                <div key={c} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <div style={{ flex: 1, padding: "7px 10px", borderRadius: 7, background: C.g100, fontSize: 12, color: C.g600, fontFamily: "monospace" }}>{c}</div>
                  <span style={{ color: C.g400, fontSize: 12 }}>→</span>
                  <div style={{ flex: 1 }}>
                    <input value={renames[c] || ""} onChange={e => setRenames(r => ({ ...r, [c]: e.target.value }))} placeholder={`new name`}
                      style={{ width: "100%", padding: "7px 10px", borderRadius: 7, border: `1px solid ${C.g200}`, fontSize: 12, outline: "none", color: C.g700, boxSizing: "border-box", fontFamily: "monospace" }} />
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
      <SaveBar onSave={() => onSave({ renames })} onClose={onClose} disabled={selected.length === 0 || selected.some(c => !renames[c])} />
    </div>
  );
}

function DropColumnsConfig({ columns, config, onSave, onClose }) {
  const [selected, setSelected] = useState(config?.columns || []);
  const add = (c) => !selected.includes(c) && setSelected(s => [...s, c]);
  const remove = (c) => setSelected(s => s.filter(x => x !== c));

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Select columns to <strong>remove</strong> from the dataset.</p>
      <div style={{ flex: 1 }}>
        <Label required>Columns to Drop</Label>
        <ColumnPicker columns={columns} selected={selected} onAdd={add} onRemove={remove} onAddAll={() => columns.forEach(add)} onRemoveAll={() => setSelected([])} />
        {selected.length > 0 && (
          <div style={{ marginTop: 10, background: C.redTint, borderRadius: 7, padding: "8px 12px", fontSize: 11, color: C.red }}>
            ⚠ Will remove: {selected.join(", ")}
          </div>
        )}
      </div>
      <SaveBar onSave={() => onSave({ columns: selected })} onClose={onClose} disabled={selected.length === 0} />
    </div>
  );
}

function OrderTableConfig({ columns, config, onSave, onClose }) {
  const [orders, setOrders] = useState(config?.orders || []); // [{col, dir}]
  const [open,   setOpen]   = useState(false);
  const usedCols = orders.map(o => o.col);
  const available = columns.filter(c => !usedCols.includes(c));

  const addCol = (c) => setOrders(o => [...o, { col: c, dir: "ASC" }]);
  const removeCol = (c) => setOrders(o => o.filter(x => x.col !== c));
  const setDir = (c, dir) => setOrders(o => o.map(x => x.col === c ? { ...x, dir } : x));

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Choose columns to sort by and set the order direction.</p>
      <div style={{ flex: 1 }}>
        <Label required>Sort Columns</Label>
        {orders.length > 0 && (
          <div style={{ display: "flex", flexDirection: "column", gap: 7, marginBottom: 10 }}>
            {orders.map((o, i) => (
              <div key={o.col} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ width: 18, height: 18, borderRadius: "50%", background: C.blue, color: C.white, fontSize: 10, fontWeight: 700, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>{i+1}</span>
                <div style={{ flex: 1, padding: "6px 10px", borderRadius: 7, background: C.g100, fontSize: 12, color: C.g700, fontFamily: "monospace" }}>{o.col}</div>
                <select value={o.dir} onChange={e => setDir(o.col, e.target.value)}
                  style={{ padding: "6px 8px", borderRadius: 7, border: `1px solid ${C.g200}`, fontSize: 11, fontWeight: 700, color: o.dir === "ASC" ? C.green : C.orange, background: C.white, width: 76 }}>
                  {ORDER_DIRS.map(d => <option key={d}>{d}</option>)}
                </select>
                <button onClick={() => removeCol(o.col)} style={{ background: C.redTint, border: "none", borderRadius: 6, width: 24, height: 24, cursor: "pointer", color: C.red, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14 }}>×</button>
              </div>
            ))}
          </div>
        )}

        <div style={{ position: "relative", display: "inline-block" }}>
          <button onClick={() => setOpen(v => !v)} style={{ padding: "5px 10px", borderRadius: 6, border: `1px solid ${C.blue}`, background: C.blueTint, color: C.blue, fontSize: 11, fontWeight: 700, cursor: "pointer" }}>+ Add Column</button>
          {open && (
            <div style={{ position: "absolute", top: "100%", left: 0, zIndex: 100, background: C.white, border: `1px solid ${C.g200}`, borderRadius: 8, boxShadow: "0 4px 16px rgba(0,0,0,.12)", maxHeight: 160, overflowY: "auto", minWidth: 180, marginTop: 3 }}>
              {available.length === 0
                ? <div style={{ padding: "10px 14px", fontSize: 11, color: C.g400 }}>All columns added</div>
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
          <div style={{ marginTop: 12, background: C.blueTint, borderRadius: 7, padding: "8px 12px", fontSize: 11, color: C.blue }}>
            ORDER BY {orders.map(o => `${o.col} ${o.dir}`).join(", ")}
          </div>
        )}
      </div>
      <SaveBar onSave={() => onSave({ orders })} onClose={onClose} disabled={orders.length === 0} />
    </div>
  );
}

function ChangeDataTypeConfig({ columns, config, onSave, onClose }) {
  const [selected, setSelected] = useState(Object.keys(config?.types || {}));
  const [types,    setTypes]    = useState(config?.types || {});

  const add = (c) => { if (!selected.includes(c)) { setSelected(s => [...s, c]); setTypes(t => ({ ...t, [c]: t[c] || "TEXT" })); }};
  const remove = (c) => { setSelected(s => s.filter(x => x !== c)); setTypes(t => { const n = { ...t }; delete n[c]; return n; }); };

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Select columns and choose the new data type for each.</p>
      <div style={{ flex: 1 }}>
        <Label required>Select Columns</Label>
        <ColumnPicker columns={columns} selected={selected} onAdd={add} onRemove={remove} onAddAll={() => columns.forEach(add)} onRemoveAll={() => { setSelected([]); setTypes({}); }} />
        {selected.length > 0 && (
          <div style={{ marginTop: 14 }}>
            <Label>New Data Types</Label>
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {selected.map(c => (
                <div key={c} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <div style={{ flex: 1, padding: "7px 10px", borderRadius: 7, background: C.g100, fontSize: 12, color: C.g600, fontFamily: "monospace" }}>{c}</div>
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
      </div>
      <SaveBar onSave={() => onSave({ types })} onClose={onClose} disabled={selected.length === 0} />
    </div>
  );
}

function SetColumnValueConfig({ columns, config, onSave, onClose }) {
  const [targetCol, setTargetCol] = useState(config?.targetCol || "");
  const [sourceCol, setSourceCol] = useState(config?.sourceCol || "");
  const [useExpr,   setUseExpr]   = useState(config?.useExpr || false);
  const [expr,      setExpr]      = useState(config?.expr || "");

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Replace the value of a column with another column's value or an expression.</p>
      <div style={{ display: "flex", flexDirection: "column", gap: 12, flex: 1 }}>
        <div><Label required>Target Column (to replace)</Label><Select value={targetCol} onChange={setTargetCol} options={columns} placeholder="— Select column —" /></div>
        <div style={{ display: "flex", gap: 8, marginBottom: 4 }}>
          <button onClick={() => setUseExpr(false)} style={{ flex: 1, padding: "7px 0", borderRadius: 7, border: `1px solid ${!useExpr ? C.blue : C.g200}`, background: !useExpr ? C.blueTint : C.white, color: !useExpr ? C.blue : C.g500, fontSize: 12, fontWeight: 600, cursor: "pointer" }}>From Column</button>
          <button onClick={() => setUseExpr(true)}  style={{ flex: 1, padding: "7px 0", borderRadius: 7, border: `1px solid ${useExpr ? C.blue : C.g200}`, background: useExpr ? C.blueTint : C.white, color: useExpr ? C.blue : C.g500, fontSize: 12, fontWeight: 600, cursor: "pointer" }}>Expression</button>
        </div>
        {!useExpr
          ? <div><Label required>Source Column (value to use)</Label><Select value={sourceCol} onChange={setSourceCol} options={columns.filter(c => c !== targetCol)} placeholder="— Select column —" /></div>
          : <div><Label required>SQL Expression</Label><Input value={expr} onChange={setExpr} placeholder="e.g. UPPER(name) or price * 1.1" mono /></div>
        }
        {targetCol && (sourceCol || expr) && (
          <div style={{ marginTop: 12, background: C.blueTint, borderRadius: 7, padding: "8px 12px", fontSize: 11, color: C.blue, fontFamily: "monospace" }}>
            UPDATE {targetCol} = {useExpr ? expr : sourceCol}
          </div>
        )}
      </div>
      <SaveBar onSave={() => onSave({ targetCol, sourceCol, useExpr, expr })} onClose={onClose} disabled={!targetCol || (!sourceCol && !expr)} />
    </div>
  );
}

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
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16, lineHeight: 1.6 }}>Map values to a new column using conditional logic.</p>
      <div style={{ display: "flex", flexDirection: "column", gap: 12, flex: 1, overflowY: "auto", paddingRight: 4 }}>
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
                  {i > 0 && (
                    <select value={w.logic} onChange={e => updateWhen(i, "logic", e.target.value)}
                      style={{ width: 64, padding: "4px 6px", borderRadius: 6, border: `1px solid ${C.g200}`, fontSize: 11, fontWeight: 700, color: C.orange, background: C.white }}>
                      {LOGIC_OPS.map(l => <option key={l}>{l}</option>)}
                    </select>
                  )}
                  <span style={{ fontSize: 11, fontWeight: 700, color: C.blue }}>WHEN #{i + 1}</span>
                  {i > 0 && <button onClick={() => removeWhen(i)} style={{ marginLeft: "auto", background: C.redTint, border: "none", borderRadius: 5, padding: "2px 7px", cursor: "pointer", color: C.red, fontSize: 11 }}>Remove</button>}
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 6 }}>
                  <div>
                    <div style={{ fontSize: 10, color: C.g400, marginBottom: 3 }}>Column</div>
                    <div style={{ padding: "6px 8px", borderRadius: 6, background: C.blueTint, fontSize: 11, color: C.blue, fontWeight: 600 }}>{sourceCol || "—"}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: 10, color: C.g400, marginBottom: 3 }}>Condition</div>
                    <select value={w.condition} onChange={e => updateWhen(i, "condition", e.target.value)}
                      style={{ width: "100%", padding: "6px 6px", borderRadius: 6, border: `1px solid ${C.g200}`, fontSize: 11, background: C.white, color: C.g700 }}>
                      {CONDITIONS.map(c => <option key={c}>{c}</option>)}
                    </select>
                  </div>
                  <div>
                    <div style={{ fontSize: 10, color: C.g400, marginBottom: 3 }}>Compare Value</div>
                    <input value={w.value} onChange={e => updateWhen(i, "value", e.target.value)} placeholder="1000"
                      style={{ width: "100%", padding: "6px 7px", borderRadius: 6, border: `1px solid ${C.g200}`, fontSize: 11, boxSizing: "border-box", outline: "none", color: C.g700 }} />
                  </div>
                  <div>
                    <div style={{ fontSize: 10, color: C.g400, marginBottom: 3 }}>Then (Result)</div>
                    <input value={w.result} onChange={e => updateWhen(i, "result", e.target.value)} placeholder="silver"
                      style={{ width: "100%", padding: "6px 7px", borderRadius: 6, border: `1px solid ${C.g200}`, fontSize: 11, boxSizing: "border-box", outline: "none", color: C.g700 }} />
                  </div>
                </div>
              </div>
            ))}
          </div>
          <button onClick={addWhen} style={{ marginTop: 8, padding: "5px 12px", borderRadius: 6, border: `1px dashed ${C.blue}`, background: C.blueTint, color: C.blue, fontSize: 11, fontWeight: 700, cursor: "pointer" }}>
            + Add WHEN
          </button>
        </div>

        <div><Label required>ELSE Value</Label><Input value={elseValue} onChange={setElseValue} placeholder="e.g. bronze (default if no condition matches)" /></div>

        {preview && (
          <div style={{ marginTop: 12, background: C.g800 || "#1e293b", borderRadius: 8, padding: 12, fontFamily: "monospace", fontSize: 11, color: "#93c5fd", lineHeight: 1.7 }}>
            <div style={{ color: C.g400, fontSize: 10, marginBottom: 4 }}>SQL Preview</div>
            CASE<br/>
            {whens.map((w, i) => <span key={i}>&nbsp;&nbsp;{i > 0 ? w.logic : "WHEN"} {sourceCol} {w.condition} '{w.value}' THEN '{w.result}'<br/></span>)}
            &nbsp;&nbsp;ELSE '{elseValue}'<br/>
            END AS {newColName}
          </div>
        )}
      </div>
      <SaveBar onSave={() => onSave({ sourceCol, newColName, elseValue, whens })} onClose={onClose} disabled={!sourceCol || !newColName || !elseValue || whens.some(w => !w.value || !w.result)} />
    </div>
  );
}

function FillNullConfig({ columns, config, onSave, onClose }) {
  const [selected, setSelected] = useState(config?.columns || []);
  const [fillValue, setFillValue] = useState(config?.fillValue || "");
  const [fillType,  setFillType]  = useState(config?.fillType  || "value"); 

  const add = (c) => !selected.includes(c) && setSelected(s => [...s, c]);
  const remove = (c) => setSelected(s => s.filter(x => x !== c));

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Replace NULL values in selected columns.</p>
      <div style={{ display: "flex", flexDirection: "column", gap: 12, flex: 1 }}>
        <div>
          <Label required>Select Columns</Label>
          <ColumnPicker columns={columns} selected={selected} onAdd={add} onRemove={remove} onAddAll={() => columns.forEach(add)} onRemoveAll={() => setSelected([])} />
        </div>
        <div>
          <Label required>Fill Method</Label>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3,1fr)", gap: 6, marginBottom: 10 }}>
            {[["value","Custom Value"],["mean","Mean"],["median","Median"],["mode","Mode"],["forward","Forward Fill"],["backward","Backward Fill"]].map(([v,l]) => (
              <button key={v} onClick={() => setFillType(v)} style={{ padding: "6px 0", borderRadius: 7, border: `1px solid ${fillType===v?C.blue:C.g200}`, background: fillType===v?C.blueTint:C.white, color: fillType===v?C.blue:C.g500, fontSize: 11, fontWeight: 600, cursor: "pointer" }}>{l}</button>
            ))}
          </div>
        </div>
        {fillType === "value" && (
          <div><Label required>Fill Value</Label><Input value={fillValue} onChange={setFillValue} placeholder='e.g. 0 or "Unknown"' /></div>
        )}
        {selected.length > 0 && (
          <div style={{ marginTop: 12, background: C.blueTint, borderRadius: 7, padding: "8px 12px", fontSize: 11, color: C.blue }}>
            Fill NULL in [{selected.join(", ")}] with {fillType === "value" ? `"${fillValue}"` : fillType}
          </div>
        )}
      </div>
      <SaveBar onSave={() => onSave({ columns: selected, fillValue, fillType })} onClose={onClose} disabled={selected.length === 0 || (fillType === "value" && !fillValue)} />
    </div>
  );
}

function FilterRowsConfig({ columns, config, onSave, onClose }) {
  const [formula,   setFormula]   = useState(config?.formula || "");
  const [error,     setError]     = useState("");
  const [validated, setValidated] = useState(false);

  const validate = () => {
    if (!formula.trim()) { setError("Formula cannot be empty"); setValidated(false); return; }
    const dangerous = ["DROP","DELETE","INSERT","UPDATE","TRUNCATE","ALTER","CREATE","EXEC","EXECUTE"];
    const upper = formula.toUpperCase();
    if (dangerous.some(d => upper.includes(d))) { setError("Dangerous SQL keywords detected"); setValidated(false); return; }
    setError("");
    setValidated(true);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 12, lineHeight: 1.6 }}>Filter rows using a SQL WHERE clause condition.</p>
      <div style={{ flex: 1 }}>
        <div style={{ marginBottom: 10 }}>
          <Label required>Filter Formula (SQL WHERE clause)</Label>
          <textarea value={formula} onChange={e => { setFormula(e.target.value); setValidated(false); setError(""); }} placeholder={"e.g. age > 18\nor: city = 'Jakarta' AND salary >= 5000000\nor: child_no < 2 OR child_no IS NULL"}
            rows={4} style={{ width: "100%", padding: "10px 12px", borderRadius: 8, border: `1.5px solid ${error ? C.red : validated ? C.green : C.g200}`, fontSize: 12, boxSizing: "border-box", outline: "none", color: C.g700, resize: "vertical", fontFamily: "monospace", lineHeight: 1.7 }} />
          {error    && <div style={{ marginTop: 5, fontSize: 11, color: C.red   }}>❌ {error}</div>}
          {validated && <div style={{ marginTop: 5, fontSize: 11, color: C.green }}>✓ Formula looks valid</div>}
        </div>

        <div style={{ marginBottom: 12 }}>
          <div style={{ fontSize: 11, color: C.g400, marginBottom: 6 }}>Available columns (click to insert):</div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 5 }}>
            {columns.map(c => (
              <button key={c} onClick={() => setFormula(f => f + (f ? " AND " : "") + c)} style={{ padding: "3px 9px", borderRadius: 5, border: `1px solid ${C.g200}`, background: C.g50, color: C.g600, fontSize: 11, cursor: "pointer", fontFamily: "monospace" }}>{c}</button>
            ))}
          </div>
        </div>

        <button onClick={validate} style={{ padding: "6px 14px", borderRadius: 7, border: `1px solid ${C.blue}`, background: C.blueTint, color: C.blue, fontSize: 12, fontWeight: 700, cursor: "pointer", marginBottom: 4 }}>
          Validate Formula
        </button>
      </div>

      <SaveBar onSave={() => onSave({ formula })} onClose={onClose} disabled={!formula || !!error} />
    </div>
  );
}

function GroupByAggConfig({ columns, config, onSave, onClose }) {
  const [groupCols, setGroupCols] = useState(config?.groupCols || []);
  const [aggCols,   setAggCols]   = useState(config?.aggCols   || []); 
  const [openGroup, setOpenGroup] = useState(false);
  const [openAgg,   setOpenAgg]   = useState(false);

  const addGroup  = (c) => !groupCols.includes(c) && setGroupCols(g => [...g, c]);
  const removeGroup = (c) => setGroupCols(g => g.filter(x => x !== c));
  const addAgg = (c) => setAggCols(a => [...a, { col: c, func: "COUNT", alias: `${c.toLowerCase()}_count` }]);
  const removeAgg = (i) => setAggCols(a => a.filter((_, idx) => idx !== i));
  const updateAgg = (i, field, val) => setAggCols(a => a.map((x, idx) => idx === i ? { ...x, [field]: val } : x));

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16 }}>Group data by columns and compute aggregations.</p>
      <div style={{ display: "flex", flexDirection: "column", gap: 14, flex: 1, overflowY: "auto", paddingRight: 4 }}>
        <div>
          <Label required>Group By Columns</Label>
          <ColumnPicker columns={columns} selected={groupCols} onAdd={addGroup} onRemove={removeGroup} onAddAll={() => columns.forEach(addGroup)} onRemoveAll={() => setGroupCols([])} />
        </div>

        <div>
          <Label required>Aggregate Columns</Label>
          {aggCols.length > 0 && (
            <div style={{ display: "flex", flexDirection: "column", gap: 7, marginBottom: 8 }}>
              {aggCols.map((a, i) => (
                <div key={i} style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr auto", gap: 6, alignItems: "center", background: C.g50, borderRadius: 7, padding: 8, border: `1px solid ${C.g200}` }}>
                  <div style={{ fontSize: 11, fontFamily: "monospace", color: C.g600, padding: "4px 6px", background: C.g100, borderRadius: 5 }}>{a.col}</div>
                  <select value={a.func} onChange={e => updateAgg(i, "func", e.target.value)}
                    style={{ padding: "5px 7px", borderRadius: 6, border: `1px solid ${C.g200}`, fontSize: 11, fontWeight: 700, color: C.blue, background: C.white }}>
                    {AGG_FUNCS.map(f => <option key={f}>{f}</option>)}
                  </select>
                  <input value={a.alias} onChange={e => updateAgg(i, "alias", e.target.value)} placeholder="alias"
                    style={{ padding: "5px 7px", borderRadius: 6, border: `1px solid ${C.g200}`, fontSize: 11, outline: "none", color: C.g700, fontFamily: "monospace" }} />
                  <button onClick={() => removeAgg(i)} style={{ background: C.redTint, border: "none", borderRadius: 5, width: 24, height: 24, cursor: "pointer", color: C.red, fontSize: 14, display: "flex", alignItems: "center", justifyContent: "center" }}>×</button>
                </div>
              ))}
            </div>
          )}
          <div style={{ position: "relative", display: "inline-block" }}>
            <button onClick={() => setOpenAgg(v => !v)} style={{ padding: "5px 10px", borderRadius: 6, border: `1px solid ${C.gold}`, background: C.goldTint, color: C.gold, fontSize: 11, fontWeight: 700, cursor: "pointer" }}>+ Add Aggregate Column</button>
            {openAgg && (
              <div style={{ position: "absolute", top: "100%", left: 0, zIndex: 100, background: C.white, border: `1px solid ${C.g200}`, borderRadius: 8, boxShadow: "0 4px 16px rgba(0,0,0,.12)", maxHeight: 160, overflowY: "auto", minWidth: 180, marginTop: 3 }}>
                {columns.map(c => (
                  <button key={c} onClick={() => { addAgg(c); setOpenAgg(false); }}
                    style={{ width: "100%", padding: "7px 14px", background: "none", border: "none", textAlign: "left", fontSize: 12, cursor: "pointer", color: C.g700 }}
                    onMouseEnter={e => e.currentTarget.style.background = C.g50}
                    onMouseLeave={e => e.currentTarget.style.background = "none"}>{c}</button>
                ))}
              </div>
            )}
          </div>
        </div>

        {groupCols.length > 0 && aggCols.length > 0 && (
          <div style={{ marginTop: 12, background: "#1e293b", borderRadius: 8, padding: 12, fontFamily: "monospace", fontSize: 11, color: "#93c5fd", lineHeight: 1.8 }}>
            <div style={{ color: C.g400, fontSize: 10, marginBottom: 4 }}>SQL Preview</div>
            SELECT {groupCols.join(", ")}, {aggCols.map(a => `${a.func}(${a.col}) AS ${a.alias}`).join(", ")}<br/>
            FROM table<br/>
            GROUP BY {groupCols.join(", ")}
          </div>
        )}
      </div>
      <SaveBar onSave={() => onSave({ groupCols, aggCols })} onClose={onClose} disabled={groupCols.length === 0 || aggCols.length === 0} />
    </div>
  );
}

function JoinDataConfig({ columns, config, onSave, onClose, allNodes }) {
  const [joinType,   setJoinType]   = useState(config?.joinType   || "INNER JOIN");
  const [leftCol,    setLeftCol]    = useState(config?.leftCol    || "");
  const [rightCol,   setRightCol]   = useState(config?.rightCol   || "");
  const [rightNodeId,setRightNodeId]= useState(config?.rightNodeId|| "");

  const inputNodes = (allNodes || []).filter(n => n.data?.type === "input_dataset" && n.data?.config?.dataset);
  const rightDataset = inputNodes.find(n => n.id === rightNodeId)?.data?.config?.dataset;
  const rightColumns = rightDataset?.columns || [];

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 16, lineHeight: 1.6 }}>
        Join two datasets. Connect both Input Dataset nodes to this Join node. The first connected node is the <strong>left table</strong>.
      </p>
      <div style={{ display: "flex", flexDirection: "column", gap: 12, flex: 1 }}>
        <div>
          <Label required>Join Type</Label>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3,1fr)", gap: 6 }}>
            {JOIN_TYPES.map(jt => (
              <button key={jt} onClick={() => setJoinType(jt)} style={{ padding: "7px 4px", borderRadius: 7, border: `1px solid ${joinType===jt?C.blue:C.g200}`, background: joinType===jt?C.blueTint:C.white, color: joinType===jt?C.blue:C.g500, fontSize: 10, fontWeight: 700, cursor: "pointer" }}>{jt.replace(" JOIN","")}</button>
            ))}
          </div>
        </div>

        {inputNodes.length > 0 && (
          <div>
            <Label required>Right Table (second dataset)</Label>
            <Select value={rightNodeId} onChange={setRightNodeId} options={inputNodes.map(n => ({ value: n.id, label: n.data.config.dataset.name }))} placeholder="— Select second dataset —" />
          </div>
        )}

        <div style={{ display: "grid", gridTemplateColumns: "1fr auto 1fr", gap: 10, alignItems: "end" }}>
          <div><Label required>Left Table Column</Label><Select value={leftCol} onChange={setLeftCol} options={columns} placeholder="— Select —" /></div>
          <div style={{ padding: "8px 0", color: C.g400, fontSize: 12, textAlign: "center" }}>=</div>
          <div><Label required>Right Table Column</Label><Select value={rightCol} onChange={setRightCol} options={rightColumns} placeholder="— Select —" /></div>
        </div>

        {leftCol && rightCol && (
          <div style={{ marginTop: 12, background: "#1e293b", borderRadius: 8, padding: 12, fontFamily: "monospace", fontSize: 11, color: "#93c5fd", lineHeight: 1.8 }}>
            <div style={{ color: C.g400, fontSize: 10, marginBottom: 4 }}>SQL Preview</div>
            FROM left_table<br/>
            {joinType} right_table<br/>
            ON left_table.{leftCol} = right_table.{rightCol}
          </div>
        )}
      </div>
      <SaveBar onSave={() => onSave({ joinType, leftCol, rightCol, rightNodeId })} onClose={onClose} disabled={!leftCol || !rightCol} />
    </div>
  );
}

function PySparkConfig({ config, onSave, onClose }) {
  const [code,     setCode]     = useState(config?.code     || `# Input dataframe is available as 'df'
# Perform your transformations below
# Example:
df = df.filter(df['age'] > 18)
df = df.withColumn('full_name', concat(col('first_name'), lit(' '), col('last_name')))

# The result 'df' will be passed to the next node`);
  const [nodeName, setNodeName] = useState(config?.nodeName || "PySpark Node");

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <p style={{ fontSize: 12, color: C.g500, marginBottom: 14, lineHeight: 1.6 }}>
        Write custom PySpark code to transform your data. The input dataframe is available as <code style={{ background: C.g100, padding: "1px 4px", borderRadius: 3, fontSize: 11 }}>df</code>.
      </p>
      <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 12 }}>
        <div>
          <Label>Node Label</Label>
          <Input value={nodeName} onChange={setNodeName} placeholder="e.g. Clean Names" />
        </div>
        <div style={{ flex: 1, display: "flex", flexDirection: "column" }}>
          <Label required>Python / PySpark Code</Label>
          <div style={{ background: "#0f172a", borderRadius: 8, padding: 4, border: `1px solid ${C.g700}`, flex: 1, display: "flex", flexDirection: "column" }}>
            <div style={{ padding: "4px 10px", background: "#1e293b", borderRadius: "6px 6px 0 0", display: "flex", gap: 5 }}>
              {["#f87171","#fbbf24","#34d399"].map((c, i) => <span key={i} style={{ width: 10, height: 10, borderRadius: "50%", background: c, display: "inline-block" }} />)}
              <span style={{ marginLeft: 8, fontSize: 10, color: C.g400 }}>python</span>
            </div>
            <textarea value={code} onChange={e => setCode(e.target.value)} spellCheck={false}
              style={{ flex: 1, minHeight: 200, width: "100%", padding: "12px 14px", background: "transparent", border: "none", outline: "none", color: "#93c5fd", fontSize: 12, fontFamily: "monospace", resize: "none", lineHeight: 1.7, boxSizing: "border-box" }} />
          </div>
        </div>
      </div>
      <SaveBar onSave={() => onSave({ code, nodeName })} onClose={onClose} disabled={!code.trim()} />
    </div>
  );
}

// ── TITLE MAP ─────────────────────────────────────────────────────────────────
const CONFIG_TITLE = {
  add_const:    "Add Constant",
  select_col:   "Select Column",
  rename_col:   "Rename Columns",
  drop_col:     "Drop Columns",
  order_table:  "Order Table",
  change_type:  "Change Column Data Type",
  set_val:      "Set Column Value",
  val_mapper:   "Value Mapper",
  fill_null:    "Fill NULL",
  filter_rows:  "Filter Rows",
  group_agg:    "Group By & Aggregate",
  join_data:    "Join Data",
  pyspark:      "PySpark Node",
};

const CONFIG_WIDTH = {
  val_mapper: 580, group_agg: 540, join_data: 500, pyspark: 580,
  filter_rows: 500, rename_col: 480, change_type: 480,
};

// ── MAIN EXPORT ───────────────────────────────────────────────────────────────
export function UtilityConfigModal({ node, columns, onSave, onClose, allNodes }) {
  if (!node) return null;
  const type    = node.data?.type;
  const config  = node.data?.config || {};
  const width   = CONFIG_WIDTH[type] || 440; // Disesuaikan sedikit untuk sidebar
  const title   = CONFIG_TITLE[type] || "Configure Node";

  const handleSave = (cfg) => {
    onSave(node.id, cfg);
    onClose();
  };

  const props = { columns: columns || [], config, onSave: handleSave, onClose, allNodes };

  return (
    <>
      {/* Animasi Sidebar ditambahkan via inline style */}
      <style>{`
        @keyframes fadeInOverlay { from { opacity: 0; } to { opacity: 1; } }
        @keyframes slideInRight { from { transform: translateX(100%); } to { transform: translateX(0); } }
      `}</style>
      
      {/* Overlay Backdrop */}
      <div 
        style={{ position: "fixed", inset: 0, background: "rgba(11,30,61,.4)", display: "flex", justifyContent: "flex-end", zIndex: 2000, animation: "fadeInOverlay 0.2s ease-out" }} 
        onClick={onClose}
      >
        {/* Panel Sidebar */}
        <div 
          style={{ background: C.white, width, maxWidth: "90vw", height: "100vh", boxShadow: "-8px 0 32px rgba(0,0,0,.15)", display: "flex", flexDirection: "column", overflow: "hidden", animation: "slideInRight 0.3s cubic-bezier(0.16, 1, 0.3, 1)" }} 
          onClick={e => e.stopPropagation()}
        >
          {/* Header Sidebar */}
          <div style={{ background: `linear-gradient(90deg,${C.navy},#15325c)`, padding: "20px 24px", display: "flex", justifyContent: "space-between", alignItems: "center", flexShrink: 0 }}>
            <div>
              <div style={{ color: C.white, fontWeight: 800, fontSize: 16 }}>{title}</div>
              <div style={{ color: "#93c5fd", fontSize: 11, marginTop: 2 }}>Configure node transformation</div>
            </div>
            <button onClick={onClose} style={{ background: "rgba(255,255,255,.1)", border: "none", borderRadius: 6, width: 28, height: 28, display: "flex", alignItems: "center", justifyContent: "center", cursor: "pointer", color: C.white, fontSize: 18, transition: "background 0.2s" }} onMouseEnter={e => e.target.style.background = "rgba(255,255,255,.2)"} onMouseLeave={e => e.target.style.background = "rgba(255,255,255,.1)"}>×</button>
          </div>
          
          {/* Scrollable body Sidebar */}
          <div style={{ padding: "24px 24px 20px", overflowY: "auto", flex: 1, display: "flex", flexDirection: "column" }}>
            {type === "add_const"   && <AddConstantConfig    {...props} />}
            {type === "select_col"  && <SelectColumnConfig   {...props} />}
            {type === "rename_col"  && <RenameColumnsConfig  {...props} />}
            {type === "drop_col"    && <DropColumnsConfig    {...props} />}
            {type === "order_table" && <OrderTableConfig     {...props} />}
            {type === "change_type" && <ChangeDataTypeConfig {...props} />}
            {type === "set_val"     && <SetColumnValueConfig {...props} />}
            {type === "val_mapper"  && <ValueMapperConfig    {...props} />}
            {type === "fill_null"   && <FillNullConfig       {...props} />}
            {type === "filter_rows" && <FilterRowsConfig     {...props} />}
            {type === "group_agg"   && <GroupByAggConfig     {...props} />}
            {type === "join_data"   && <JoinDataConfig       {...props} allNodes={allNodes} />}
            {type === "pyspark"     && <PySparkConfig        {...props} />}
            {!CONFIG_TITLE[type]    && <div style={{ color: C.g400, padding: 20, textAlign: "center" }}>No configuration available for this node type.</div>}
          </div>
        </div>
      </div>
    </>
  );
}