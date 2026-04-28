// NodePreview.jsx
// Preview panel for node data — shows sample rows and allows download

import { useState, useEffect } from "react";
import axios from "axios";

const api = axios.create({ baseURL: "/api" });

const C = {
  navy:"#0B1E3D", blue:"#1D6FEB", blueMid:"#3B82F6",
  blueTint:"#EFF6FF", blueTint2:"#DBEAFE",
  gold:"#F59E0B", goldTint:"#FFFBEB",
  white:"#FFFFFF", g50:"#F8FAFC", g100:"#F1F5F9", g200:"#E2E8F0",
  g300:"#CBD5E1", g400:"#94A3B8", g500:"#64748B", g600:"#475569", g700:"#334155",
  green:"#16A34A", greenTint:"#DCFCE7",
  red:"#DC2626", redTint:"#FEE2E2",
};

function downloadCSV(columns, rows, filename) {
  const header = columns.join(",");
  const body   = rows.map(row =>
    columns.map(c => {
      const val = row[c];
      if (val == null) return "";
      const s = String(val);
      return s.includes(",") || s.includes('"') || s.includes("\n")
        ? `"${s.replace(/"/g, '""')}"`
        : s;
    }).join(",")
  ).join("\n");

  const blob = new Blob([header + "\n" + body], { type: "text/csv;charset=utf-8;" });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement("a");
  a.href     = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

export function NodePreviewPanel({ node, columns, onClose }) {
  const [loading, setLoading]   = useState(false);
  const [preview, setPreview]   = useState(null);
  const [error, setError]       = useState(null);
  const [searchCol, setSearchCol] = useState("");
  const [page, setPage]         = useState(0);
  const PAGE_SIZE = 50;

  const isInput  = node?.data?.type === "input_dataset";
  const isOutput = node?.data?.type === "output_dataset";
  const datasetId = node?.data?.config?.dataset?.id;
  const tableName = node?.data?.config?.dataset?.table_name;

  useEffect(() => {
    if (!node) return;
    if (isInput && datasetId) {
      fetchDatasetPreview(datasetId);
    } else if (!isInput && !isOutput) {
      // Utility node — show column info only (no live preview until DAG runs)
      setPreview({ columns: columns || [], rows: [], isColumnOnly: true });
    }
  }, [node?.id]);

  const fetchDatasetPreview = async (id) => {
    setLoading(true); setError(null);
    try {
      const r = await api.get(`/datasets/${id}/preview?limit=200`);
      setPreview(r.data);
    } catch (e) {
      setError("Could not load preview");
    } finally {
      setLoading(false); }
  };

  if (!node) return null;

  const visibleCols = preview?.columns?.filter(c =>
    c.toLowerCase().includes(searchCol.toLowerCase())
  ) || columns || [];

  const pageRows  = (preview?.rows || []).slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
  const totalPages = Math.ceil((preview?.rows?.length || 0) / PAGE_SIZE);

  const nodeLabel = node.data?.label || "Node";
  const nodeBg    = { input_dataset: C.blue, output_dataset: C.green }[node.data?.type] || C.gold;

  return (
    <div style={{
      position: "absolute", right: 0, top: 0, bottom: 0,
      width: 520, background: C.white,
      borderLeft: `1px solid ${C.g200}`,
      display: "flex", flexDirection: "column",
      zIndex: 50, boxShadow: "-4px 0 24px rgba(0,0,0,0.08)",
      fontFamily: "'DM Sans','Segoe UI',sans-serif",
    }}>
      {/* Header */}
      <div style={{
        background: `linear-gradient(90deg, ${C.navy}, ${nodeBg}22)`,
        padding: "12px 16px",
        display: "flex", justifyContent: "space-between", alignItems: "center",
        flexShrink: 0,
      }}>
        <div>
          <div style={{ color: C.white, fontWeight: 800, fontSize: 14 }}>
            {nodeLabel}
          </div>
          <div style={{ color: "rgba(255,255,255,0.6)", fontSize: 10, marginTop: 1 }}>
            {isInput
              ? `Source: ${node.data?.config?.dataset?.name || "not configured"}`
              : preview?.isColumnOnly
              ? `${columns?.length || 0} columns (output of transformation)`
              : `${preview?.columns?.length || 0} columns · ${preview?.rows?.length || 0} rows`
            }
          </div>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {preview && !preview.isColumnOnly && preview.rows?.length > 0 && (
            <button
              onClick={() => downloadCSV(
                preview.columns,
                preview.rows,
                `${nodeLabel.replace(/\s+/g,"_")}_preview.csv`
              )}
              style={{
                display: "flex", alignItems: "center", gap: 5,
                padding: "5px 10px", borderRadius: 6,
                background: "rgba(255,255,255,0.15)", border: "1px solid rgba(255,255,255,0.2)",
                color: C.white, fontSize: 11, fontWeight: 700, cursor: "pointer",
              }}
            >
              ⬇ Download CSV
            </button>
          )}
          <button onClick={onClose} style={{
            background: "rgba(255,255,255,0.1)", border: "none",
            borderRadius: 6, padding: "4px 8px", cursor: "pointer",
            color: C.white, fontSize: 16,
          }}>×</button>
        </div>
      </div>

      {/* Column chips */}
      {visibleCols.length > 0 && (
        <div style={{ padding: "10px 14px", borderBottom: `1px solid ${C.g200}`, flexShrink: 0, background: C.g50 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
            <div style={{ fontSize: 10, fontWeight: 700, color: C.g400, textTransform: "uppercase", letterSpacing: 1 }}>
              Columns ({visibleCols.length})
            </div>
            <input
              value={searchCol}
              onChange={e => setSearchCol(e.target.value)}
              placeholder="Filter columns..."
              style={{
                padding: "3px 8px", borderRadius: 5, border: `1px solid ${C.g200}`,
                fontSize: 11, outline: "none", color: C.g700, background: C.white,
                flex: 1, maxWidth: 160,
              }}
            />
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 4, maxHeight: 80, overflowY: "auto" }}>
            {visibleCols.map(c => (
              <span key={c} style={{
                fontSize: 10, padding: "2px 7px", borderRadius: 4,
                background: C.blueTint2, color: C.blue,
                fontWeight: 600, fontFamily: "monospace",
              }}>{c}</span>
            ))}
          </div>
        </div>
      )}

      {/* Content */}
      <div style={{ flex: 1, overflow: "hidden", display: "flex", flexDirection: "column" }}>
        {loading && (
          <div style={{ padding: 40, textAlign: "center", color: C.g400 }}>
            Loading preview…
          </div>
        )}

        {error && (
          <div style={{ padding: 20 }}>
            <div style={{ background: C.redTint, borderRadius: 8, padding: 12, fontSize: 12, color: C.red }}>
              {error}
            </div>
          </div>
        )}

        {/* Column-only view for utility nodes not yet run */}
        {preview?.isColumnOnly && (
          <div style={{ padding: 24, textAlign: "center", color: C.g400 }}>
            <div style={{ fontSize: 28, marginBottom: 10 }}>⚡</div>
            <div style={{ fontWeight: 700, fontSize: 13, marginBottom: 4, color: C.g600 }}>
              Output columns preview
            </div>
            <div style={{ fontSize: 11, marginBottom: 16 }}>
              These columns will be available after transformation runs
            </div>
            {columns?.length > 0 ? (
              <div style={{ display: "flex", flexWrap: "wrap", gap: 5, justifyContent: "center" }}>
                {columns.map(c => (
                  <span key={c} style={{
                    fontSize: 11, padding: "4px 10px", borderRadius: 20,
                    background: C.greenTint, color: C.green, fontWeight: 600,
                  }}>{c}</span>
                ))}
              </div>
            ) : (
              <div style={{ fontSize: 11, color: C.g300 }}>
                Connect to an upstream node to see columns
              </div>
            )}
          </div>
        )}

        {/* Data table */}
        {preview && !preview.isColumnOnly && preview.rows?.length > 0 && (
          <>
            <div style={{ flex: 1, overflowX: "auto", overflowY: "auto" }}>
              <table style={{ borderCollapse: "collapse", fontSize: 11, width: "100%" }}>
                <thead>
                  <tr style={{ background: C.g50, position: "sticky", top: 0, zIndex: 1 }}>
                    <th style={{ padding: "6px 10px", color: C.g400, fontWeight: 600, borderBottom: `1px solid ${C.g200}`, width: 32, textAlign: "center", fontSize: 10 }}>#</th>
                    {preview.columns.map(c => (
                      <th key={c} style={{
                        padding: "6px 10px", textAlign: "left",
                        fontWeight: 700, color: C.g600,
                        borderBottom: `2px solid ${C.g200}`,
                        whiteSpace: "nowrap", fontSize: 11,
                        fontFamily: "monospace",
                      }}>{c}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {pageRows.map((row, i) => (
                    <tr key={i}
                      style={{ background: i % 2 === 0 ? C.white : C.g50 }}
                      onMouseEnter={e => e.currentTarget.style.background = C.blueTint}
                      onMouseLeave={e => e.currentTarget.style.background = i % 2 === 0 ? C.white : C.g50}
                    >
                      <td style={{ padding: "5px 10px", color: C.g300, fontSize: 10, textAlign: "center", borderBottom: `1px solid ${C.g100}` }}>
                        {page * PAGE_SIZE + i + 1}
                      </td>
                      {preview.columns.map(c => (
                        <td key={c} style={{
                          padding: "5px 10px",
                          borderBottom: `1px solid ${C.g100}`,
                          color: row[c] == null ? C.g300 : C.g700,
                          fontStyle: row[c] == null ? "italic" : "normal",
                          whiteSpace: "nowrap",
                          maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis",
                        }}>
                          {row[c] == null ? "null" : String(row[c])}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Pagination + download */}
            <div style={{
              padding: "8px 14px", borderTop: `1px solid ${C.g200}`,
              background: C.g50, display: "flex",
              justifyContent: "space-between", alignItems: "center",
              flexShrink: 0,
            }}>
              <span style={{ fontSize: 11, color: C.g400 }}>
                {preview.rows.length} rows · page {page + 1}/{Math.max(1, totalPages)}
              </span>
              <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                <button
                  onClick={() => setPage(p => Math.max(0, p - 1))}
                  disabled={page === 0}
                  style={{ padding: "3px 8px", borderRadius: 5, border: `1px solid ${C.g200}`, background: C.white, cursor: page === 0 ? "not-allowed" : "pointer", color: page === 0 ? C.g300 : C.g600, fontSize: 11 }}
                >←</button>
                <button
                  onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
                  disabled={page >= totalPages - 1}
                  style={{ padding: "3px 8px", borderRadius: 5, border: `1px solid ${C.g200}`, background: C.white, cursor: page >= totalPages - 1 ? "not-allowed" : "pointer", color: page >= totalPages - 1 ? C.g300 : C.g600, fontSize: 11 }}
                >→</button>
                <button
                  onClick={() => downloadCSV(
                    preview.columns,
                    preview.rows,
                    `${nodeLabel.replace(/\s+/g,"_")}_data.csv`
                  )}
                  style={{
                    padding: "3px 10px", borderRadius: 5,
                    border: `1px solid ${C.blue}`,
                    background: C.blueTint, color: C.blue,
                    fontSize: 11, fontWeight: 700, cursor: "pointer",
                    display: "flex", alignItems: "center", gap: 4,
                  }}
                >⬇ CSV</button>
              </div>
            </div>
          </>
        )}

        {/* Empty state */}
        {preview && !preview.isColumnOnly && preview.rows?.length === 0 && !loading && (
          <div style={{ padding: 40, textAlign: "center", color: C.g400 }}>
            <div style={{ fontSize: 28, marginBottom: 8 }}>📭</div>
            <div style={{ fontWeight: 700, marginBottom: 4 }}>No data to preview</div>
            <div style={{ fontSize: 11 }}>This dataset appears to be empty</div>
          </div>
        )}

        {/* Not configured */}
        {!loading && !error && !preview && (
          <div style={{ padding: 40, textAlign: "center", color: C.g400 }}>
            <div style={{ fontSize: 28, marginBottom: 8 }}>⚙️</div>
            <div style={{ fontWeight: 700, marginBottom: 4 }}>Node not configured</div>
            <div style={{ fontSize: 11 }}>Configure this node first to see a preview</div>
          </div>
        )}
      </div>
    </div>
  );
}
