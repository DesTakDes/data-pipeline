// src/workflowCache.js
const STORAGE_KEY = "etl_workflows_v2";
let _cache        = null;   // in-memory cache
let _dirty        = false;  // ada perubahan yang belum di-flush

// Baca sekali, simpan di memory
export function getWorkflows() {
  if (_cache !== null) return _cache;  // ← langsung return, tidak baca disk lagi
  try {
    const raw = JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
    _cache = Array.isArray(raw)
      ? raw
          .filter(w => w && typeof w === "object" && w.id)
          .map(w => ({
            ...w,
            name:        w.name        || "Untitled Workflow",
            description: w.description || "",
            status:      w.status      || "draft",
            nodes:       Array.isArray(w.nodes) ? w.nodes : [],
            edges:       Array.isArray(w.edges) ? w.edges : [],
            createdAt:   w.createdAt   || new Date().toISOString(),
            updatedAt:   w.updatedAt   || new Date().toISOString(),
          }))
      : [];
  } catch {
    _cache = [];
  }
  return _cache;
}

export function saveWorkflow(w) {
  if (!w?.id) return w;
  const sanitized = {
    ...w,
    name:      (w.name || "").trim() || "Untitled Workflow",
    status:    w.status  || "draft",
    nodes:     Array.isArray(w.nodes) ? w.nodes : [],
    edges:     Array.isArray(w.edges) ? w.edges : [],
    updatedAt: new Date().toISOString(),
  };
  // Update cache dulu (sync, instant)
  _cache = _cache
    ? [..._cache.filter(x => x.id !== sanitized.id), sanitized]
    : [sanitized];
  // Tulis ke disk secara async (tidak blocking UI)
  _dirty = true;
  _scheduledFlush();
  return sanitized;
}

export function deleteWorkflow(id) {
  _cache = (_cache || []).filter(w => w.id !== id);
  _dirty = true;
  _scheduledFlush();
}

// Flush ke localStorage pakai requestIdleCallback
// supaya tidak blocking render
let _flushTimer = null;
function _scheduledFlush() {
  if (_flushTimer) return;
  _flushTimer = (
    typeof requestIdleCallback !== "undefined"
      ? requestIdleCallback
      : (cb) => setTimeout(cb, 100)
  )(() => {
    if (_dirty && _cache !== null) {
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(_cache));
        _dirty = false;
      } catch (e) {
        console.error("flush workflows failed:", e);
      }
    }
    _flushTimer = null;
  });
}

// Invalidate cache (pakai kalau butuh force reload)
export function invalidateCache() {
  _cache = null;
  _dirty = false;
}