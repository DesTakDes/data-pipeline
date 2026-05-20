// frontend/src/api.js
import axios from 'axios';

// PERBAIKAN 1: Arahkan langsung ke URL backend FastAPI kamu.
// Asumsi backend Python kamu jalan di port 8000. (Ubah jika port-nya beda)
const api = axios.create({ baseURL: 'http://localhost:8000/api' });

// ── Datasets ──────────────────────────────────────────────────────
export const getDatasets     = ()         => api.get('/datasets');
export const deleteDataset   = (id)       => api.delete(`/datasets/${id}`);
export const previewDataset  = (id)       => api.get(`/datasets/${id}/preview`);

export const uploadDataset   = (file, name) => {
  const form = new FormData();
  form.append('file', file);
  if (name) form.append('name', name);
  return api.post('/datasets/upload', form);
};

export const connectDB = (payload) => api.post('/datasets/connect-db', payload);

// ── Airflow ───────────────────────────────────────────────────────
export const getAirflowStatus = ()              => api.get('/airflow/status');
export const getDagRuns       = (dagId)         => api.get(`/airflow/dags/${dagId}/runs`);
export const triggerDag       = (dagId, force)  => api.post(`/airflow/dags/${dagId}/trigger?force=${force}`);
export const getTaskInstances = (dagId, runId)  => api.get(`/airflow/dags/${dagId}/runs/${runId}/tasks`);

// ── Warehouse ─────────────────────────────────────────────────────
export const getWarehouseTables = () => api.get('/warehouse/tables');

// ── Pipeline (PERBAIKAN UTAMA DI SINI) ────────────────────────────

// 1. Sesuaikan nama fungsinya dengan yang dipanggil di App.jsx
// 2. Sesuaikan endpointnya pakai "pipelines" (ada 's' nya)
export const runNewPipeline = (payload) => api.post('/pipelines/run', payload);

// 3. Tambahkan fungsi untuk Polling Status DAG yang sebelumnya hilang
export const getDagStatus = (runId) => api.get(`/pipelines/runs/${runId}/dag-status`);