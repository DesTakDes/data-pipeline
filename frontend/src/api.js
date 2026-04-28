// frontend/src/api.js
import axios from 'axios';

const api = axios.create({ baseURL: '/api' });

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

// ── Pipeline ──────────────────────────────────────────────────────
export const runPipeline = (nodes, edges) => api.post('/pipeline/run', { nodes, edges });