import { ReactFlow, Background, Controls, MiniMap,
         addEdge, useNodesState, useEdgesState } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { useCallback, useEffect, useState } from 'react';
import axios from 'axios';

// ── Konstanta ─────────────────────────────────────────────────────
const DAG_ID       = 'etl_pipeline';
const AIRFLOW_BASE = '/airflow-api/api/v1';
const AUTH         = { username: 'admin', password: 'admin' };

const STATUS_COLOR = {
  success: '#1A7F37',
  running: '#1F6FEB',
  failed:  '#CF222E',
  queued:  '#9A6700',
  skipped: '#8250DF',
  none:    '#D0D7DE',
};

const STATUS_BG = {
  success: '#EAFBEE',
  running: '#EEF5FF',
  failed:  '#FFEBE9',
  queued:  '#FFF8E1',
  skipped: '#F3EEFF',
  none:    '#F6F8FA',
};

// ── Node awal ────────────────────────────────────────────────────
const initialNodes = [
  {
    id: 'extract',
    position: { x: 50, y: 120 },
    data: { label: '📥 Extract' },
    style: {
      background: STATUS_BG.none,
      border: `2px solid ${STATUS_COLOR.none}`,
      borderRadius: 10, padding: '12px 20px',
      fontWeight: 600, fontSize: 13, minWidth: 140,
    }
  },
  {
    id: 'transform',
    position: { x: 280, y: 120 },
    data: { label: '⚡ Spark Transform' },
    style: {
      background: STATUS_BG.none,
      border: `2px solid ${STATUS_COLOR.none}`,
      borderRadius: 10, padding: '12px 20px',
      fontWeight: 600, fontSize: 13, minWidth: 160,
    }
  },
  {
    id: 'load',
    position: { x: 530, y: 120 },
    data: { label: '🗄️ Load to DWH' },
    style: {
      background: STATUS_BG.none,
      border: `2px solid ${STATUS_COLOR.none}`,
      borderRadius: 10, padding: '12px 20px',
      fontWeight: 600, fontSize: 13, minWidth: 140,
    }
  },
  {
    id: 'metabase',
    position: { x: 280, y: 280 },
    data: { label: '📊 Metabase Dashboard' },
    style: {
      background: '#F3EEFF',
      border: '2px solid #8250DF',
      borderRadius: 10, padding: '12px 20px',
      fontWeight: 600, fontSize: 13, minWidth: 180,
    }
  },
];

const initialEdges = [
  { id: 'e1', source: 'extract',   target: 'transform', animated: true,  style: { stroke: '#1F6FEB' } },
  { id: 'e2', source: 'transform', target: 'load',      animated: true,  style: { stroke: '#1F6FEB' } },
  { id: 'e3', source: 'load',      target: 'metabase',  animated: false, style: { strokeDasharray: '5 5', stroke: '#8250DF' } },
];

// ── Axios instance ────────────────────────────────────────────────
const airflow = axios.create({
  baseURL: AIRFLOW_BASE,
  auth: AUTH,
});

// ── Komponen utama ────────────────────────────────────────────────
export default function PipelineCanvas() {
  const [selectedPipelineDS, setSelectedPipelineDS] = useState(null);
  const [showDSSelector, setShowDSSelector] = useState(false);
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
  const [status, setStatus] = useState('Menghubungkan ke Airflow...');
  const [lastRun, setLastRun] = useState(null);
  const [error, setError]     = useState(null);

  const fetchStatus = async () => {
    try {
      // 1. Ambil DAG run terbaru
      const { data: runData } = await airflow.get(
        `/dags/${DAG_ID}/dagRuns?limit=1&order_by=-execution_date`
      );

      const run = runData.dag_runs?.[0];
      if (!run) {
        setStatus('Belum ada DAG run — trigger manual di Airflow UI.');
        return;
      }

      setLastRun(run);
      setStatus(`Run: ${run.dag_run_id} | State: ${run.state} | ${new Date().toLocaleTimeString()}`);

      // 2. Ambil status tiap task
      const { data: taskData } = await airflow.get(
        `/dags/${DAG_ID}/dagRuns/${run.dag_run_id}/taskInstances`
      );

      const stateMap = {};
      taskData.task_instances?.forEach(t => {
        stateMap[t.task_id] = t.state || 'none';
      });

      console.log('Task states:', stateMap);

      // 3. Update warna node sesuai status
      setNodes(nds => nds.map(n => {
        const state = stateMap[n.id] || 'none';
        return {
          ...n,
          style: {
            ...n.style,
            background:  STATUS_BG[state]   || STATUS_BG.none,
            borderColor: STATUS_COLOR[state] || STATUS_COLOR.none,
          }
        };
      }));

      setError(null);
    } catch (err) {
      const msg = err.response
        ? `Airflow API error: ${err.response.status}`
        : err.message;
      setError(msg);
      setStatus('❌ Error: ' + msg);
      console.error('Fetch error:', err);
    }
  };

  useEffect(() => {
    fetchStatus();
    const interval = setInterval(fetchStatus, 10000);
    return () => clearInterval(interval);
  }, []);

  const onConnect = useCallback(
    (params) => setEdges(eds => addEdge({ ...params, animated: true }, eds)),
    [setEdges]
  );

  return (
    <div style={{ height: '100vh', width: '100%', fontFamily: 'Arial, sans-serif' }}>

      {/* Status bar */}
      <div style={{
        padding: '10px 20px',
        background: error ? '#FFEBE9' : '#EEF5FF',
        borderBottom: `1px solid ${error ? '#CF222E' : '#C0D8FF'}`,
        fontSize: 13,
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
      }}>
        <span style={{ color: error ? '#CF222E' : '#1F2328', fontWeight: 600 }}>
          {status}
        </span>
        <button onClick={fetchStatus} style={{
          padding: '4px 12px', borderRadius: 6,
          border: '1px solid #C0D8FF', background: '#FFFFFF',
          cursor: 'pointer', fontSize: 12, fontWeight: 600,
        }}>
          🔄 Refresh
        </button>
      </div>

      {/* Legend status */}
      <div style={{
        padding: '8px 20px', background: '#F6F8FA',
        borderBottom: '1px solid #D0D7DE',
        display: 'flex', gap: 20, fontSize: 12,
      }}>
        {Object.entries(STATUS_COLOR).map(([state, color]) => (
          <span key={state} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{
              width: 12, height: 12, borderRadius: 3,
              background: STATUS_BG[state] || '#F6F8FA',
              border: `2px solid ${color}`,
              display: 'inline-block',
            }}/>
            {state}
          </span>
        ))}
      </div>

      {/* Canvas ReactFlow */}
      <div style={{ height: 'calc(100vh - 90px)' }}>
        <ReactFlow
          nodes={nodes} edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          fitView
        >
          <Background variant="dots" gap={16} color="#D0D7DE" />
          <Controls />
          <MiniMap />
        </ReactFlow>
      </div>
    </div>
  );
}