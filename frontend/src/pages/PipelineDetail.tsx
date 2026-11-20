import { useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { FiPlay, FiSquare, FiTrash2, FiRefreshCw, FiSettings, FiInfo } from 'react-icons/fi';
import { format } from 'date-fns';
import { pipelinesApi } from '../services/api';
import { StatusBadge } from '../components/StatusBadge';
import type { PipelineState, PipelineHistory, PipelineConfig } from '../types';

export function PipelineDetail() {
  const { name } = useParams<{ name: string }>();
  const [pipeline, setPipeline] = useState<PipelineState | null>(null);
  const [history, setHistory] = useState<PipelineHistory[]>([]);
  const [config, setConfig] = useState<PipelineConfig | null>(null);
  const [loading, setLoading] = useState(true);
  const [isRunning, setIsRunning] = useState(false);
  const [fullLoad, setFullLoad] = useState(false);
  const [actionMessage, setActionMessage] = useState<string | null>(null);

  const fetchData = async () => {
    if (!name) return;
    try {
      const [pipelineData, historyData, configData, runningData] = await Promise.all([
        pipelinesApi.getOne(name),
        pipelinesApi.getHistory(name),
        pipelinesApi.getConfig(name),
        pipelinesApi.getRunning(),
      ]);
      setPipeline(pipelineData);
      setHistory(historyData);
      setConfig(configData);
      setIsRunning(runningData.includes(name));
    } catch (err) {
      console.error('Failed to load pipeline data:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [name]);

  const handleRun = async () => {
    if (!name) return;
    try {
      setActionMessage('Starting pipeline...');
      const result = await pipelinesApi.run(name, { fullLoad });
      if (result.success) {
        setActionMessage(result.message);
        setIsRunning(true);
        fetchData();
      } else {
        setActionMessage(`Error: ${result.message}`);
      }
    } catch (err: any) {
      setActionMessage(`Failed to start pipeline: ${err.response?.data?.message || err.message || 'Unknown error'}`);
    }
  };

  const handleStop = async () => {
    if (!name) return;
    try {
      setActionMessage('Stopping pipeline...');
      const result = await pipelinesApi.stop(name);
      setActionMessage(result.message);
      setIsRunning(false);
      fetchData();
    } catch (err: any) {
      setActionMessage(`Failed to stop pipeline: ${err.response?.data?.message || err.message || 'Unknown error'}`);
    }
  };

  const handleClearQueue = async () => {
    if (!name) return;
    try {
      setActionMessage('Clearing queue...');
      const result = await pipelinesApi.clearQueue(name);
      setActionMessage(result.message);
      fetchData();
    } catch (err: any) {
      setActionMessage(`Failed to clear queue: ${err.response?.data?.message || err.message || 'Unknown error'}`);
    }
  };

  if (loading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', minHeight: '60vh' }}>
        <div className="spinner" />
      </div>
    );
  }

  if (!pipeline) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', minHeight: '60vh' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 18, fontWeight: 600 }}>Pipeline not found</div>
          <Link to="/" style={{ color: 'var(--accent-2)', marginTop: 8, display: 'inline-block' }}>Back to Dashboard</Link>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="breadcrumb">
        <Link to="/">Dashboard</Link>
        <span>›</span>
        <Link to="/pipelines">Pipelines</Link>
        <span>›</span>
        <span className="current">{pipeline.pipelineName}</span>
      </div>

      <div className="page-header">
        <div>
          <div className="page-title">{pipeline.pipelineName}</div>
          <div className="page-sub">Pipeline Details</div>
        </div>
        <StatusBadge status={isRunning ? 'running' : pipeline.lastRunStatus} />
      </div>

      <div style={{ background: 'var(--accent-dim)', border: '1px solid var(--accent)', borderRadius: 10, padding: 14, marginBottom: 20, display: 'flex', gap: 10, alignItems: 'flex-start' }}>
        <FiInfo width={16} height={16} style={{ color: 'var(--accent)', marginTop: 2, flexShrink: 0 }} />
        <div style={{ fontSize: 13, color: 'var(--text-secondary)' }}>
          <strong style={{ color: 'var(--text-primary)' }}>About Streaming Pipelines: </strong>
          This pipeline runs continuously, checking the source for new records on an interval using the TMSTAMP field, and processes only new data since the last run. It keeps running until manually stopped. Use "Full Load" to reprocess all data from scratch.
        </div>
      </div>

      {actionMessage && (
        <div style={{ background: 'var(--accent-dim)', border: '1px solid var(--accent)', borderRadius: 8, padding: '10px 14px', marginBottom: 20, fontSize: 13, color: 'var(--text-primary)' }}>
          {actionMessage}
        </div>
      )}

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-label" style={{ marginBottom: 6 }}>Status</div>
          <div className="stat-value" style={{ fontSize: 18 }}>
            {isRunning ? 'Running' : (pipeline.lastRunStatus === 'completed' ? 'Idle' : pipeline.lastRunStatus || 'Unknown')}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label" style={{ marginBottom: 6 }}>Last Run</div>
          <div className="stat-value" style={{ fontSize: 18 }}>
            {pipeline.lastRun ? format(new Date(pipeline.lastRun), 'MMM d, HH:mm') : 'Never'}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label" style={{ marginBottom: 6 }}>Records Processed</div>
          <div className="stat-value" style={{ fontSize: 18 }}>{(pipeline.recordsProcessed || 0).toLocaleString()}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label" style={{ marginBottom: 6 }}>Last Successful Run</div>
          <div className="stat-value" style={{ fontSize: 18 }}>
            {pipeline.lastSuccessfulRun ? format(new Date(pipeline.lastSuccessfulRun), 'MMM d, HH:mm') : 'Never'}
          </div>
        </div>
      </div>

      <div className="card" style={{ padding: 18, marginBottom: 20 }}>
        <div className="card-title" style={{ marginBottom: 14 }}>Actions</div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10, alignItems: 'center' }}>
          {isRunning ? (
            <button onClick={handleStop} className="btn-danger">
              <FiSquare width={14} height={14} />
              Stop Pipeline
            </button>
          ) : (
            <button onClick={handleRun} className="btn-accent2">
              <FiPlay width={14} height={14} />
              Run Pipeline
            </button>
          )}
          <button onClick={handleClearQueue} className="btn-secondary">
            <FiTrash2 width={14} height={14} />
            Clear Queue
          </button>
          <button onClick={fetchData} className="btn-secondary">
            <FiRefreshCw width={14} height={14} />
            Refresh
          </button>
          <label className="btn-secondary" style={{ cursor: 'pointer' }}>
            <input
              type="checkbox"
              checked={fullLoad}
              onChange={(e) => setFullLoad(e.target.checked)}
              style={{ width: 14, height: 14, accentColor: 'var(--accent-2)' }}
            />
            Full Load
          </label>
        </div>
      </div>

      {config && (
        <div className="card" style={{ padding: 18, marginBottom: 20 }}>
          <div className="card-title" style={{ marginBottom: 14, display: 'flex', alignItems: 'center', gap: 8 }}>
            <FiSettings width={15} height={15} />
            Configuration
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 14 }}>
            <div>
              <div className="stat-label">Mode</div>
              <div style={{ fontWeight: 500, marginTop: 2 }}>{config.mode}</div>
            </div>
            <div>
              <div className="stat-label">Schedule</div>
              <div style={{ fontWeight: 500, marginTop: 2 }}>{config.schedule}</div>
            </div>
            <div>
              <div className="stat-label">Source</div>
              <div style={{ fontWeight: 500, marginTop: 2 }}>{config.source}</div>
            </div>
            <div>
              <div className="stat-label">Queue</div>
              <div style={{ fontWeight: 500, marginTop: 2 }}>{config.queue}</div>
            </div>
          </div>
        </div>
      )}

      <div className="card">
        <div className="card-header">
          <div className="card-title">Run History</div>
        </div>
        <table className="data-table">
          <thead>
            <tr>
              <th>Run Time</th>
              <th>Status</th>
              <th>Records</th>
              <th>Error</th>
            </tr>
          </thead>
          <tbody>
            {history.map((run, index) => (
              <tr key={index}>
                <td>{run.runTime ? format(new Date(run.runTime), 'MMM d, yyyy HH:mm:ss') : '-'}</td>
                <td><StatusBadge status={run.status} size="sm" /></td>
                <td style={{ color: 'var(--text-secondary)' }}>{run.records.toLocaleString()}</td>
                <td style={{ color: 'var(--red)', maxWidth: 260, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {run.errorMessage || '-'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
