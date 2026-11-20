import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  FiPlay,
  FiSquare,
  FiTrash2,
  FiExternalLink,
  FiRefreshCw,
  FiChevronLeft,
  FiChevronRight,
  FiSearch,
  FiServer,
  FiActivity,
  FiPauseCircle,
  FiAlertTriangle,
} from 'react-icons/fi';
import { format } from 'date-fns';
import { pipelinesApi } from '../services/api';
import { StatusBadge } from '../components/StatusBadge';
import type { PipelineState } from '../types';

const PAGE_SIZE = 10;

type StatusFilter = 'all' | 'running' | 'completed' | 'idle' | 'failed';

export function Pipelines() {
  const [pipelines, setPipelines] = useState<PipelineState[]>([]);
  const [runningPipelines, setRunningPipelines] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [fullLoadMode, setFullLoadMode] = useState<Record<string, boolean>>({});
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState('');
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all');

  const fetchData = async () => {
    try {
      const [pipelinesData, runningData] = await Promise.all([
        pipelinesApi.getAll(),
        pipelinesApi.getRunning(),
      ]);
      setPipelines(pipelinesData);
      setRunningPipelines(runningData);
    } catch (err) {
      console.error('Failed to load pipelines:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    setPage(1);
  }, [search, statusFilter]);

  const handleRun = async (name: string, fullLoad: boolean) => {
    try {
      const result = await pipelinesApi.run(name, { fullLoad });
      if (result.success) {
        fetchData();
      } else {
        alert(`Error: ${result.message}`);
      }
    } catch (err: any) {
      alert(`Failed to run pipeline: ${err.response?.data?.message || err.message || 'Unknown error'}`);
      console.error('Failed to run pipeline:', err);
    }
  };

  const handleStop = async (name: string) => {
    try {
      const result = await pipelinesApi.stop(name);
      if (result.success) {
        fetchData();
      } else {
        alert(`Error: ${result.message}`);
      }
    } catch (err: any) {
      alert(`Failed to stop pipeline: ${err.response?.data?.message || err.message || 'Unknown error'}`);
      console.error('Failed to stop pipeline:', err);
    }
  };

  const handleClearQueue = async (name: string) => {
    try {
      const result = await pipelinesApi.clearQueue(name);
      if (result.success) {
        fetchData();
      } else {
        alert(`Error: ${result.message}`);
      }
    } catch (err: any) {
      alert(`Failed to clear queue: ${err.response?.data?.message || err.message || 'Unknown error'}`);
      console.error('Failed to clear queue:', err);
    }
  };

  const toggleFullLoad = (name: string) => {
    setFullLoadMode((prev) => ({ ...prev, [name]: !prev[name] }));
  };

  const effectiveStatus = (pipeline: PipelineState) =>
    runningPipelines.includes(pipeline.pipelineName) ? 'running' : pipeline.lastRunStatus || 'unknown';

  const counts = useMemo(
    () => ({
      total: pipelines.length,
      running: pipelines.filter((p) => effectiveStatus(p) === 'running').length,
      idle: pipelines.filter((p) => effectiveStatus(p) === 'idle').length,
      failed: pipelines.filter((p) => effectiveStatus(p) === 'failed').length,
    }),
    [pipelines, runningPipelines]
  );

  const filteredPipelines = useMemo(() => {
    return pipelines.filter((p) => {
      if (search && !p.pipelineName.toLowerCase().includes(search.toLowerCase())) return false;
      if (statusFilter !== 'all' && effectiveStatus(p) !== statusFilter) return false;
      return true;
    });
  }, [pipelines, runningPipelines, search, statusFilter]);

  const totalPages = Math.max(1, Math.ceil(filteredPipelines.length / PAGE_SIZE));
  const currentPage = Math.min(page, totalPages);
  const paginatedPipelines = filteredPipelines.slice(
    (currentPage - 1) * PAGE_SIZE,
    currentPage * PAGE_SIZE
  );

  if (loading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', minHeight: '60vh' }}>
        <div className="spinner" />
      </div>
    );
  }

  const statusFilters: [StatusFilter, string][] = [
    ['all', 'All'],
    ['running', 'Running'],
    ['completed', 'Completed'],
    ['idle', 'Idle'],
    ['failed', 'Failed'],
  ];

  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title">Pipelines</div>
          <div className="page-sub">Manage and monitor all data pipelines</div>
        </div>
        <button className="btn-primary" onClick={fetchData}>
          <FiRefreshCw width={14} height={14} />
          Refresh
        </button>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-top">
            <div className="stat-icon blue"><FiServer width={16} height={16} /></div>
          </div>
          <div className="stat-value">{counts.total}</div>
          <div className="stat-label">Total Pipelines</div>
        </div>
        <div className="stat-card">
          <div className="stat-top">
            <div className="stat-icon green"><FiActivity width={16} height={16} /></div>
          </div>
          <div className="stat-value" style={{ color: 'var(--green)' }}>{counts.running}</div>
          <div className="stat-label">Running</div>
        </div>
        <div className="stat-card">
          <div className="stat-top">
            <div className="stat-icon amber"><FiPauseCircle width={16} height={16} /></div>
          </div>
          <div className="stat-value">{counts.idle}</div>
          <div className="stat-label">Idle</div>
        </div>
        <div className="stat-card">
          <div className="stat-top">
            <div className="stat-icon red"><FiAlertTriangle width={16} height={16} /></div>
          </div>
          <div className="stat-value" style={{ color: 'var(--red)' }}>{counts.failed}</div>
          <div className="stat-label">Failed</div>
        </div>
      </div>

      <div className="toolbar">
        <div className="filters">
          {statusFilters.map(([key, label]) => (
            <button
              key={key}
              className={`filter-pill ${statusFilter === key ? 'active' : ''}`}
              onClick={() => setStatusFilter(key)}
            >
              {label} <span className="pill-count">{key === 'all' ? counts.total : counts[key as keyof typeof counts] ?? 0}</span>
            </button>
          ))}
        </div>
        <div className="search-box">
          <FiSearch width={13} height={13} />
          <input
            type="text"
            placeholder="Search pipelines…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
      </div>

      <div className="card">
        <table className="data-table">
          <thead>
            <tr>
              <th>Pipeline</th>
              <th>Status</th>
              <th>Last Run</th>
              <th>Records</th>
              <th>Mode</th>
              <th style={{ textAlign: 'right' }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {paginatedPipelines.map((pipeline) => {
              const isRunning = runningPipelines.includes(pipeline.pipelineName);
              return (
                <tr key={pipeline.pipelineName}>
                  <td>
                    <Link
                      to={`/pipeline/${pipeline.pipelineName}`}
                      style={{ color: 'var(--accent)', fontWeight: 500, display: 'flex', alignItems: 'center', gap: 6, textDecoration: 'none' }}
                    >
                      {pipeline.pipelineName}
                      <FiExternalLink width={12} height={12} style={{ color: 'var(--text-muted)' }} />
                    </Link>
                  </td>
                  <td>
                    <StatusBadge status={isRunning ? 'running' : pipeline.lastRunStatus} size="sm" />
                  </td>
                  <td style={{ color: 'var(--text-secondary)' }}>
                    {pipeline.lastRun ? format(new Date(pipeline.lastRun), 'MMM d, HH:mm') : 'Never'}
                  </td>
                  <td style={{ color: 'var(--text-secondary)' }}>
                    {pipeline.recordsProcessed?.toLocaleString() || '0'}
                  </td>
                  <td>
                    <label style={{ display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer', fontSize: 12.5, color: 'var(--text-secondary)' }}>
                      <input
                        type="checkbox"
                        checked={fullLoadMode[pipeline.pipelineName] || false}
                        onChange={() => toggleFullLoad(pipeline.pipelineName)}
                        style={{ width: 13, height: 13, accentColor: 'var(--accent-2)' }}
                      />
                      Full load
                    </label>
                  </td>
                  <td>
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 6 }}>
                      {isRunning ? (
                        <button
                          onClick={() => handleStop(pipeline.pipelineName)}
                          className="icon-btn danger"
                          title="Stop"
                        >
                          <FiSquare width={14} height={14} />
                        </button>
                      ) : (
                        <button
                          onClick={() => handleRun(pipeline.pipelineName, fullLoadMode[pipeline.pipelineName] || false)}
                          className="icon-btn accent2"
                          title="Run"
                        >
                          <FiPlay width={14} height={14} />
                        </button>
                      )}
                      <button
                        onClick={() => handleClearQueue(pipeline.pipelineName)}
                        className="icon-btn"
                        title="Clear Queue"
                      >
                        <FiTrash2 width={14} height={14} />
                      </button>
                    </div>
                  </td>
                </tr>
              );
            })}
            {paginatedPipelines.length === 0 && (
              <tr>
                <td colSpan={6} style={{ textAlign: 'center', color: 'var(--text-muted)', padding: '32px 18px' }}>
                  No pipelines match your search.
                </td>
              </tr>
            )}
          </tbody>
        </table>

        <div className="pagination-bar">
          <span className="pagination-info">
            Showing {filteredPipelines.length === 0 ? 0 : (currentPage - 1) * PAGE_SIZE + 1}-
            {Math.min(currentPage * PAGE_SIZE, filteredPipelines.length)} of {filteredPipelines.length}
          </span>
          <div className="pagination-controls">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={currentPage === 1}
              className="icon-btn"
              title="Previous page"
            >
              <FiChevronLeft width={14} height={14} />
            </button>
            <span style={{ fontSize: 12, color: 'var(--text-secondary)', padding: '0 4px' }}>
              Page {currentPage} of {totalPages}
            </span>
            <button
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={currentPage === totalPages}
              className="icon-btn"
              title="Next page"
            >
              <FiChevronRight width={14} height={14} />
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
