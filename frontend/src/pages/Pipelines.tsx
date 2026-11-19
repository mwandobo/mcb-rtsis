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

const BRAND_BLUE = '#1B3E74';
const BRAND_ORANGE = '#DA7F3D';
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
      <div className="flex items-center justify-center min-h-[60vh]">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2" style={{ borderColor: BRAND_BLUE }}></div>
      </div>
    );
  }

  return (
    <div>
      {/* Page Header */}
      <div className="mb-4 flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold text-gray-900">Pipelines</h1>
          <p className="text-xs text-gray-500 mt-0.5">Manage and monitor all data pipelines</p>
        </div>
        <button
          onClick={fetchData}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-white rounded-md hover:opacity-90 transition-opacity"
          style={{ backgroundColor: BRAND_BLUE }}
        >
          <FiRefreshCw className="w-3.5 h-3.5" />
          Refresh
        </button>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
        <div className="bg-white rounded-lg border border-gray-100 shadow-sm p-3 flex items-center justify-between">
          <div>
            <p className="text-[11px] text-gray-400">Total Pipelines</p>
            <p className="text-xl font-bold text-gray-900 mt-0.5">{counts.total}</p>
          </div>
          <div className="p-2 rounded-lg bg-blue-50 text-blue-600">
            <FiServer className="w-4 h-4" />
          </div>
        </div>
        <div className="bg-white rounded-lg border border-gray-100 shadow-sm p-3 flex items-center justify-between">
          <div>
            <p className="text-[11px] text-gray-400">Running</p>
            <p className="text-xl font-bold text-green-600 mt-0.5">{counts.running}</p>
          </div>
          <div className="p-2 rounded-lg bg-green-50 text-green-600">
            <FiActivity className="w-4 h-4" />
          </div>
        </div>
        <div className="bg-white rounded-lg border border-gray-100 shadow-sm p-3 flex items-center justify-between">
          <div>
            <p className="text-[11px] text-gray-400">Idle</p>
            <p className="text-xl font-bold text-gray-900 mt-0.5">{counts.idle}</p>
          </div>
          <div className="p-2 rounded-lg bg-gray-100 text-gray-500">
            <FiPauseCircle className="w-4 h-4" />
          </div>
        </div>
        <div className="bg-white rounded-lg border border-gray-100 shadow-sm p-3 flex items-center justify-between">
          <div>
            <p className="text-[11px] text-gray-400">Failed</p>
            <p className="text-xl font-bold text-red-600 mt-0.5">{counts.failed}</p>
          </div>
          <div className="p-2 rounded-lg bg-red-50 text-red-600">
            <FiAlertTriangle className="w-4 h-4" />
          </div>
        </div>
      </div>

      {/* Pipelines Table */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-100 overflow-hidden">
        {/* Toolbar: search + filter */}
        <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between gap-3">
          <div className="relative w-64">
            <FiSearch className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400" />
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search pipelines..."
              className="w-full pl-8 pr-3 py-1.5 text-xs border border-gray-200 rounded-md focus:outline-none focus:ring-1 focus:ring-gray-300 placeholder:text-gray-400"
            />
          </div>
          <div className="flex items-center gap-2">
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value as StatusFilter)}
              className="text-xs border border-gray-200 rounded-md py-1.5 px-2 text-gray-600 focus:outline-none focus:ring-1 focus:ring-gray-300"
            >
              <option value="all">All statuses</option>
              <option value="running">Running</option>
              <option value="completed">Completed</option>
              <option value="idle">Idle</option>
              <option value="failed">Failed</option>
            </select>
            <span className="text-xs text-gray-400">{filteredPipelines.length} results</span>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50/80 border-b border-gray-100">
              <tr>
                <th className="px-4 py-2 text-left text-[11px] font-medium text-gray-400 uppercase tracking-wide">
                  Pipeline
                </th>
                <th className="px-4 py-2 text-left text-[11px] font-medium text-gray-400 uppercase tracking-wide">
                  Status
                </th>
                <th className="px-4 py-2 text-left text-[11px] font-medium text-gray-400 uppercase tracking-wide">
                  Last Run
                </th>
                <th className="px-4 py-2 text-left text-[11px] font-medium text-gray-400 uppercase tracking-wide">
                  Records
                </th>
                <th className="px-4 py-2 text-left text-[11px] font-medium text-gray-400 uppercase tracking-wide">
                  Mode
                </th>
                <th className="px-4 py-2 text-right text-[11px] font-medium text-gray-400 uppercase tracking-wide">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {paginatedPipelines.map((pipeline) => {
                const isRunning = runningPipelines.includes(pipeline.pipelineName);
                return (
                  <tr key={pipeline.pipelineName} className="hover:bg-gray-50/60 transition-colors">
                    <td className="px-4 py-2.5">
                      <Link
                        to={`/pipeline/${pipeline.pipelineName}`}
                        className="text-sm font-medium hover:opacity-80 flex items-center gap-1.5"
                        style={{ color: BRAND_BLUE }}
                      >
                        {pipeline.pipelineName}
                        <FiExternalLink className="w-3 h-3 text-gray-400" />
                      </Link>
                    </td>
                    <td className="px-4 py-2.5">
                      <StatusBadge status={isRunning ? 'running' : pipeline.lastRunStatus} size="sm" />
                    </td>
                    <td className="px-4 py-2.5 text-xs text-gray-500">
                      {pipeline.lastRun ? format(new Date(pipeline.lastRun), 'MMM d, HH:mm') : 'Never'}
                    </td>
                    <td className="px-4 py-2.5 text-xs text-gray-500">
                      {pipeline.recordsProcessed?.toLocaleString() || '0'}
                    </td>
                    <td className="px-4 py-2.5">
                      <label className="flex items-center gap-1.5 cursor-pointer select-none">
                        <input
                          type="checkbox"
                          checked={fullLoadMode[pipeline.pipelineName] || false}
                          onChange={() => toggleFullLoad(pipeline.pipelineName)}
                          className="w-3 h-3 rounded border-gray-300"
                          style={{ accentColor: BRAND_ORANGE }}
                        />
                        <span className="text-xs text-gray-500">Full load</span>
                      </label>
                    </td>
                    <td className="px-4 py-2.5">
                      <div className="flex items-center justify-end gap-1">
                        {isRunning ? (
                          <button
                            onClick={() => handleStop(pipeline.pipelineName)}
                            className="p-1.5 text-red-600 hover:bg-red-50 rounded-md transition-colors"
                            title="Stop"
                          >
                            <FiSquare className="w-3.5 h-3.5" />
                          </button>
                        ) : (
                          <button
                            onClick={() =>
                              handleRun(pipeline.pipelineName, fullLoadMode[pipeline.pipelineName] || false)
                            }
                            className="p-1.5 rounded-md transition-colors"
                            style={{ color: BRAND_ORANGE, backgroundColor: `${BRAND_ORANGE}15` }}
                            title="Run"
                          >
                            <FiPlay className="w-3.5 h-3.5" />
                          </button>
                        )}
                        <button
                          onClick={() => handleClearQueue(pipeline.pipelineName)}
                          className="p-1.5 text-gray-500 hover:bg-gray-100 rounded-md transition-colors"
                          title="Clear Queue"
                        >
                          <FiTrash2 className="w-3.5 h-3.5" />
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
              {paginatedPipelines.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-4 py-8 text-center text-xs text-gray-400">
                    No pipelines match your search.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div className="px-4 py-2.5 border-t border-gray-100 flex items-center justify-between">
          <span className="text-xs text-gray-400">
            Showing {filteredPipelines.length === 0 ? 0 : (currentPage - 1) * PAGE_SIZE + 1}-
            {Math.min(currentPage * PAGE_SIZE, filteredPipelines.length)} of {filteredPipelines.length}
          </span>
          <div className="flex items-center gap-1">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={currentPage === 1}
              className="p-1.5 rounded-md text-gray-500 hover:bg-gray-100 transition-colors disabled:opacity-30 disabled:hover:bg-transparent"
              title="Previous page"
            >
              <FiChevronLeft className="w-3.5 h-3.5" />
            </button>
            <span className="text-xs text-gray-500 px-1">
              Page {currentPage} of {totalPages}
            </span>
            <button
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={currentPage === totalPages}
              className="p-1.5 rounded-md text-gray-500 hover:bg-gray-100 transition-colors disabled:opacity-30 disabled:hover:bg-transparent"
              title="Next page"
            >
              <FiChevronRight className="w-3.5 h-3.5" />
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
