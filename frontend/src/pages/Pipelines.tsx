import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { FiPlay, FiSettings, FiTrash2, FiExternalLink, FiRefreshCw, FiPlus } from 'react-icons/fi';
import { format } from 'date-fns';
import { pipelinesApi } from '../services/api';
import { StatusBadge } from '../components/StatusBadge';
import type { PipelineState } from '../types';

const BRAND_BLUE = '#1B3E74';
const BRAND_ORANGE = '#DA7F3D';

export function Pipelines() {
  const [pipelines, setPipelines] = useState<PipelineState[]>([]);
  const [runningPipelines, setRunningPipelines] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [fullLoadMode, setFullLoadMode] = useState<Record<string, boolean>>({});

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

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2" style={{ borderColor: BRAND_BLUE }}></div>
      </div>
    );
  }

  return (
    <div>
      {/* Page Header */}
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Pipelines</h1>
          <p className="text-sm text-gray-500 mt-1">Manage and monitor all data pipelines</p>
        </div>
        <button
          onClick={fetchData}
          className="flex items-center gap-2 px-4 py-2 text-white rounded-lg hover:opacity-90 transition-opacity"
          style={{ backgroundColor: BRAND_BLUE }}
        >
          <FiRefreshCw className="w-4 h-4" />
          Refresh
        </button>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <p className="text-sm text-gray-500">Total Pipelines</p>
          <p className="text-2xl font-bold" style={{ color: BRAND_BLUE }}>{pipelines.length}</p>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <p className="text-sm text-gray-500">Running</p>
          <p className="text-2xl font-bold text-green-600">{runningPipelines.length}</p>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <p className="text-sm text-gray-500">Idle</p>
          <p className="text-2xl font-bold text-gray-900">
            {pipelines.filter(p => p.lastRunStatus === 'idle').length}
          </p>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <p className="text-sm text-gray-500">Failed</p>
          <p className="text-2xl font-bold text-red-600">
            {pipelines.filter(p => p.lastRunStatus === 'failed').length}
          </p>
        </div>
      </div>

      {/* Pipelines Table */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-100 flex items-center justify-between">
          <h2 className="text-lg font-semibold text-gray-900">All Pipelines</h2>
          <span className="text-sm text-gray-500">{pipelines.length} pipelines</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                  Pipeline
                </th>
                <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                  Last Run
                </th>
                <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                  Records
                </th>
                <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                  Last Run
                </th>
                <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                  Mode
                </th>
                <th className="px-6 py-3 text-right text-xs font-semibold text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {pipelines.map((pipeline) => {
                const isRunning = runningPipelines.includes(pipeline.pipelineName);
                return (
                  <tr key={pipeline.pipelineName} className="hover:bg-gray-50 transition-colors">
                    <td className="px-6 py-4">
                      <Link
                        to={`/pipeline/${pipeline.pipelineName}`}
                        className="font-medium hover:opacity-80 flex items-center gap-2"
                        style={{ color: BRAND_BLUE }}
                      >
                        {pipeline.pipelineName}
                        <FiExternalLink className="w-4 h-4 text-gray-400" />
                      </Link>
                    </td>
                    <td className="px-6 py-4">
                      <StatusBadge status={isRunning ? 'running' : pipeline.lastRunStatus} />
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-600">
                      {pipeline.lastRun
                        ? format(new Date(pipeline.lastRun), 'MMM d, yyyy HH:mm')
                        : 'Never'}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-600">
                      {pipeline.recordsProcessed?.toLocaleString() || '0'}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-600">
                      {pipeline.lastRun
                        ? format(new Date(pipeline.lastRun), 'MMM d, yyyy HH:mm')
                        : 'Never'}
                    </td>
                    <td className="px-6 py-4">
                      <label className="flex items-center gap-2 cursor-pointer">
                        <input
                          type="checkbox"
                          checked={fullLoadMode[pipeline.pipelineName] || false}
                          onChange={() => toggleFullLoad(pipeline.pipelineName)}
                          className="w-4 h-4 rounded border-gray-300"
                          style={{ accentColor: BRAND_ORANGE }}
                        />
                        <span className="text-sm text-gray-600">Full Load</span>
                      </label>
                    </td>
                    <td className="px-6 py-4">
                      <div className="flex items-center justify-end gap-2">
                        {isRunning ? (
                          <button
                            onClick={() => handleStop(pipeline.pipelineName)}
                            className="p-2 text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                            title="Stop"
                          >
                            <FiSettings className="w-4 h-4" />
                          </button>
                        ) : (
                          <button
                            onClick={() =>
                              handleRun(pipeline.pipelineName, fullLoadMode[pipeline.pipelineName] || false)
                            }
                            className="p-2 rounded-lg transition-colors"
                            style={{ color: BRAND_ORANGE, backgroundColor: `${BRAND_ORANGE}15` }}
                            title="Run"
                          >
                            <FiPlay className="w-4 h-4" />
                          </button>
                        )}
                        <button
                          onClick={() => handleClearQueue(pipeline.pipelineName)}
                          className="p-2 text-gray-600 hover:bg-gray-100 rounded-lg transition-colors"
                          title="Clear Queue"
                        >
                          <FiTrash2 className="w-4 h-4" />
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Quick Stats Footer */}
      <div className="mt-8 grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <h3 className="text-sm font-semibold text-gray-500 uppercase mb-4">Total Records Processed</h3>
          <p className="text-3xl font-bold" style={{ color: BRAND_BLUE }}>
            {pipelines.reduce((sum, p) => sum + (Number(p.recordsProcessed) || 0), 0).toLocaleString()}
          </p>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <h3 className="text-sm font-semibold text-gray-500 uppercase mb-4">Active Rate</h3>
          <p className="text-3xl font-bold text-green-600">
            {pipelines.length > 0
              ? Math.round((pipelines.filter(p => p.lastRunStatus === 'running').length / pipelines.length) * 100)
              : 0}%
          </p>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <h3 className="text-sm font-semibold text-gray-500 uppercase mb-4">Active Pipelines</h3>
          <p className="text-3xl font-bold" style={{ color: BRAND_ORANGE }}>
            {runningPipelines.length}
          </p>
        </div>
      </div>
    </div>
  );
}