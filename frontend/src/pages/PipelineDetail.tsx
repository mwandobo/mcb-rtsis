import { useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { FiPlay, FiSquare, FiTrash2, FiRefreshCw, FiSettings, FiInfo, FiChevronRight } from 'react-icons/fi';
import { format } from 'date-fns';
import { pipelinesApi, statsApi } from '../services/api';
import { StatusBadge } from '../components/StatusBadge';
import type { PipelineState, PipelineHistory, PipelineConfig } from '../types';

const BRAND_BLUE = '#1B3E74';
const BRAND_ORANGE = '#DA7F3D';

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
      <div className="flex items-center justify-center min-h-[60vh]">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2" style={{ borderColor: BRAND_BLUE }}></div>
      </div>
    );
  }

  if (!pipeline) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <div className="text-center">
          <h2 className="text-xl font-semibold text-gray-900">Pipeline not found</h2>
          <Link to="/" className="hover:underline mt-2 inline-block" style={{ color: BRAND_ORANGE }}>
            Back to Dashboard
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div>
      {/* Breadcrumb */}
      <nav className="mb-4 flex items-center gap-2 text-sm">
        <Link to="/" className="text-gray-500 hover:text-gray-700">Dashboard</Link>
        <FiChevronRight className="w-4 h-4 text-gray-400" />
        <Link to="/pipelines" className="text-gray-500 hover:text-gray-700">Pipelines</Link>
        <FiChevronRight className="w-4 h-4 text-gray-400" />
        <span className="text-gray-900 font-medium">{pipeline.pipelineName}</span>
      </nav>

      {/* Page Header */}
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">{pipeline.pipelineName}</h1>
          <p className="text-sm text-gray-500 mt-1">Pipeline Details</p>
        </div>
        <div className="flex items-center gap-3">
          <StatusBadge status={isRunning ? 'running' : pipeline.lastRunStatus} />
        </div>
      </div>

      {/* Info Banner - Streaming Pipeline Explanation */}
      <div className="mb-6 bg-blue-50 border border-blue-200 rounded-xl p-4">
        <div className="flex items-start gap-3">
          <FiInfo className="w-5 h-5 text-blue-600 mt-0.5" />
          <div>
            <h3 className="font-semibold text-blue-900">About Streaming Pipelines</h3>
            <p className="text-sm text-blue-800 mt-1">
              This is a <strong>streaming/incremental pipeline</strong> that runs continuously. It monitors the source table (GLI_TRX_EXTRACT) for new records using the TMSTAMP field and processes only new data since the last run. The pipeline will keep running until manually stopped. Use "Full Load" to reprocess all data from scratch.
            </p>
          </div>
        </div>
      </div>

      {/* Action Message */}
      {actionMessage && (
        <div className="mb-6 bg-blue-50 border border-blue-200 rounded-lg p-4">
          <p className="text-blue-800">{actionMessage}</p>
        </div>
      )}

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <p className="text-sm text-gray-500">Status</p>
          <p className="text-2xl font-bold text-gray-900 mt-1">
            {isRunning ? 'Running' : (pipeline.lastRunStatus === 'completed' ? 'Idle' : pipeline.lastRunStatus || 'Unknown')}
          </p>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <p className="text-sm text-gray-500">Last Run</p>
          <p className="text-2xl font-bold text-gray-900 mt-1">
            {pipeline.lastRun
              ? format(new Date(pipeline.lastRun), 'MMM d, HH:mm')
              : 'Never'}
          </p>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <p className="text-sm text-gray-500">Records Processed</p>
          <p className="text-2xl font-bold text-gray-900 mt-1">
            {(pipeline.recordsProcessed || 0).toLocaleString()}
          </p>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <p className="text-sm text-gray-500">Last Run</p>
          <p className="text-2xl font-bold text-gray-900 mt-1">
            {pipeline.lastRun
              ? format(new Date(pipeline.lastRun), 'MMM d, HH:mm')
              : 'Never'}
          </p>
        </div>
      </div>

      {/* Actions */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6 mb-8">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Actions</h2>
        <div className="flex flex-wrap gap-4">
          {isRunning ? (
            <button
              onClick={handleStop}
              className="flex items-center gap-2 px-4 py-2 text-white rounded-lg hover:opacity-90 transition-opacity"
              style={{ backgroundColor: '#ef4444' }}
            >
              <FiSquare className="w-4 h-4" />
              Stop Pipeline
            </button>
          ) : (
            <button
              onClick={handleRun}
              className="flex items-center gap-2 px-4 py-2 text-white rounded-lg hover:opacity-90 transition-opacity"
              style={{ backgroundColor: BRAND_ORANGE }}
            >
              <FiPlay className="w-4 h-4" />
              Run Pipeline
            </button>
          )}
          <button
            onClick={handleClearQueue}
            className="flex items-center gap-2 px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors"
          >
            <FiTrash2 className="w-4 h-4" />
            Clear Queue
          </button>
          <button
            onClick={fetchData}
            className="flex items-center gap-2 px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors"
          >
            <FiRefreshCw className="w-4 h-4" />
            Refresh
          </button>
          <label className="flex items-center gap-2 px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors cursor-pointer">
            <input
              type="checkbox"
              checked={fullLoad}
              onChange={(e) => setFullLoad(e.target.checked)}
              className="w-4 h-4 rounded border-gray-300"
              style={{ accentColor: BRAND_ORANGE }}
            />
            Full Load
          </label>
        </div>
      </div>

      {/* Configuration */}
      {config && (
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6 mb-8">
          <h2 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <FiSettings className="w-5 h-5" />
            Configuration
          </h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <p className="text-sm text-gray-500">Mode</p>
              <p className="font-medium text-gray-900">{config.mode}</p>
            </div>
            <div>
              <p className="text-sm text-gray-500">Schedule</p>
              <p className="font-medium text-gray-900">{config.schedule}</p>
            </div>
            <div>
              <p className="text-sm text-gray-500">Source</p>
              <p className="font-medium text-gray-900">{config.source}</p>
            </div>
            <div>
              <p className="text-sm text-gray-500">Queue</p>
              <p className="font-medium text-gray-900">{config.queue}</p>
            </div>
          </div>
        </div>
      )}

      {/* Run History */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Run History</h2>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-500 uppercase">Run Time</th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-500 uppercase">Status</th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-500 uppercase">Records</th>
                <th className="px-4 py-3 text-left text-xs font-semibold text-gray-500 uppercase">Error</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {history.map((run, index) => (
                <tr key={index} className="hover:bg-gray-50">
                  <td className="px-4 py-3 text-sm text-gray-900">
                    {run.runTime ? format(new Date(run.runTime), 'MMM d, yyyy HH:mm:ss') : '-'}
                  </td>
                  <td className="px-4 py-3">
                    <StatusBadge status={run.status} size="sm" />
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-600">{run.records.toLocaleString()}</td>
                  <td className="px-4 py-3 text-sm text-red-600 max-w-xs truncate">
                    {run.errorMessage || '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}