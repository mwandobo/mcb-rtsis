import { useState } from 'react';
import { Link } from 'react-router-dom';
import { PipelineState } from '../types';
import { StatusBadge } from './StatusBadge';
import { format } from 'date-fns';
import { FiPlay, FiSettings, FiTrash2, FiExternalLink, FiAlertCircle } from 'react-icons/fi';

interface PipelineTableProps {
  pipelines: PipelineState[];
  runningPipelines: string[];
  onRun: (name: string, fullLoad: boolean) => void;
  onStop: (name: string) => void;
  onClearQueue: (name: string) => void;
}

const BRAND_BLUE = '#1B3E74';
const BRAND_ORANGE = '#DA7F3D';

export function PipelineTable({ pipelines, runningPipelines, onRun, onStop, onClearQueue }: PipelineTableProps) {
  const [fullLoadMode, setFullLoadMode] = useState<Record<string, boolean>>({});
  const [error, setError] = useState<string | null>(null);
  const [processing, setProcessing] = useState<string | null>(null);

  const toggleFullLoad = (name: string) => {
    setFullLoadMode((prev) => ({ ...prev, [name]: !prev[name] }));
  };

  const handleRun = async (name: string, fullLoad: boolean) => {
    setProcessing(name);
    setError(null);
    try {
      await onRun(name, fullLoad);
    } catch (err: any) {
      setError(err.message || 'Failed to run pipeline');
    } finally {
      setProcessing(null);
    }
  };

  const handleStop = async (name: string) => {
    setProcessing(name);
    setError(null);
    try {
      await onStop(name);
    } catch (err: any) {
      setError(err.message || 'Failed to stop pipeline');
    } finally {
      setProcessing(null);
    }
  };

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
      {error && (
        <div className="px-6 py-3 bg-red-50 border-b border-red-100 flex items-center gap-2 text-red-700">
          <FiAlertCircle className="w-4 h-4" />
          <span className="text-sm">{error}</span>
          <button onClick={() => setError(null)} className="ml-auto text-red-500 hover:text-red-700">
            ×
          </button>
        </div>
      )}
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="bg-gray-50 border-b border-gray-100">
            <tr>
              <th className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                Pipeline
              </th>
              <th className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                Status
              </th>
              <th className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                Last Run
              </th>
              <th className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                Records
              </th>
              <th className="px-6 py-4 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">
                Mode
              </th>
              <th className="px-6 py-4 text-right text-xs font-semibold text-gray-500 uppercase tracking-wider">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {pipelines.map((pipeline) => {
              const isRunning = runningPipelines.includes(pipeline.pipelineName);
              const isProcessing = processing === pipeline.pipelineName;
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
                      {isProcessing ? (
                        <span className="text-sm text-gray-500">Processing...</span>
                      ) : isRunning ? (
                        <button
                          onClick={() => handleStop(pipeline.pipelineName)}
                          className="p-2 text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                          title="Stop"
                        >
                          <FiSettings className="w-4 h-4" />
                        </button>
                      ) : (
                        <button
                          onClick={() => handleRun(pipeline.pipelineName, fullLoadMode[pipeline.pipelineName] || false)}
                          className="p-2 rounded-lg transition-colors"
                          style={{ color: BRAND_ORANGE, backgroundColor: `${BRAND_ORANGE}15` }}
                          title="Run"
                        >
                          <FiPlay className="w-4 h-4" />
                        </button>
                      )}
                      <button
                        onClick={() => onClearQueue(pipeline.pipelineName)}
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
  );
}