import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import Chart from 'react-apexcharts';
import { FiActivity, FiServer, FiAlertTriangle, FiClock, FiRefreshCw, FiExternalLink, FiChevronRight } from 'react-icons/fi';
import { pipelinesApi, statsApi } from '../services/api';
import { StatusBadge } from '../components/StatusBadge';
import type { PipelineState, DashboardStats, TimeSeriesData } from '../types';
import { format } from 'date-fns';

const BRAND_BLUE = '#1B3E74';
const BRAND_ORANGE = '#DA7F3D';

export function Dashboard() {
  const [pipelines, setPipelines] = useState<PipelineState[]>([]);
  const [runningPipelines, setRunningPipelines] = useState<string[]>([]);
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [recordsData, setRecordsData] = useState<TimeSeriesData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = async () => {
    try {
      const [pipelinesData, runningData, statsData, records] = await Promise.all([
        pipelinesApi.getAll(),
        pipelinesApi.getRunning(),
        statsApi.getDashboard(),
        statsApi.getRecordsOverTime(7),
      ]);
      setPipelines(pipelinesData);
      setRunningPipelines(runningData);
      setStats(statsData);
      setRecordsData(records);
      setError(null);
    } catch (err) {
      setError('Failed to load dashboard data');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2" style={{ borderColor: BRAND_BLUE }}></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <div className="bg-white rounded-xl shadow-sm p-8 text-center">
          <FiAlertTriangle className="w-12 h-12 text-red-500 mx-auto mb-4" />
          <h2 className="text-xl font-semibold text-gray-900 mb-2">Error Loading Dashboard</h2>
          <p className="text-gray-600">{error}</p>
          <button
            onClick={fetchData}
            className="mt-4 px-4 py-2 text-white rounded-lg hover:opacity-90 transition-opacity"
            style={{ backgroundColor: BRAND_BLUE }}
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  const barChartOptions = {
    chart: {
      type: 'bar' as const,
      fontFamily: 'inherit',
      toolbar: { show: false },
      animations: {
        enabled: true,
        easing: 'easeinout' as const,
        speed: 800,
      },
    },
    plotOptions: {
      bar: {
        borderRadius: 4,
        columnWidth: '60%',
      },
    },
    dataLabels: { enabled: false },
    xaxis: {
      categories: recordsData.map(d => new Date(d.timestamp).toLocaleDateString('en-US', { weekday: 'short' })),
      labels: { style: { colors: '#6b7280' } },
    },
    yaxis: {
      labels: { style: { colors: '#6b7280' } },
    },
    grid: {
      borderColor: '#f3f4f6',
      strokeDashArray: 4,
    },
    colors: [BRAND_BLUE],
    tooltip: {
      theme: 'light',
      y: { formatter: (val: number) => val.toLocaleString() },
    },
  };

  const lineChartOptions = {
    chart: {
      type: 'area' as const,
      fontFamily: 'inherit',
      toolbar: { show: false },
      animations: {
        enabled: true,
        easing: 'easeinout' as const,
        speed: 800,
      },
    },
    stroke: {
      curve: 'smooth' as const,
      width: 3,
    },
    fill: {
      type: 'gradient' as const,
      gradient: {
        shadeIntensity: 1,
        opacityFrom: 0.4,
        opacityTo: 0.1,
        stops: [0, 100],
      },
    },
    dataLabels: { enabled: false },
    xaxis: {
      categories: recordsData.map(d => new Date(d.timestamp).toLocaleDateString('en-US', { weekday: 'short' })),
      labels: { style: { colors: '#6b7280' } },
    },
    yaxis: {
      labels: { style: { colors: '#6b7280' } },
    },
    grid: {
      borderColor: '#f3f4f6',
      strokeDashArray: 4,
    },
    colors: [BRAND_ORANGE],
    tooltip: {
      theme: 'light',
      y: { formatter: (val: number) => val.toString() },
    },
  };

  const pieChartOptions = {
    chart: { type: 'donut' as const, fontFamily: 'inherit' },
    labels: Object.keys(stats?.pipelinesByStatus || {}).map(s => {
      if (s === 'completed') return 'Idle';
      return s.charAt(0).toUpperCase() + s.slice(1);
    }),
    colors: ['#10b981', '#1B3E74', '#ef4444', '#6b7280'],
    legend: {
      position: 'bottom' as const,
      fontSize: '13px',
    },
    plotOptions: {
      pie: {
        donut: {
          size: '70%',
          labels: {
            show: true,
            total: {
              show: true,
              label: 'Total',
              fontSize: '14px',
              fontWeight: 600,
              formatter: () => stats?.totalPipelines?.toString() || '0',
            },
          },
        },
      },
    },
    dataLabels: { enabled: false },
    tooltip: {
      theme: 'light',
    },
  };

  const recentPipelines = pipelines.slice(0, 5);

  return (
    <div>
      {/* Breadcrumb */}
      <nav className="mb-4 flex items-center gap-2 text-sm">
        <Link to="/" className="text-gray-500 hover:text-gray-700">Dashboard</Link>
        <FiChevronRight className="w-4 h-4 text-gray-400" />
        <span className="text-gray-900 font-medium">Overview</span>
      </nav>

      {/* Page Header */}
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Pipeline Dashboard</h1>
          <p className="text-sm text-gray-500 mt-1">Monitor and manage your streaming data pipelines</p>
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

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-500">Total Pipelines</p>
              <p className="text-3xl font-bold text-gray-900 mt-1">{stats?.totalPipelines || 0}</p>
            </div>
            <div className="p-3 rounded-xl bg-blue-50 text-blue-600">
              <FiServer className="w-6 h-6" />
            </div>
          </div>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-500">Currently Running</p>
              <p className="text-3xl font-bold text-green-600 mt-1">{stats?.runningPipelines || 0}</p>
              <p className="text-xs text-gray-400 mt-1">Active streams</p>
            </div>
            <div className="p-3 rounded-xl bg-green-50 text-green-600">
              <FiActivity className="w-6 h-6" />
            </div>
          </div>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-500">Failed Today</p>
              <p className="text-3xl font-bold text-red-600 mt-1">{stats?.failedToday || 0}</p>
            </div>
            <div className="p-3 rounded-xl bg-red-50 text-red-600">
              <FiAlertTriangle className="w-6 h-6" />
            </div>
          </div>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-500">Total Records</p>
              <p className="text-3xl font-bold text-gray-900 mt-1">
                {(stats?.totalRecordsToday || 0).toLocaleString()}
              </p>
              <p className="text-xs text-gray-400 mt-1">Processed today</p>
            </div>
            <div className="p-3 rounded-xl bg-orange-50 text-orange-600">
              <FiClock className="w-6 h-6" />
            </div>
          </div>
        </div>
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
        <div className="lg:col-span-2 bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Records Processed (7 Days)</h2>
          <Chart
            options={barChartOptions}
            series={[{ name: 'Records', data: recordsData.map(d => d.records) }]}
            type="bar"
            height={280}
          />
        </div>

        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Pipeline Status</h2>
          <Chart
            options={pieChartOptions}
            series={Object.values(stats?.pipelinesByStatus || {}).map(Number)}
            type="donut"
            height={280}
          />
        </div>
      </div>

      {/* Area Chart */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Pipeline Runs Trend (7 Days)</h2>
          <Chart
            options={lineChartOptions}
            series={[{ name: 'Pipelines', data: recordsData.map(d => d.pipelines) }]}
            type="area"
            height={280}
          />
        </div>

        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Records Trend (7 Days)</h2>
          <Chart
            options={{
              ...lineChartOptions,
              colors: [BRAND_BLUE],
              fill: {
                type: 'gradient' as const,
                gradient: {
                  shadeIntensity: 1,
                  opacityFrom: 0.5,
                  opacityTo: 0.1,
                  stops: [0, 100],
                },
              },
            }}
            series={[{ name: 'Records', data: recordsData.map(d => d.records) }]}
            type="area"
            height={280}
          />
        </div>
      </div>

      {/* Recent Pipelines */}
      <div className="mb-6 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900">Recent Pipelines</h2>
        <Link
          to="/pipelines"
          className="flex items-center gap-1 text-sm font-medium hover:opacity-80 transition-opacity"
          style={{ color: BRAND_ORANGE }}
        >
          View All <FiExternalLink className="w-4 h-4" />
        </Link>
      </div>

      <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden mb-8">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50 border-b border-gray-100">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">Pipeline</th>
                <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">Status</th>
                <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">Last Run</th>
                <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider">Records</th>
                <th className="px-6 py-3 text-right text-xs font-semibold text-gray-500 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {recentPipelines.map((pipeline) => {
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
                    <td className="px-6 py-4 text-right">
                      <Link
                        to={`/pipeline/${pipeline.pipelineName}`}
                        className="text-sm font-medium hover:opacity-80"
                        style={{ color: BRAND_ORANGE }}
                      >
                        View Details
                      </Link>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Total Records Today */}
      <div
        className="rounded-xl p-6 text-white"
        style={{ background: `linear-gradient(135deg, ${BRAND_BLUE} 0%, ${BRAND_ORANGE} 100%)` }}
      >
        <div className="flex items-center justify-between">
          <div>
            <p className="text-white/80 text-sm">Total Records Processed Today</p>
            <p className="text-4xl font-bold mt-1">
              {(stats?.totalRecordsToday || 0).toLocaleString()}
            </p>
          </div>
          <FiClock className="w-16 h-16 text-white/30" />
        </div>
      </div>
    </div>
  );
}