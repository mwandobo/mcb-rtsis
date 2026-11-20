import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import Chart from 'react-apexcharts';
import { FiActivity, FiServer, FiAlertTriangle, FiClock, FiRefreshCw, FiExternalLink } from 'react-icons/fi';
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
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', minHeight: '60vh' }}>
        <div className="spinner" />
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', minHeight: '60vh' }}>
        <div className="card" style={{ padding: 32, textAlign: 'center' }}>
          <FiAlertTriangle width={40} height={40} style={{ color: 'var(--red)', margin: '0 auto 14px' }} />
          <div style={{ fontSize: 16, fontWeight: 600, marginBottom: 6 }}>Error Loading Dashboard</div>
          <div style={{ color: 'var(--text-secondary)' }}>{error}</div>
          <button className="btn-primary" onClick={fetchData} style={{ margin: '16px auto 0', justifyContent: 'center' }}>
            Retry
          </button>
        </div>
      </div>
    );
  }

  const barChartOptions = {
    chart: { type: 'bar' as const, fontFamily: 'inherit', toolbar: { show: false }, animations: { enabled: true, easing: 'easeinout' as const, speed: 800 } },
    plotOptions: { bar: { borderRadius: 4, columnWidth: '60%' } },
    dataLabels: { enabled: false },
    xaxis: { categories: recordsData.map(d => new Date(d.timestamp).toLocaleDateString('en-US', { weekday: 'short' })), labels: { style: { colors: '#9a9ca6' } } },
    yaxis: { labels: { style: { colors: '#9a9ca6' } } },
    grid: { borderColor: '#eeeff2', strokeDashArray: 4 },
    colors: [BRAND_BLUE],
    tooltip: { theme: 'light', y: { formatter: (val: number) => val.toLocaleString() } },
  };

  const lineChartOptions = {
    chart: { type: 'area' as const, fontFamily: 'inherit', toolbar: { show: false }, animations: { enabled: true, easing: 'easeinout' as const, speed: 800 } },
    stroke: { curve: 'smooth' as const, width: 3 },
    fill: { type: 'gradient' as const, gradient: { shadeIntensity: 1, opacityFrom: 0.4, opacityTo: 0.1, stops: [0, 100] } },
    dataLabels: { enabled: false },
    xaxis: { categories: recordsData.map(d => new Date(d.timestamp).toLocaleDateString('en-US', { weekday: 'short' })), labels: { style: { colors: '#9a9ca6' } } },
    yaxis: { labels: { style: { colors: '#9a9ca6' } } },
    grid: { borderColor: '#eeeff2', strokeDashArray: 4 },
    colors: [BRAND_ORANGE],
    tooltip: { theme: 'light', y: { formatter: (val: number) => val.toString() } },
  };

  const pieChartOptions = {
    chart: { type: 'donut' as const, fontFamily: 'inherit' },
    labels: Object.keys(stats?.pipelinesByStatus || {}).map(s => (s === 'completed' ? 'Idle' : s.charAt(0).toUpperCase() + s.slice(1))),
    colors: ['#16a34a', BRAND_BLUE, '#dc2626', '#9a9ca6'],
    legend: { position: 'bottom' as const, fontSize: '13px' },
    plotOptions: { pie: { donut: { size: '70%', labels: { show: true, total: { show: true, label: 'Total', fontSize: '14px', fontWeight: 600, formatter: () => stats?.totalPipelines?.toString() || '0' } } } } },
    dataLabels: { enabled: false },
    tooltip: { theme: 'light' },
  };

  const recentPipelines = pipelines.slice(0, 5);

  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title">Pipeline Dashboard</div>
          <div className="page-sub">Monitor and manage your streaming data pipelines</div>
        </div>
        <button className="btn-primary" onClick={fetchData}>
          <FiRefreshCw width={14} height={14} />
          Refresh
        </button>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-top"><div className="stat-icon blue"><FiServer width={16} height={16} /></div></div>
          <div className="stat-value">{stats?.totalPipelines || 0}</div>
          <div className="stat-label">Total Pipelines</div>
        </div>
        <div className="stat-card">
          <div className="stat-top"><div className="stat-icon green"><FiActivity width={16} height={16} /></div></div>
          <div className="stat-value" style={{ color: 'var(--green)' }}>{stats?.runningPipelines || 0}</div>
          <div className="stat-label">Currently Running</div>
        </div>
        <div className="stat-card">
          <div className="stat-top"><div className="stat-icon red"><FiAlertTriangle width={16} height={16} /></div></div>
          <div className="stat-value" style={{ color: 'var(--red)' }}>{stats?.failedToday || 0}</div>
          <div className="stat-label">Failed Today</div>
        </div>
        <div className="stat-card">
          <div className="stat-top"><div className="stat-icon orange"><FiClock width={16} height={16} /></div></div>
          <div className="stat-value">{(stats?.totalRecordsToday || 0).toLocaleString()}</div>
          <div className="stat-label">Records Processed Today</div>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 16, marginBottom: 16 }}>
        <div className="card" style={{ padding: 18 }}>
          <div className="card-title" style={{ marginBottom: 14 }}>Records Processed (7 Days)</div>
          <Chart options={barChartOptions} series={[{ name: 'Records', data: recordsData.map(d => d.records) }]} type="bar" height={280} />
        </div>
        <div className="card" style={{ padding: 18 }}>
          <div className="card-title" style={{ marginBottom: 14 }}>Pipeline Status</div>
          <Chart options={pieChartOptions} series={Object.values(stats?.pipelinesByStatus || {}).map(Number)} type="donut" height={280} />
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
        <div className="card" style={{ padding: 18 }}>
          <div className="card-title" style={{ marginBottom: 14 }}>Pipeline Runs Trend (7 Days)</div>
          <Chart options={lineChartOptions} series={[{ name: 'Pipelines', data: recordsData.map(d => d.pipelines) }]} type="area" height={280} />
        </div>
        <div className="card" style={{ padding: 18 }}>
          <div className="card-title" style={{ marginBottom: 14 }}>Records Trend (7 Days)</div>
          <Chart
            options={{ ...lineChartOptions, colors: [BRAND_BLUE], fill: { type: 'gradient' as const, gradient: { shadeIntensity: 1, opacityFrom: 0.5, opacityTo: 0.1, stops: [0, 100] } } }}
            series={[{ name: 'Records', data: recordsData.map(d => d.records) }]}
            type="area"
            height={280}
          />
        </div>
      </div>

      <div className="card-header" style={{ padding: 0, marginBottom: 12, border: 'none' }}>
        <div className="card-title" style={{ fontSize: 16 }}>Recent Pipelines</div>
        <Link to="/pipelines" style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 13, fontWeight: 500, color: 'var(--accent-2)', textDecoration: 'none' }}>
          View All <FiExternalLink width={14} height={14} />
        </Link>
      </div>

      <div className="card" style={{ marginBottom: 20 }}>
        <table className="data-table">
          <thead>
            <tr>
              <th>Pipeline</th>
              <th>Status</th>
              <th>Last Run</th>
              <th>Records</th>
              <th style={{ textAlign: 'right' }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {recentPipelines.map((pipeline) => {
              const isRunning = runningPipelines.includes(pipeline.pipelineName);
              return (
                <tr key={pipeline.pipelineName}>
                  <td>
                    <Link to={`/pipeline/${pipeline.pipelineName}`} style={{ color: 'var(--accent)', fontWeight: 500, display: 'flex', alignItems: 'center', gap: 6, textDecoration: 'none' }}>
                      {pipeline.pipelineName}
                      <FiExternalLink width={12} height={12} style={{ color: 'var(--text-muted)' }} />
                    </Link>
                  </td>
                  <td><StatusBadge status={isRunning ? 'running' : pipeline.lastRunStatus} /></td>
                  <td style={{ color: 'var(--text-secondary)' }}>{pipeline.lastRun ? format(new Date(pipeline.lastRun), 'MMM d, yyyy HH:mm') : 'Never'}</td>
                  <td style={{ color: 'var(--text-secondary)' }}>{pipeline.recordsProcessed?.toLocaleString() || '0'}</td>
                  <td style={{ textAlign: 'right' }}>
                    <Link to={`/pipeline/${pipeline.pipelineName}`} style={{ fontSize: 13, fontWeight: 500, color: 'var(--accent-2)', textDecoration: 'none' }}>
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
  );
}
