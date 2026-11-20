import { useEffect, useState } from 'react';
import { FiServer, FiTable, FiSearch, FiRefreshCw, FiChevronLeft, FiChevronRight, FiActivity } from 'react-icons/fi';
import { format } from 'date-fns';
import api from '../services/api';
import { StatusBadge } from '../components/StatusBadge';

interface TableInfo {
  name: string;
  schema: string;
  recordCount: number;
  columnCount: number;
  remarks?: string;
}

interface PipelineStat {
  name: string;
  lastRun: string;
  lastRunStatus: string;
  recordsProcessed: number;
  lastSuccessfulRun: string;
  errorMessage?: string;
}

interface TableData {
  data: any[];
  total: number;
  columns: string[];
}

export function PostgresTarget() {
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [pipelineStats, setPipelineStats] = useState<PipelineStat[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [tableData, setTableData] = useState<TableData | null>(null);
  const [tableLoading, setTableLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [offset, setOffset] = useState(0);
  const [activeTab, setActiveTab] = useState<'tables' | 'pipelines'>('tables');
  const [pipelinePage, setPipelinePage] = useState(1);
  const limit = 100;
  const PIPELINE_PAGE_SIZE = 10;

  const fetchData = async () => {
    setLoading(true);
    try {
      const [tablesResponse, pipelinesResponse] = await Promise.all([
        api.get('/databases/postgres/tables'),
        api.get('/databases/postgres/pipeline-stats'),
      ]);
      setTables(Array.isArray(tablesResponse.data) ? tablesResponse.data : []);
      setPipelineStats(Array.isArray(pipelinesResponse.data) ? pipelinesResponse.data : []);
    } catch (err) {
      console.error('Failed to load PostgreSQL data:', err);
      setTables([]);
      setPipelineStats([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const fetchTableData = async (tableName: string) => {
    setSelectedTable(tableName);
    setTableLoading(true);
    setOffset(0);

    try {
      const response = await api.get(`/databases/postgres/table/${tableName}`, { params: { limit, offset: 0 } });
      setTableData(response.data);
    } catch (err) {
      console.error('Failed to load table data:', err);
      setTableData({ data: [], total: 0, columns: [] });
    } finally {
      setTableLoading(false);
    }
  };

  const loadMore = async () => {
    if (!selectedTable || !tableData) return;

    const newOffset = offset + limit;
    setOffset(newOffset);

    try {
      const response = await api.get(`/databases/postgres/table/${selectedTable}`, { params: { limit, offset: newOffset } });
      setTableData({ ...response.data, data: [...tableData.data, ...response.data.data] });
    } catch (err) {
      console.error('Failed to load more data:', err);
    }
  };

  const filteredTables = tables.filter(t => t.name.toLowerCase().includes(searchTerm.toLowerCase()));
  const totalRecords = tables.reduce((sum, t) => sum + t.recordCount, 0);
  const totalPipelineRecords = pipelineStats.reduce((sum, p) => sum + p.recordsProcessed, 0);
  const pipelineTotalPages = Math.max(1, Math.ceil(pipelineStats.length / PIPELINE_PAGE_SIZE));
  const pipelineCurrentPage = Math.min(pipelinePage, pipelineTotalPages);
  const paginatedPipelineStats = pipelineStats.slice(
    (pipelineCurrentPage - 1) * PIPELINE_PAGE_SIZE,
    pipelineCurrentPage * PIPELINE_PAGE_SIZE
  );

  if (loading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', minHeight: '60vh' }}>
        <div className="spinner" />
      </div>
    );
  }

  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title" style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <FiServer width={22} height={22} style={{ color: 'var(--accent)' }} />
            PostgreSQL Target Database
          </div>
          <div className="page-sub">
            {tables.length} tables &middot; {totalRecords.toLocaleString()} total records &middot; {pipelineStats.length} pipelines
          </div>
        </div>
        <button className="btn-primary" onClick={fetchData}>
          <FiRefreshCw width={14} height={14} />
          Refresh
        </button>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-value" style={{ color: 'var(--accent)' }}>{tables.length}</div>
          <div className="stat-label">Total Tables</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{totalRecords.toLocaleString()}</div>
          <div className="stat-label">Total Records</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{pipelineStats.length}</div>
          <div className="stat-label">Pipelines</div>
        </div>
        <div className="stat-card">
          <div className="stat-value" style={{ color: 'var(--accent-2)' }}>{totalPipelineRecords.toLocaleString()}</div>
          <div className="stat-label">Pipeline Records</div>
        </div>
      </div>

      <div style={{ display: 'flex', gap: 4, marginBottom: 20, borderBottom: '1px solid var(--border-subtle)' }}>
        <button
          onClick={() => setActiveTab('tables')}
          style={{
            padding: '9px 4px', marginRight: 16, background: 'none', border: 'none', borderBottom: activeTab === 'tables' ? '2px solid var(--accent)' : '2px solid transparent',
            color: activeTab === 'tables' ? 'var(--accent)' : 'var(--text-secondary)', fontWeight: 500, fontSize: 13.5, cursor: 'pointer', fontFamily: 'inherit',
          }}
        >
          Tables
        </button>
        <button
          onClick={() => setActiveTab('pipelines')}
          style={{
            padding: '9px 4px', display: 'flex', alignItems: 'center', gap: 6, background: 'none', border: 'none', borderBottom: activeTab === 'pipelines' ? '2px solid var(--accent)' : '2px solid transparent',
            color: activeTab === 'pipelines' ? 'var(--accent)' : 'var(--text-secondary)', fontWeight: 500, fontSize: 13.5, cursor: 'pointer', fontFamily: 'inherit',
          }}
        >
          <FiActivity width={14} height={14} />
          Pipeline Status
        </button>
      </div>

      {activeTab === 'tables' && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: 16 }}>
          <div className="card">
            <div className="card-header">
              <div className="card-title">Tables</div>
              <span className="pagination-info">{filteredTables.length}</span>
            </div>
            <div style={{ padding: 10 }}>
              <div className="search-box" style={{ width: '100%' }}>
                <FiSearch width={13} height={13} />
                <input type="text" placeholder="Search tables…" value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} />
              </div>
            </div>
            <div style={{ maxHeight: 500, overflowY: 'auto' }}>
              {filteredTables.map((table) => (
                <button
                  key={table.name}
                  onClick={() => fetchTableData(table.name)}
                  style={{
                    width: '100%', padding: '11px 16px', display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                    background: selectedTable === table.name ? 'var(--accent-dim)' : 'none', border: 'none', borderTop: '1px solid var(--border-subtle)',
                    cursor: 'pointer', textAlign: 'left', fontFamily: 'inherit',
                  }}
                >
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                    <FiTable width={15} height={15} style={{ color: 'var(--text-muted)' }} />
                    <div>
                      <div style={{ fontWeight: 500, fontSize: 13 }}>{table.name}</div>
                      <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{table.recordCount.toLocaleString()} records &middot; {table.columnCount} cols</div>
                    </div>
                  </div>
                  <FiChevronRight width={14} height={14} style={{ color: 'var(--text-muted)' }} />
                </button>
              ))}
            </div>
          </div>

          <div>
            {selectedTable ? (
              <div className="card">
                <div className="card-header">
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <FiTable width={15} height={15} style={{ color: 'var(--text-muted)' }} />
                    <div className="card-title">{selectedTable}</div>
                  </div>
                  <span className="pagination-info">
                    {tableData ? `${tableData.data.length} of ${tableData.total.toLocaleString()} records` : 'Loading...'}
                  </span>
                </div>

                {tableLoading ? (
                  <div style={{ padding: 40, display: 'flex', justifyContent: 'center' }}>
                    <div className="spinner" style={{ width: 24, height: 24 }} />
                  </div>
                ) : tableData && tableData.data.length > 0 ? (
                  <>
                    <div style={{ overflowX: 'auto' }}>
                      <table className="data-table">
                        <thead>
                          <tr>{tableData.columns.map((col) => <th key={col}>{col}</th>)}</tr>
                        </thead>
                        <tbody>
                          {tableData.data.map((row, idx) => (
                            <tr key={idx}>
                              {tableData.columns.map((col) => (
                                <td key={col} style={{ maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--text-secondary)' }}>
                                  {row[col] !== null && row[col] !== undefined ? String(row[col]) : '-'}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>

                    {tableData.data.length + offset < tableData.total && (
                      <div style={{ padding: 14, borderTop: '1px solid var(--border-subtle)', display: 'flex', justifyContent: 'center' }}>
                        <button className="btn-accent2" onClick={loadMore}>Load More</button>
                      </div>
                    )}
                  </>
                ) : (
                  <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-muted)' }}>No data available</div>
                )}
              </div>
            ) : (
              <div className="card" style={{ padding: 40, textAlign: 'center' }}>
                <FiServer width={36} height={36} style={{ color: 'var(--border)', margin: '0 auto 10px' }} />
                <div style={{ color: 'var(--text-muted)' }}>Select a table to view its data</div>
              </div>
            )}
          </div>
        </div>
      )}

      {activeTab === 'pipelines' && (
        <div className="card">
          <table className="data-table">
            <thead>
              <tr>
                <th>Pipeline</th>
                <th>Status</th>
                <th>Last Run</th>
                <th>Records Processed</th>
                <th>Last Success</th>
                <th>Error</th>
              </tr>
            </thead>
            <tbody>
              {paginatedPipelineStats.map((pipeline) => (
                <tr key={pipeline.name}>
                  <td style={{ fontWeight: 500 }}>{pipeline.name}</td>
                  <td><StatusBadge status={pipeline.lastRunStatus} size="sm" /></td>
                  <td style={{ color: 'var(--text-secondary)' }}>{pipeline.lastRun ? format(new Date(pipeline.lastRun), 'MMM d, HH:mm') : 'Never'}</td>
                  <td style={{ color: 'var(--text-secondary)' }}>{pipeline.recordsProcessed.toLocaleString()}</td>
                  <td style={{ color: 'var(--text-secondary)' }}>{pipeline.lastSuccessfulRun ? format(new Date(pipeline.lastSuccessfulRun), 'MMM d, HH:mm') : 'Never'}</td>
                  <td style={{ color: 'var(--red)', maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{pipeline.errorMessage || '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>

          <div className="pagination-bar">
            <span className="pagination-info">
              Showing {pipelineStats.length === 0 ? 0 : (pipelineCurrentPage - 1) * PIPELINE_PAGE_SIZE + 1}-
              {Math.min(pipelineCurrentPage * PIPELINE_PAGE_SIZE, pipelineStats.length)} of {pipelineStats.length}
            </span>
            <div className="pagination-controls">
              <button
                onClick={() => setPipelinePage((p) => Math.max(1, p - 1))}
                disabled={pipelineCurrentPage === 1}
                className="icon-btn"
                title="Previous page"
              >
                <FiChevronLeft width={14} height={14} />
              </button>
              <span style={{ fontSize: 12, color: 'var(--text-secondary)', padding: '0 4px' }}>
                Page {pipelineCurrentPage} of {pipelineTotalPages}
              </span>
              <button
                onClick={() => setPipelinePage((p) => Math.min(pipelineTotalPages, p + 1))}
                disabled={pipelineCurrentPage === pipelineTotalPages}
                className="icon-btn"
                title="Next page"
              >
                <FiChevronRight width={14} height={14} />
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
