import { useEffect, useState } from 'react';
import { FiDatabase, FiTable, FiSearch, FiRefreshCw, FiChevronRight } from 'react-icons/fi';
import api from '../services/api';

interface TableInfo {
  name: string;
  schema: string;
  recordCount: number;
  remarks?: string;
}

interface TableData {
  data: any[];
  total: number;
  columns: string[];
}

export function DB2Source() {
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [tableData, setTableData] = useState<TableData | null>(null);
  const [tableLoading, setTableLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [offset, setOffset] = useState(0);
  const limit = 10;

  const [error, setError] = useState<string | null>(null);
  const [cacheInfo, setCacheInfo] = useState<{ cached: boolean; age?: number; ttl: number } | null>(null);

  const fetchTables = async (forceRefresh = false) => {
    try {
      setError(null);

      if (forceRefresh) {
        await api.delete('/databases/db2/cache');
      }

      const response = await api.get('/databases/db2/tables');
      const data = response.data;
      setTables(Array.isArray(data) ? data : []);

      const cacheResponse = await api.get('/databases/db2/cache-info');
      setCacheInfo(cacheResponse.data);
    } catch (err: any) {
      console.error('Failed to load DB2 tables:', err);

      if (err.code === 'ECONNABORTED' || err.message?.includes('timeout')) {
        setError('DB2 query is taking longer than expected. The backend is still processing - please wait and try refreshing in a moment.');
      } else {
        setError(err.response?.data?.message || err.message || 'Failed to connect to DB2');
      }
      setTables([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTables();
  }, []);

  const fetchTableData = async (tableName: string) => {
    setSelectedTable(tableName);
    setTableLoading(true);
    setOffset(0);

    try {
      const response = await api.get(`/databases/db2/table/${tableName}`, { params: { limit, offset: 0 } });
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
      const response = await api.get(`/databases/db2/table/${selectedTable}`, { params: { limit, offset: newOffset } });
      setTableData({ ...response.data, data: [...tableData.data, ...response.data.data] });
    } catch (err) {
      console.error('Failed to load more data:', err);
    }
  };

  const filteredTables = tables.filter(t => t.name.toLowerCase().includes(searchTerm.toLowerCase()));
  const totalRecords = tables.reduce((sum, t) => sum + t.recordCount, 0);

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
            <FiDatabase width={22} height={22} style={{ color: 'var(--accent)' }} />
            DB2 Source Database
          </div>
          <div className="page-sub">
            {tables.length} tables &middot; {totalRecords.toLocaleString()} total records
            {cacheInfo?.cached && <span style={{ color: 'var(--accent)' }}> &middot; Cached {Math.floor((cacheInfo.age || 0) / 1000)}s ago</span>}
          </div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button className="btn-primary" onClick={() => fetchTables(false)}>
            <FiRefreshCw width={14} height={14} />
            Refresh
          </button>
          {cacheInfo?.cached && (
            <button className="btn-secondary" onClick={() => fetchTables(true)} title="Force refresh from DB2 (clears cache)">
              <FiRefreshCw width={14} height={14} />
              Force Refresh
            </button>
          )}
        </div>
      </div>

      <div className="search-box" style={{ width: '100%', marginBottom: 20 }}>
        <FiSearch width={14} height={14} />
        <input type="text" placeholder="Search tables…" value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} />
      </div>

      {error && (
        <div style={{ background: 'var(--red-dim)', border: '1px solid var(--red)', borderRadius: 10, padding: 14, marginBottom: 20 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, color: 'var(--red)', fontWeight: 600 }}>
            <FiDatabase width={16} height={16} />
            Connection Error
          </div>
          <div style={{ marginTop: 6, fontSize: 13, color: 'var(--red)' }}>{error}</div>
          <div style={{ marginTop: 6, fontSize: 11, color: 'var(--red)', opacity: 0.8 }}>Check your DB2 connection settings in the backend .env file.</div>
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: 16 }}>
        <div className="card">
          <div className="card-header">
            <div className="card-title">Tables</div>
            <span className="pagination-info">{filteredTables.length}</span>
          </div>
          <div style={{ maxHeight: 600, overflowY: 'auto' }}>
            {filteredTables.map((table) => (
              <button
                key={table.name}
                onClick={() => fetchTableData(table.name)}
                style={{
                  width: '100%', padding: '11px 16px', display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                  background: selectedTable === table.name ? 'var(--accent-dim)' : 'none', border: 'none', borderBottom: '1px solid var(--border-subtle)',
                  cursor: 'pointer', textAlign: 'left', fontFamily: 'inherit',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <FiTable width={15} height={15} style={{ color: 'var(--text-muted)' }} />
                  <div>
                    <div style={{ fontWeight: 500, fontSize: 13 }}>{table.name}</div>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{table.recordCount.toLocaleString()} records</div>
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
              <FiDatabase width={36} height={36} style={{ color: 'var(--border)', margin: '0 auto 10px' }} />
              <div style={{ color: 'var(--text-muted)' }}>Select a table to view its data</div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
