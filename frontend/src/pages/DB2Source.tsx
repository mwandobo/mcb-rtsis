import { useEffect, useState } from 'react';
import { FiDatabase, FiTable, FiSearch, FiRefreshCw, FiChevronRight } from 'react-icons/fi';
import { format } from 'date-fns';
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

const BRAND_BLUE = '#1B3E74';
const BRAND_ORANGE = '#DA7F3D';

export function DB2Source() {
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [tableData, setTableData] = useState<TableData | null>(null);
  const [tableLoading, setTableLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [offset, setOffset] = useState(0);
  const limit = 100;

  const [error, setError] = useState<string | null>(null);

  const fetchTables = async () => {
    try {
      setError(null);
      const data = await api.get('/databases/db2/tables');
      setTables(Array.isArray(data) ? data : []);
    } catch (err: any) {
      console.error('Failed to load DB2 tables:', err);
      setError(err.response?.data?.message || err.message || 'Failed to connect to DB2');
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
      const data = await api.get(`/databases/db2/table/${tableName}`, { params: { limit, offset: 0 } });
      setTableData(data);
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
      const data = await api.get(`/databases/db2/table/${selectedTable}`, { params: { limit, offset: newOffset } });
      setTableData({
        ...data,
        data: [...tableData.data, ...data.data],
      });
    } catch (err) {
      console.error('Failed to load more data:', err);
    }
  };

  const filteredTables = tables.filter(t => 
    t.name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const totalRecords = tables.reduce((sum, t) => sum + t.recordCount, 0);

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
          <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-3">
            <FiDatabase className="w-8 h-8" style={{ color: BRAND_BLUE }} />
            DB2 Source Database
          </h1>
          <p className="text-sm text-gray-500 mt-1">
            {tables.length} tables • {totalRecords.toLocaleString()} total records
          </p>
        </div>
        <button
          onClick={fetchTables}
          className="flex items-center gap-2 px-4 py-2 text-white rounded-lg hover:opacity-90 transition-opacity"
          style={{ backgroundColor: BRAND_BLUE }}
        >
          <FiRefreshCw className="w-4 h-4" />
          Refresh
        </button>
      </div>

      {/* Search */}
      <div className="mb-6 relative">
        <FiSearch className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" />
        <input
          type="text"
          placeholder="Search tables..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-[#DA7F3D] focus:border-transparent"
        />
      </div>

      {/* Error Display */}
      {error && (
        <div className="mb-6 bg-red-50 border border-red-200 rounded-lg p-4">
          <div className="flex items-center gap-2 text-red-800">
            <FiDatabase className="w-5 h-5" />
            <span className="font-medium">Connection Error</span>
          </div>
          <p className="mt-2 text-sm text-red-700">{error}</p>
          <p className="mt-2 text-xs text-red-600">
            Check your DB2 connection settings in the backend .env file.
          </p>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Tables List */}
        <div className="lg:col-span-1">
          <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
              <h2 className="font-semibold text-gray-900">Tables</h2>
              <span className="text-sm text-gray-500">{filteredTables.length}</span>
            </div>
            <div className="max-h-[600px] overflow-y-auto">
              {filteredTables.map((table) => (
                <button
                  key={table.name}
                  onClick={() => fetchTableData(table.name)}
                  className={`w-full px-4 py-3 flex items-center justify-between hover:bg-gray-50 transition-colors border-b border-gray-50 ${
                    selectedTable === table.name ? 'bg-blue-50' : ''
                  }`}
                >
                  <div className="flex items-center gap-3">
                    <FiTable className="w-5 h-5 text-gray-400" />
                    <div className="text-left">
                      <p className="font-medium text-gray-900">{table.name}</p>
                      <p className="text-xs text-gray-500">{table.recordCount.toLocaleString()} records</p>
                    </div>
                  </div>
                  <FiChevronRight className="w-4 h-4 text-gray-400" />
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Table Data */}
        <div className="lg:col-span-2">
          {selectedTable ? (
            <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
              <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <FiTable className="w-5 h-5 text-gray-400" />
                  <h2 className="font-semibold text-gray-900">{selectedTable}</h2>
                </div>
                <span className="text-sm text-gray-500">
                  {tableData ? `${tableData.data.length} of ${tableData.total.toLocaleString()} records` : 'Loading...'}
                </span>
              </div>
              
              {tableLoading ? (
                <div className="p-8 flex items-center justify-center">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2" style={{ borderColor: BRAND_BLUE }}></div>
                </div>
              ) : tableData && tableData.data.length > 0 ? (
                <>
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead className="bg-gray-50 border-b border-gray-100">
                        <tr>
                          {tableData.columns.map((col) => (
                            <th key={col} className="px-4 py-3 text-left text-xs font-semibold text-gray-500 uppercase">
                              {col}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-100">
                        {tableData.data.map((row, idx) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            {tableData.columns.map((col) => (
                              <td key={col} className="px-4 py-3 text-sm text-gray-600 max-w-xs truncate">
                                {row[col] !== null && row[col] !== undefined ? String(row[col]) : '-'}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  
                  {tableData.data.length + offset < tableData.total && (
                    <div className="px-4 py-3 border-t border-gray-100 flex justify-center">
                      <button
                        onClick={loadMore}
                        className="px-4 py-2 text-sm text-white rounded-lg hover:opacity-90 transition-opacity"
                        style={{ backgroundColor: BRAND_ORANGE }}
                      >
                        Load More
                      </button>
                    </div>
                  )}
                </>
              ) : (
                <div className="p-8 text-center text-gray-500">
                  No data available
                </div>
              )}
            </div>
          ) : (
            <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-8 text-center">
              <FiDatabase className="w-12 h-12 text-gray-300 mx-auto mb-3" />
              <p className="text-gray-500">Select a table to view its data</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}