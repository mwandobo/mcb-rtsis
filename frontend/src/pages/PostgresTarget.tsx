import { useEffect, useState } from 'react';
import { FiServer, FiTable, FiSearch, FiRefreshCw, FiChevronRight, FiActivity } from 'react-icons/fi';
import { format } from 'date-fns';
import api from '../services/api';

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

const BRAND_BLUE = '#1B3E74';
const BRAND_ORANGE = '#DA7F3D';

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
  const limit = 100;

  const fetchData = async () => {
    setLoading(true);
    try {
      const [tablesData, pipelinesData] = await Promise.all([
        api.get('/databases/postgres/tables'),
        api.get('/databases/postgres/pipeline-stats'),
      ]);
      setTables(Array.isArray(tablesData) ? tablesData : []);
      setPipelineStats(Array.isArray(pipelinesData) ? pipelinesData : []);
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
      const data = await api.get(`/databases/postgres/table/${tableName}`, { params: { limit, offset: 0 } });
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
      const data = await api.get(`/databases/postgres/table/${selectedTable}`, { params: { limit, offset: newOffset } });
      setTableData({
        ...data,
        data: [...tableData.data, ...data.data],
      });
    } catch (err) {
      console.error('Failed to load more data:', err);
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'running': return 'text-green-600 bg-green-100';
      case 'completed': return 'text-blue-600 bg-blue-100';
      case 'idle': return 'text-gray-600 bg-gray-100';
      case 'failed': return 'text-red-600 bg-red-100';
      default: return 'text-gray-600 bg-gray-100';
    }
  };

  const filteredTables = tables.filter(t => 
    t.name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const totalRecords = tables.reduce((sum, t) => sum + t.recordCount, 0);
  const totalPipelineRecords = pipelineStats.reduce((sum, p) => sum + p.recordsProcessed, 0);

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
            <FiServer className="w-8 h-8" style={{ color: BRAND_BLUE }} />
            PostgreSQL Target Database
          </h1>
          <p className="text-sm text-gray-500 mt-1">
            {tables.length} tables • {totalRecords.toLocaleString()} total records • {pipelineStats.length} pipelines
          </p>
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
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <p className="text-sm text-gray-500">Total Tables</p>
          <p className="text-2xl font-bold" style={{ color: BRAND_BLUE }}>{tables.length}</p>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <p className="text-sm text-gray-500">Total Records</p>
          <p className="text-2xl font-bold text-gray-900">{totalRecords.toLocaleString()}</p>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <p className="text-sm text-gray-500">Pipelines</p>
          <p className="text-2xl font-bold text-gray-900">{pipelineStats.length}</p>
        </div>
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-4">
          <p className="text-sm text-gray-500">Pipeline Records</p>
          <p className="text-2xl font-bold" style={{ color: BRAND_ORANGE }}>{totalPipelineRecords.toLocaleString()}</p>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-4 mb-6 border-b border-gray-200">
        <button
          onClick={() => setActiveTab('tables')}
          className={`pb-3 px-1 font-medium transition-colors ${
            activeTab === 'tables'
              ? 'text-[#1B3E74] border-b-2 border-[#1B3E74]'
              : 'text-gray-500 hover:text-gray-700'
          }`}
        >
          Tables
        </button>
        <button
          onClick={() => setActiveTab('pipelines')}
          className={`pb-3 px-1 font-medium transition-colors flex items-center gap-2 ${
            activeTab === 'pipelines'
              ? 'text-[#1B3E74] border-b-2 border-[#1B3E74]'
              : 'text-gray-500 hover:text-gray-700'
          }`}
        >
          <FiActivity className="w-4 h-4" />
          Pipeline Status
        </button>
      </div>

      {activeTab === 'tables' && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Tables List */}
          <div className="lg:col-span-1">
            <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
              <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
                <h2 className="font-semibold text-gray-900">Tables</h2>
                <span className="text-sm text-gray-500">{filteredTables.length}</span>
              </div>
              <div className="p-3">
                <div className="relative mb-3">
                  <FiSearch className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
                  <input
                    type="text"
                    placeholder="Search tables..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    className="w-full pl-9 pr-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-1 focus:ring-[#DA7F3D]"
                  />
                </div>
              </div>
              <div className="max-h-[500px] overflow-y-auto">
                {filteredTables.map((table) => (
                  <button
                    key={table.name}
                    onClick={() => fetchTableData(table.name)}
                    className={`w-full px-4 py-3 flex items-center justify-between hover:bg-gray-50 transition-colors border-t border-gray-50 ${
                      selectedTable === table.name ? 'bg-blue-50' : ''
                    }`}
                  >
                    <div className="flex items-center gap-3">
                      <FiTable className="w-5 h-5 text-gray-400" />
                      <div className="text-left">
                        <p className="font-medium text-gray-900 text-sm">{table.name}</p>
                        <p className="text-xs text-gray-500">{table.recordCount.toLocaleString()} records • {table.columnCount} cols</p>
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
                <FiServer className="w-12 h-12 text-gray-300 mx-auto mb-3" />
                <p className="text-gray-500">Select a table to view its data</p>
              </div>
            )}
          </div>
        </div>
      )}

      {activeTab === 'pipelines' && (
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50 border-b border-gray-100">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase">Pipeline</th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase">Status</th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase">Last Run</th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase">Records Processed</th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase">Last Success</th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase">Error</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {pipelineStats.map((pipeline) => (
                  <tr key={pipeline.name} className="hover:bg-gray-50">
                    <td className="px-6 py-4 font-medium text-gray-900">{pipeline.name}</td>
                    <td className="px-6 py-4">
                      <span className={`inline-flex px-2 py-1 text-xs font-medium rounded-full ${getStatusColor(pipeline.lastRunStatus)}`}>
                        {pipeline.lastRunStatus || 'Unknown'}
                      </span>
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-600">
                      {pipeline.lastRun ? format(new Date(pipeline.lastRun), 'MMM d, HH:mm') : 'Never'}
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-600">{pipeline.recordsProcessed.toLocaleString()}</td>
                    <td className="px-6 py-4 text-sm text-gray-600">
                      {pipeline.lastSuccessfulRun ? format(new Date(pipeline.lastSuccessfulRun), 'MMM d, HH:mm') : 'Never'}
                    </td>
                    <td className="px-6 py-4 text-sm text-red-600 max-w-xs truncate">
                      {pipeline.errorMessage || '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}