import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { Layout } from './components/Layout';
import { Dashboard } from './pages/Dashboard';
import { PipelineDetail } from './pages/PipelineDetail';
import { Pipelines } from './pages/Pipelines';
import { DB2Source } from './pages/DB2Source';
import { PostgresTarget } from './pages/PostgresTarget';

function App() {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/pipelines" element={<Pipelines />} />
          <Route path="/pipeline/:name" element={<PipelineDetail />} />
          <Route path="/db2" element={<DB2Source />} />
          <Route path="/postgres" element={<PostgresTarget />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  );
}

export default App;