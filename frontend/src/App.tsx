import { Routes, Route } from 'react-router-dom';
import Sidebar from './components/Sidebar';
import Dashboard from './pages/Dashboard';
import WriteRead from './pages/WriteRead';
import MemTable from './pages/MemTable';
import SSTables from './pages/SSTables';
import Compaction from './pages/Compaction';
import LookupTrace from './pages/LookupTrace';
import LiveLogs from './pages/LiveLogs';
import Config from './pages/Config';
import Terminal from './pages/Terminal';
import LoadGenerator from './pages/LoadGenerator';

export default function App() {
  return (
    <div className="flex h-screen overflow-hidden">
      <Sidebar />
      <main className="flex-1 overflow-y-auto p-6">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/write-read" element={<WriteRead />} />
          <Route path="/memtable" element={<MemTable />} />
          <Route path="/sstables" element={<SSTables />} />
          <Route path="/compaction" element={<Compaction />} />
          <Route path="/lookup-trace" element={<LookupTrace />} />
          <Route path="/live-logs" element={<LiveLogs />} />
          <Route path="/load-generator" element={<LoadGenerator />} />
          <Route path="/terminal" element={<Terminal />} />
          <Route path="/config" element={<Config />} />
        </Routes>
      </main>
    </div>
  );
}
