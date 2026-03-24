import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom';
import { Dashboard } from '@/pages/Dashboard';
import { AskPage } from '@/pages/AskPage';
import { AnalyticsPage } from '@/pages/AnalyticsPage';
import { QualityPage } from '@/pages/QualityPage';
import { AboutPage } from '@/pages/AboutPage';

function NavBar() {
  const linkClass = ({ isActive }: { isActive: boolean }) =>
    `px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
      isActive
        ? 'bg-brand-600 text-white'
        : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700/50'
    }`;

  return (
    <nav className="border-b border-slate-800 bg-slate-900/50 backdrop-blur-sm sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex items-center h-14 gap-1">
        <NavLink to="/" className="text-lg font-bold text-slate-100 mr-6">
          BR Economic Pulse
        </NavLink>
        <NavLink to="/" end className={linkClass}>Home</NavLink>
        <NavLink to="/dashboard" className={linkClass}>Dashboard</NavLink>
        <NavLink to="/ask" className={linkClass}>Ask AI</NavLink>
        <NavLink to="/quality" className={linkClass}>Quality</NavLink>
        <NavLink to="/about" className={linkClass}>About</NavLink>
      </div>
    </nav>
  );
}

function App() {
  return (
    <BrowserRouter>
      <NavBar />
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/dashboard" element={<AnalyticsPage />} />
        <Route path="/ask" element={<AskPage />} />
        <Route path="/quality" element={<QualityPage />} />
        <Route path="/about" element={<AboutPage />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
