import { useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { FiGrid, FiActivity, FiSettings, FiMenu, FiX, FiChevronDown, FiDatabase, FiServer } from 'react-icons/fi';

interface LayoutProps {
  children: React.ReactNode;
}

const navItems = [
  { path: '/', label: 'Dashboard', icon: FiGrid },
  { path: '/pipelines', label: 'Pipelines', icon: FiActivity },
  { path: '/db2', label: 'DB2 Source', icon: FiDatabase },
  { path: '/postgres', label: 'PostgreSQL Target', icon: FiServer },
];

export function Layout({ children }: LayoutProps) {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const location = useLocation();

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Mobile sidebar backdrop */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-gray-900/50 z-40 lg:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* Sidebar */}
      <aside
        className={`fixed top-0 left-0 z-50 h-full w-64 bg-[#1B3E74] text-white transform transition-transform duration-200 ease-in-out lg:translate-x-0 ${
          sidebarOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        <div className="flex items-center gap-3 h-16 px-4 border-b border-white/10">
          <img src="/logo.svg" alt="Logo" className="h-8 w-auto" />
          <span className="text-lg font-bold">Pipeline Monitor</span>
          <button
            onClick={() => setSidebarOpen(false)}
            className="lg:hidden ml-auto text-white/70 hover:text-white"
          >
            <FiX className="w-5 h-5" />
          </button>
        </div>

        <nav className="mt-6 px-3">
          {navItems.map((item) => {
            const Icon = item.icon;
            const isActive = location.pathname === item.path;
            return (
              <Link
                key={item.path}
                to={item.path}
                className={`flex items-center gap-3 px-3 py-2.5 rounded-lg mb-1 transition-all ${
                  isActive
                    ? 'bg-[#DA7F3D] text-white shadow-lg'
                    : 'text-white/80 hover:bg-white/10 hover:text-white'
                }`}
              >
                <Icon className="w-5 h-5" />
                {item.label}
              </Link>
            );
          })}
        </nav>

        <div className="absolute bottom-0 left-0 right-0 p-4 border-t border-white/10">
          <Link
            to="/settings"
            className="flex items-center gap-3 px-3 py-2.5 rounded-lg text-white/80 hover:bg-white/10 hover:text-white transition-all"
          >
            <FiSettings className="w-5 h-5" />
            Settings
          </Link>
        </div>
      </aside>

      {/* Main content */}
      <div className="lg:ml-64">
        {/* Header */}
        <header className="sticky top-0 z-30 h-16 bg-white border-b border-gray-200 flex items-center justify-between px-4 lg:px-6 shadow-sm">
          <button
            onClick={() => setSidebarOpen(true)}
            className="lg:hidden text-gray-600 hover:text-gray-900"
          >
            <FiMenu className="w-6 h-6" />
          </button>

          <div className="flex-1 lg:flex-none" />

          <div className="flex items-center gap-4">
            <div className="hidden sm:flex items-center gap-2 text-sm text-gray-600">
              <span className="w-2 h-2 bg-[#DA7F3D] rounded-full animate-pulse" />
              System Online
            </div>
            <div className="flex items-center gap-2 cursor-pointer">
              <div className="w-8 h-8 bg-[#1B3E74] rounded-full flex items-center justify-center text-white font-medium text-sm">
                MC
              </div>
              <FiChevronDown className="w-4 h-4 text-gray-500" />
            </div>
          </div>
        </header>

        {/* Page content */}
        <main className="p-4 lg:p-6">
          {children}
        </main>
      </div>
    </div>
  );
}