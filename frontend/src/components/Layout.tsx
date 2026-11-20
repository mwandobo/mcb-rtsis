import { useEffect, useRef, useState } from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import { FiGrid, FiActivity, FiDatabase, FiServer, FiMenu, FiUser, FiSettings, FiLogOut } from 'react-icons/fi';
import { useAuth } from '../contexts/AuthContext';

interface LayoutProps {
  children: React.ReactNode;
  title?: string;
}

const navItems = [
  { path: '/', label: 'Dashboard', icon: FiGrid },
  { path: '/pipelines', label: 'Pipelines', icon: FiActivity },
  { path: '/db2', label: 'DB2 Source', icon: FiDatabase },
  { path: '/postgres', label: 'PostgreSQL Target', icon: FiServer },
];

export function Layout({ children, title }: LayoutProps) {
  const [collapsed, setCollapsed] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);
  const location = useLocation();
  const navigate = useNavigate();
  const { user, logout } = useAuth();

  const activeItem = navItems.find((item) => item.path === location.pathname);
  const initials = user?.name
    ? user.name.split(' ').map((p) => p[0]).slice(0, 2).join('').toUpperCase()
    : '?';

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const goToSettings = () => {
    setMenuOpen(false);
    navigate('/settings');
  };

  const handleLogout = () => {
    setMenuOpen(false);
    logout();
    navigate('/login', { replace: true });
  };

  return (
    <div className="app-layout">
      <aside className={`sidebar ${collapsed ? 'collapsed' : ''}`}>
        <div className="sidebar-header">
          <div style={{ background: 'var(--accent)', padding: '5px 8px', display: 'flex', alignItems: 'center', flexShrink: 0 }}>
            <img src="/logo.svg" alt="Mwalimu Commercial Bank" style={{ height: 16, width: 'auto', display: 'block' }} />
          </div>
          <span className="logo-text">RTSIS Pipelines</span>
        </div>

        <nav className="nav">
          {navItems.map((item) => {
            const Icon = item.icon;
            const isActive = location.pathname === item.path;
            return (
              <Link key={item.path} to={item.path} className={`nav-item ${isActive ? 'active' : ''}`}>
                <Icon />
                <span className="nav-label">{item.label}</span>
              </Link>
            );
          })}
        </nav>

        <div className="sidebar-footer" ref={menuRef} style={{ position: 'relative' }}>
          {menuOpen && (
            <div className="user-menu-dropdown" style={{ position: 'absolute', bottom: '100%', left: 8, right: 8, marginBottom: 8 }}>
              <div className="user-menu-email">{user?.email}</div>
              <div style={{ padding: '4px 6px' }}>
                <button className="user-menu-item" onClick={goToSettings}>
                  <FiUser width={14} height={14} />
                  Profile
                </button>
                <button className="user-menu-item" onClick={goToSettings}>
                  <FiSettings width={14} height={14} />
                  Settings
                </button>
              </div>
              <div className="user-menu-divider" />
              <div style={{ padding: '4px 6px' }}>
                <button className="user-menu-item user-menu-item--danger" onClick={handleLogout}>
                  <FiLogOut width={14} height={14} />
                  Log out
                </button>
              </div>
            </div>
          )}

          <button
            onClick={() => setMenuOpen((o) => !o)}
            className="user-card"
            style={{ width: '100%', background: 'none', border: 'none', cursor: 'pointer', textAlign: 'left' }}
          >
            <div className="avatar">{initials}</div>
            <div className="user-info">
              <div className="user-name">{user?.name || 'Unknown User'}</div>
              <div className="user-role" style={{ textTransform: 'capitalize' }}>{user?.role || ''}</div>
            </div>
          </button>
        </div>
      </aside>

      <div className="main">
        <header className="topbar">
          <button className="toggle-btn" onClick={() => setCollapsed((c) => !c)} title="Toggle menu">
            <FiMenu />
          </button>
          <div className="topbar-title">{title || activeItem?.label || 'Pipelines'}</div>
        </header>

        <div className="content">{children}</div>
      </div>
    </div>
  );
}
