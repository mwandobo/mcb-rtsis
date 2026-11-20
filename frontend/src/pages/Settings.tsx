import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { FiLogOut, FiMail } from 'react-icons/fi';
import { useAuth } from '../contexts/AuthContext';

export function Settings() {
  const { user, logout } = useAuth();
  const navigate = useNavigate();
  const [tab, setTab] = useState<'personal' | 'security'>('personal');

  const initials = user?.name
    ? user.name.split(' ').map((p) => p[0]).slice(0, 2).join('').toUpperCase()
    : '?';
  const roleLabel = user?.role ? user.role.charAt(0).toUpperCase() + user.role.slice(1) : 'User';

  const handleLogout = () => {
    logout();
    navigate('/login', { replace: true });
  };

  return (
    <div className="profile-page">
      <div className="profile-grid">
        <div className="profile-left-col">
          <div className="card">
            <div className="profile-avatar-card-head">
              <div className="profile-avatar-display" style={{ marginBottom: 14 }}>
                {initials}
              </div>
              <div className="profile-display-name">{user?.name}</div>
              <div className="profile-role-text">{roleLabel}</div>
              <span className="profile-active-pill">● Active</span>
            </div>
          </div>

          <div className="card">
            <div style={{ padding: '14px 16px', display: 'flex', flexDirection: 'column', gap: 10 }}>
              <div className="profile-info-row">
                <FiMail width={13} height={13} />
                {user?.email}
              </div>
            </div>
          </div>
        </div>

        <div style={{ minWidth: 0 }}>
          <div className="profile-tabbar">
            <button className={`prof-tab ${tab === 'personal' ? 'active' : ''}`} onClick={() => setTab('personal')}>
              Personal
            </button>
            <button className={`prof-tab ${tab === 'security' ? 'active' : ''}`} onClick={() => setTab('security')}>
              Security
            </button>
          </div>

          <div className={`prof-tab-content ${tab === 'personal' ? 'active' : ''}`}>
            <div className="card">
              <div className="profile-card-header-row">
                <div style={{ fontSize: 14, fontWeight: 600 }}>Personal Information</div>
              </div>
              <div className="profile-form-grid">
                <div>
                  <label className="form-label">Full Name</label>
                  <input className="form-input" type="text" value={user?.name || ''} readOnly style={{ opacity: 0.65, cursor: 'default' }} />
                </div>
                <div>
                  <label className="form-label">Email Address</label>
                  <input className="form-input" type="email" value={user?.email || ''} readOnly style={{ opacity: 0.65, cursor: 'default' }} />
                </div>
                <div>
                  <label className="form-label">Role</label>
                  <input className="form-input" type="text" value={roleLabel} readOnly style={{ opacity: 0.65, cursor: 'default' }} />
                </div>
                <div>
                  <label className="form-label">User ID</label>
                  <input className="form-input" type="text" value={user?.id || ''} readOnly style={{ opacity: 0.65, cursor: 'default', fontSize: 12 }} />
                </div>
              </div>
            </div>
          </div>

          <div className={`prof-tab-content ${tab === 'security' ? 'active' : ''}`}>
            <div className="card">
              <div className="profile-card-header-row">
                <div style={{ fontSize: 14, fontWeight: 600 }}>Session</div>
              </div>
              <div style={{ padding: 20 }}>
                <div style={{ fontSize: 13, color: 'var(--text-secondary)', marginBottom: 14 }}>
                  Signed in as <strong style={{ color: 'var(--text-primary)' }}>{user?.email}</strong>. Logging out ends this session on this device.
                </div>
                <button className="btn-danger" onClick={handleLogout}>
                  <FiLogOut width={14} height={14} />
                  Log out
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
