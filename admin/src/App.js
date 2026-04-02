import React, { useState, useEffect } from 'react';
import api from './services/api';

const tabs = ['dashboard', 'users', 'annonces', 'okr'];

function App() {
  const [token, setToken] = useState(localStorage.getItem('admin_token') || '');
  const [error, setError] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [currentTab, setCurrentTab] = useState('dashboard');
  const [stats, setStats] = useState(null);
  const [users, setUsers] = useState([]);
  const [annonces, setAnnonces] = useState([]);
  const [okr, setOkr] = useState(null);

  const isLogged = Boolean(token);

  useEffect(() => {
    if (!isLogged) return;
    if (currentTab === 'dashboard') fetchDashboard();
    if (currentTab === 'users') fetchUsers();
    if (currentTab === 'annonces') fetchAnnonces();
    if (currentTab === 'okr') fetchOkr();
  }, [isLogged, currentTab]);

  const saveToken = (jwt) => {
    localStorage.setItem('admin_token', jwt);
    setToken(jwt);
  };

  const logout = () => {
    localStorage.removeItem('admin_token');
    setToken('');
    setStats(null);
    setUsers([]);
    setAnnonces([]);
    setOkr(null);
  };

  const handleLogin = async (e) => {
    e.preventDefault();
    setError('');
    try {
      const res = await api.post('/auth/login', { email, password });
      saveToken(res.data.access_token);
      setEmail('');
      setPassword('');
    } catch (e) {
      console.error(e);
      setError(e.response?.data?.detail || 'Login failed');
    }
  };

  const fetchDashboard = async () => {
    try {
      const res = await api.get('/admin/stats');
      setStats(res.data);
    } catch (e) {
      setError('Dashboard load failed: ' + (e.response?.data?.detail || e.message));
    }
  };

  const fetchUsers = async () => {
    try {
      const res = await api.get('/admin/users');
      setUsers(res.data);
    } catch (e) {
      setError('Users load failed: ' + (e.response?.data?.detail || e.message));
    }
  };

  const fetchAnnonces = async () => {
    try {
      const res = await api.get('/admin/annonces');
      setAnnonces(res.data);
    } catch (e) {
      setError('Annonces load failed: ' + (e.response?.data?.detail || e.message));
    }
  };

  const fetchOkr = async () => {
    try {
      const res = await api.get('/admin/okr');
      setOkr(res.data);
    } catch (e) {
      setError('OKR load failed: ' + (e.response?.data?.detail || e.message));
    }
  };

  const changeUserRole = async (id, role) => {
    try {
      await api.put(`/admin/users/${id}`, null, { params: { role } });
      fetchUsers();
    } catch (e) {
      setError('User update failed: ' + (e.response?.data?.detail || e.message));
    }
  };

  const updateAnnonceStatus = async (id, action) => {
    try {
      await api.put(`/admin/annonces/${id}/${action}`);
      fetchAnnonces();
    } catch (e) {
      setError('Annonce update failed: ' + (e.response?.data?.detail || e.message));
    }
  };

  if (!isLogged) {
    return (
      <div style={{ padding: 30 }}>
        <h1>Admin login</h1>
        <form onSubmit={handleLogin}>
          <div>
            <label>Email</label><br />
            <input value={email} onChange={(e) => setEmail(e.target.value)} />
          </div>
          <div style={{ marginTop: 8 }}>
            <label>Password</label><br />
            <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
          </div>
          <button style={{ marginTop: 12 }}>Login</button>
        </form>
        {error && <p style={{ color: 'red' }}>{error}</p>}
      </div>
    );
  }

  return (
    <div style={{ padding: 20 }}>
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h1>Back-Office Admin</h1>
        <button onClick={logout}>Logout</button>
      </header>

      <nav style={{ display: 'flex', gap: 10, marginTop: 12 }}>
        {tabs.map((tab) => (
          <button key={tab} onClick={() => setCurrentTab(tab)} style={{ fontWeight: tab === currentTab ? 'bold' : 'normal' }}>
            {tab}
          </button>
        ))}
      </nav>

      {error && <p style={{ color: 'red' }}>{error}</p>}

      <section style={{ marginTop: 20 }}>
        {currentTab === 'dashboard' && (
          <div>
            <h2>Admin stats</h2>
            {stats ? (
              <div>
                <p>Users: {stats.nb_users}</p>
                <p>Taux refus: {stats.taux_rejet?.toFixed(2)}%</p>
                <p>Ann. par statut: {JSON.stringify(stats.annonces_par_statut)}</p>
              </div>
            ) : <p>Loading...</p>}
          </div>
        )}

        {currentTab === 'users' && (
          <div>
            <h2>Utilisateurs</h2>
            <table border="1" cellPadding="8">
              <thead>
                <tr><th>Email</th><th>Role</th><th>Actions</th></tr>
              </thead>
              <tbody>
                {users.map((u) => (
                  <tr key={u.id}>
                    <td>{u.email}</td>
                    <td>{u.role}</td>
                    <td>
                      {u.role !== 'admin' && <button onClick={() => changeUserRole(u.id, 'admin')}>Make admin</button>}
                      {u.role !== 'user' && <button onClick={() => changeUserRole(u.id, 'user')}>Make user</button>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {currentTab === 'annonces' && (
          <div>
            <h2>Annonces</h2>
            <table border="1" cellPadding="8">
              <thead>
                <tr><th>Id</th><th>Titre</th><th>Statut</th><th>Actions</th></tr>
              </thead>
              <tbody>
                {annonces.map((a) => (
                  <tr key={a.id}>
                    <td>{a.id}</td>
                    <td>{a.titre || 'n/a'}</td>
                    <td>{a.statut || 'inconnu'}</td>
                    <td>
                      <button onClick={() => updateAnnonceStatus(a.id, 'valider')}>Valider</button>
                      <button onClick={() => updateAnnonceStatus(a.id, 'refuser')}>Refuser</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {currentTab === 'okr' && (
          <div>
            <h2>OKR</h2>
            {okr ? (<pre>{JSON.stringify(okr, null, 2)}</pre>) : <p>Loading...</p>}
          </div>
        )}
      </section>
    </div>
  );
}

export default App;
