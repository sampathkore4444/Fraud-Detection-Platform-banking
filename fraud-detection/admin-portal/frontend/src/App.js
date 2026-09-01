import React, { useState, useEffect, useCallback } from 'react';
import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom';
import './App.css';

const API_BASE = process.env.REACT_APP_API_URL || 'http://localhost:8000';

// ─── API Client ──────────────────────────────────────────────────────────────

const api = {
  async get(path) {
    const res = await fetch(`${API_BASE}${path}`);
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json();
  },
  async put(path, body) {
    const res = await fetch(`${API_BASE}${path}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json();
  },
  async post(path, body) {
    const res = await fetch(`${API_BASE}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json();
  },
};

// ─── Layout ──────────────────────────────────────────────────────────────────

function Layout({ children }) {
  return (
    <div className="app">
      <nav className="sidebar">
        <div className="logo">
          <h2>Fraud Admin</h2>
          <span className="version">v1.0.0</span>
        </div>
        <ul>
          <li><NavLink to="/" end>Dashboard</NavLink></li>
          <li><NavLink to="/review-queue">Review Queue</NavLink></li>
          <li><NavLink to="/decisions">Decisions</NavLink></li>
          <li><NavLink to="/models">Models</NavLink></li>
          <li><NavLink to="/rules">Rules</NavLink></li>
          <li><NavLink to="/cases">Cases</NavLink></li>
          <li><NavLink to="/audit">Audit Trail</NavLink></li>
        </ul>
      </nav>
      <main className="content">{children}</main>
    </div>
  );
}

// ─── Dashboard Page ──────────────────────────────────────────────────────────

function Dashboard() {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get('/api/stats').then(setStats).catch(console.error).finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="page"><h1>Dashboard</h1><p>Loading...</p></div>;
  if (!stats) return <div className="page"><h1>Dashboard</h1><p>Failed to load stats</p></div>;

  return (
    <div className="page">
      <h1>Dashboard</h1>
      <p className="subtitle">Last 1 hour — {stats.timestamp}</p>

      <div className="stats-grid">
        <StatCard title="Total Transactions" value={stats.total_transactions} color="#3b82f6" />
        <StatCard title="Approved" value={stats.approved} color="#22c55e" />
        <StatCard title="Review Queue" value={stats.reviewed} color="#f59e0b" />
        <StatCard title="Declined" value={stats.declined} color="#ef4444" />
        <StatCard title="Fraud Rate" value={`${(stats.fraud_rate * 100).toFixed(2)}%`} color="#ef4444" />
        <StatCard title="Avg Latency" value={`${stats.avg_latency_ms}ms`} color="#3b82f6" />
        <StatCard title="P95 Latency" value={`${stats.p95_latency_ms}ms`} color="#8b5cf6" />
        <StatCard title="Active Model" value={stats.active_model} color="#06b6d4" />
        <StatCard title="Open Cases" value={stats.open_cases} color="#f97316" />
      </div>

      <div className="card">
        <h3>Decision Distribution</h3>
        <div className="bar-chart">
          <div className="bar-row">
            <span className="bar-label">Approved</span>
            <div className="bar-track">
              <div className="bar-fill approved" style={{width: `${(stats.approved / Math.max(stats.total_transactions, 1)) * 100}%`}} />
            </div>
            <span className="bar-value">{stats.approved}</span>
          </div>
          <div className="bar-row">
            <span className="bar-label">Review</span>
            <div className="bar-track">
              <div className="bar-fill review" style={{width: `${(stats.reviewed / Math.max(stats.total_transactions, 1)) * 100}%`}} />
            </div>
            <span className="bar-value">{stats.reviewed}</span>
          </div>
          <div className="bar-row">
            <span className="bar-label">Declined</span>
            <div className="bar-track">
              <div className="bar-fill declined" style={{width: `${(stats.declined / Math.max(stats.total_transactions, 1)) * 100}%`}} />
            </div>
            <span className="bar-value">{stats.declined}</span>
          </div>
        </div>
      </div>
    </div>
  );
}

function StatCard({ title, value, color }) {
  return (
    <div className="stat-card" style={{ borderTopColor: color }}>
      <div className="stat-title">{title}</div>
      <div className="stat-value" style={{ color }}>{value}</div>
    </div>
  );
}

// ─── Review Queue Page ───────────────────────────────────────────────────────

function ReviewQueue() {
  const [queue, setQueue] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);

  const loadQueue = useCallback(() => {
    setLoading(true);
    api.get('/api/review-queue?limit=100')
      .then(data => { setQueue(data.queue); setTotal(data.total); })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  useEffect(loadQueue, [loadQueue]);

  const handleAction = async (txId, action) => {
    const notes = prompt(`Add notes for ${action}:`);
    if (notes === null) return;
    await api.put(`/api/review-queue/${txId}`, {
      action, analyst_id: 'analyst-1', notes,
    });
    loadQueue();
  };

  return (
    <div className="page">
      <h1>Review Queue</h1>
      <p className="subtitle">{total} transactions pending review</p>

      {loading ? <p>Loading...</p> : (
        <table className="data-table">
          <thead>
            <tr>
              <th>Transaction ID</th>
              <th>Account</th>
              <th>Fraud Probability</th>
              <th>Reason</th>
              <th>Model</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {queue.map(item => (
              <tr key={item.transaction_id}>
                <td className="mono">{item.transaction_id?.substring(0, 12)}...</td>
                <td>{item.account_id}</td>
                <td>
                  <span className={`badge ${item.fraud_probability > 0.7 ? 'danger' : item.fraud_probability > 0.3 ? 'warning' : 'success'}`}>
                    {(item.fraud_probability * 100).toFixed(1)}%
                  </span>
                </td>
                <td>{item.reason_code}</td>
                <td>{item.model_version}</td>
                <td>
                  <button className="btn btn-approve" onClick={() => handleAction(item.transaction_id, 'APPROVE')}>Approve</button>
                  <button className="btn btn-decline" onClick={() => handleAction(item.transaction_id, 'DECLINE')}>Decline</button>
                </td>
              </tr>
            ))}
            {queue.length === 0 && <tr><td colSpan="6" className="empty">No items in review queue</td></tr>}
          </tbody>
        </table>
      )}
    </div>
  );
}

// ─── Decisions Page ──────────────────────────────────────────────────────────

function Decisions() {
  const [decisions, setDecisions] = useState([]);
  const [filter, setFilter] = useState('');
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    const params = filter ? `?decision=${filter}&limit=100` : '?limit=100';
    api.get(`/api/decisions${params}`)
      .then(data => { setDecisions(data.decisions); setTotal(data.total); })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [filter]);

  return (
    <div className="page">
      <h1>Decisions</h1>
      <div className="filters">
        <select value={filter} onChange={e => setFilter(e.target.value)}>
          <option value="">All</option>
          <option value="APPROVE">Approved</option>
          <option value="REVIEW">Review</option>
          <option value="DECLINE">Declined</option>
        </select>
        <span className="count">{total} results</span>
      </div>

      {loading ? <p>Loading...</p> : (
        <table className="data-table">
          <thead>
            <tr>
              <th>Transaction ID</th>
              <th>Account</th>
              <th>Decision</th>
              <th>Fraud Probability</th>
              <th>Reason</th>
              <th>Latency</th>
            </tr>
          </thead>
          <tbody>
            {decisions.map(d => (
              <tr key={d.transaction_id}>
                <td className="mono">{d.transaction_id?.substring(0, 12)}...</td>
                <td>{d.account_id}</td>
                <td>
                  <span className={`badge ${d.decision === 'APPROVE' ? 'success' : d.decision === 'REVIEW' ? 'warning' : 'danger'}`}>
                    {d.decision}
                  </span>
                </td>
                <td>{(d.fraud_probability * 100).toFixed(1)}%</td>
                <td>{d.reason_code}</td>
                <td>{d.latency_ms}ms</td>
              </tr>
            ))}
            {decisions.length === 0 && <tr><td colSpan="6" className="empty">No decisions found</td></tr>}
          </tbody>
        </table>
      )}
    </div>
  );
}

// ─── Models Page ─────────────────────────────────────────────────────────────

function Models() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get('/api/models').then(setData).catch(console.error).finally(() => setLoading(false));
  }, []);

  const activateModel = async (version) => {
    if (!window.confirm(`Activate model ${version}?`)) return;
    await api.post('/api/models/activate', { version });
    api.get('/api/models').then(setData);
  };

  if (loading) return <div className="page"><h1>Models</h1><p>Loading...</p></div>;

  return (
    <div className="page">
      <h1>Models</h1>
      <p className="subtitle">Active: {data?.active_version}</p>

      <table className="data-table">
        <thead>
          <tr>
            <th>Version</th>
            <th>Status</th>
            <th>Trained At</th>
            <th>Trees</th>
            <th>AUC-ROC</th>
            <th>Precision</th>
            <th>Recall</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {(data?.models || []).map(m => (
            <tr key={m.version}>
              <td className="mono">{m.version}</td>
              <td>
                <span className={`badge ${m.status === 'active' ? 'success' : 'neutral'}`}>
                  {m.status}
                </span>
              </td>
              <td>{m.trained_at}</td>
              <td>{m.num_trees}</td>
              <td>{m.metrics?.auc_roc}</td>
              <td>{m.metrics?.precision}</td>
              <td>{m.metrics?.recall}</td>
              <td>
                {m.status !== 'active' && (
                  <button className="btn btn-approve" onClick={() => activateModel(m.version)}>
                    Activate
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Rules Page ──────────────────────────────────────────────────────────────

function Rules() {
  const [rules, setRules] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get('/api/rules').then(data => setRules(data.rules)).catch(console.error).finally(() => setLoading(false));
  }, []);

  const toggleRule = async (ruleId) => {
    await api.put(`/api/rules/${ruleId}/toggle`);
    api.get('/api/rules').then(data => setRules(data.rules));
  };

  if (loading) return <div className="page"><h1>Rules</h1><p>Loading...</p></div>;

  return (
    <div className="page">
      <h1>Fraud Rules</h1>
      <p className="subtitle">{rules.length} rules configured</p>

      <table className="data-table">
        <thead>
          <tr>
            <th>ID</th>
            <th>Name</th>
            <th>Type</th>
            <th>Condition</th>
            <th>Severity</th>
            <th>Enabled</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {rules.map(rule => (
            <tr key={rule.id}>
              <td className="mono">{rule.id}</td>
              <td>{rule.name}</td>
              <td>{rule.rule_type}</td>
              <td className="mono">{rule.condition?.field} {rule.condition?.operator} {rule.condition?.value}</td>
              <td>
                <span className={`badge ${rule.severity === 2 ? 'danger' : 'warning'}`}>
                  {rule.severity === 2 ? 'DECLINE' : 'REVIEW'}
                </span>
              </td>
              <td>
                <span className={`badge ${rule.enabled ? 'success' : 'neutral'}`}>
                  {rule.enabled ? 'ON' : 'OFF'}
                </span>
              </td>
              <td>
                <button className="btn btn-toggle" onClick={() => toggleRule(rule.id)}>
                  {rule.enabled ? 'Disable' : 'Enable'}
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Cases Page ──────────────────────────────────────────────────────────────

function Cases() {
  const [cases, setCases] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get('/api/cases?limit=100')
      .then(data => { setCases(data.cases); setTotal(data.total); })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="page"><h1>Cases</h1><p>Loading...</p></div>;

  return (
    <div className="page">
      <h1>Investigation Cases</h1>
      <p className="subtitle">{total} cases</p>

      <table className="data-table">
        <thead>
          <tr>
            <th>Case ID</th>
            <th>Transaction</th>
            <th>Account</th>
            <th>Fraud Prob</th>
            <th>Status</th>
            <th>Analyst</th>
            <th>Created</th>
          </tr>
        </thead>
        <tbody>
          {cases.map(c => (
            <tr key={c.case_id}>
              <td className="mono">{c.case_id}</td>
              <td className="mono">{c.decision_id?.substring(0, 12)}...</td>
              <td>{c.account_id}</td>
              <td>{(c.fraud_probability * 100).toFixed(1)}%</td>
              <td>
                <span className={`badge ${c.status === 'OPEN' ? 'warning' : c.status === 'INVESTIGATING' ? 'info' : 'success'}`}>
                  {c.status}
                </span>
              </td>
              <td>{c.analyst_id}</td>
              <td>{c.created_at}</td>
            </tr>
          ))}
          {cases.length === 0 && <tr><td colSpan="7" className="empty">No cases found</td></tr>}
        </tbody>
      </table>
    </div>
  );
}

// ─── Audit Trail Page ────────────────────────────────────────────────────────

function AuditTrail() {
  const [audits, setAudits] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get('/api/audit?limit=100')
      .then(data => { setAudits(data.audit_trail); setTotal(data.total); })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="page"><h1>Audit Trail</h1><p>Loading...</p></div>;

  return (
    <div className="page">
      <h1>Audit Trail</h1>
      <p className="subtitle">{total} entries</p>

      <table className="data-table">
        <thead>
          <tr>
            <th>ID</th>
            <th>Action</th>
            <th>Details</th>
            <th>Timestamp</th>
          </tr>
        </thead>
        <tbody>
          {audits.map(a => (
            <tr key={a.id}>
              <td className="mono">{a.id}</td>
              <td>
                <span className={`badge ${a.action.includes('override') ? 'warning' : a.action.includes('create') ? 'info' : 'neutral'}`}>
                  {a.action}
                </span>
              </td>
              <td className="mono small">{JSON.stringify(a.details)}</td>
              <td>{a.timestamp}</td>
            </tr>
          ))}
          {audits.length === 0 && <tr><td colSpan="4" className="empty">No audit entries</td></tr>}
        </tbody>
      </table>
    </div>
  );
}

// ─── App ─────────────────────────────────────────────────────────────────────

function App() {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/review-queue" element={<ReviewQueue />} />
          <Route path="/decisions" element={<Decisions />} />
          <Route path="/models" element={<Models />} />
          <Route path="/rules" element={<Rules />} />
          <Route path="/cases" element={<Cases />} />
          <Route path="/audit" element={<AuditTrail />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  );
}

export default App;
