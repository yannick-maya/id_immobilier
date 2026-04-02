import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { AuthProvider } from './context/AuthContext';

// Import des vrais composants
import Navbar from './components/Navbar';
import Home from './pages/Home';
import Login from './pages/Login';
import Register from './pages/Register';
import Dashboard from './pages/Dashboard';
import Recherche from './pages/Recherche';
import BienDetail from './pages/BienDetail';
import Favoris from './pages/Favoris';
import Indice from './pages/Indice';
import Simulateur from './pages/Simulateur';
import Profil from './pages/Profil';

function App() {
  return (
    <AuthProvider>
      <Router>
        <div className="App">
          <Navbar />
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/login" element={<Login />} />
            <Route path="/register" element={<Register />} />
            <Route path="/dashboard" element={<Dashboard />} />
            <Route path="/recherche" element={<Recherche />} />
            <Route path="/bien/:id" element={<BienDetail />} />
            <Route path="/favoris" element={<Favoris />} />
            <Route path="/indice" element={<Indice />} />
            <Route path="/simulateur" element={<Simulateur />} />
            <Route path="/profil" element={<Profil />} />
          </Routes>
        </div>
      </Router>
    </AuthProvider>
  );
}

export default App;