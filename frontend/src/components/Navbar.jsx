import React from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';

const Navbar = () => {
  const { user, logout } = useAuth();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate('/');
  };

  return (
    <nav className="bg-[#065A82] text-white shadow-lg">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between h-16">
          <div className="flex items-center">
            <Link to="/" className="text-xl font-bold">
              ID Immobilier
            </Link>
          </div>

          <div className="flex items-center space-x-4">
            <Link to="/" className="hover:text-[#F59E0B] transition-colors">
              Accueil
            </Link>
            <Link to="/recherche" className="hover:text-[#F59E0B] transition-colors">
              Recherche
            </Link>
            <Link to="/indice" className="hover:text-[#F59E0B] transition-colors">
              Indice
            </Link>
            <Link to="/simulateur" className="hover:text-[#F59E0B] transition-colors">
              Simulateur
            </Link>

            {user ? (
              <div className="flex items-center space-x-4">
                <span>Bonjour, {user.prenom}</span>
                <button
                  onClick={handleLogout}
                  className="bg-[#F59E0B] text-[#065A82] px-4 py-2 rounded hover:bg-opacity-90 transition-colors"
                >
                  Déconnexion
                </button>
              </div>
            ) : (
              <div className="flex items-center space-x-4">
                <Link
                  to="/login"
                  className="bg-[#F59E0B] text-[#065A82] px-4 py-2 rounded hover:bg-opacity-90 transition-colors"
                >
                  Connexion
                </Link>
                <Link
                  to="/register"
                  className="border border-white px-4 py-2 rounded hover:bg-white hover:text-[#065A82] transition-colors"
                >
                  Inscription
                </Link>
              </div>
            )}
          </div>
        </div>
      </div>
    </nav>
  );
};

export default Navbar;