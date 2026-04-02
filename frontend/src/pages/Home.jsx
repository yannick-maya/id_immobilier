import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { indiceService } from '../services/api';
import Navbar from '../components/Navbar';

const Home = () => {
  const [stats, setStats] = useState({});
  const [tendances, setTendances] = useState({});
  const [topZones, setTopZones] = useState([]);

  useEffect(() => {
    // Charger les statistiques générales
    fetch('/api/statistiques')
      .then(res => res.json())
      .then(data => {
        setStats({
          totalAnnonces: data.length,
          zones: [...new Set(data.map(item => item.zone))].length,
          prixMoyen: Math.round(data.reduce((sum, item) => sum + item.prix_moyen_m2, 0) / data.length)
        });

        // Top 5 zones les plus chères
        const sortedZones = data
          .sort((a, b) => b.prix_moyen_m2 - a.prix_moyen_m2)
          .slice(0, 5);
        setTopZones(sortedZones);
      })
      .catch(err => console.error('Erreur stats:', err));

    // Charger les tendances
    indiceService.getTendances()
      .then(response => setTendances(response.data))
      .catch(err => console.error('Erreur tendances:', err));
  }, []);

  return (
    <div className="min-h-screen bg-[#F0F4F8]">
      <Navbar />

      {/* Hero Section */}
      <section className="bg-gradient-to-r from-[#065A82] to-[#0a6b9c] text-white py-20">
        <div className="max-w-7xl mx-auto px-4 text-center">
          <h1 className="text-4xl md:text-6xl font-bold mb-4">
            Marché Immobilier du Togo
          </h1>
          <p className="text-xl mb-8 max-w-2xl mx-auto">
            Découvrez les dernières tendances et trouvez votre bien idéal avec des données fiables et actualisées.
          </p>
          <Link
            to="/recherche"
            className="bg-[#F59E0B] text-[#065A82] px-8 py-4 rounded-lg text-lg font-semibold hover:bg-opacity-90 transition-colors inline-block"
          >
            Rechercher un bien
          </Link>
        </div>
      </section>

      {/* KPI Cards */}
      <section className="py-16">
        <div className="max-w-7xl mx-auto px-4">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
            <div className="bg-white rounded-lg shadow-md p-6 text-center">
              <div className="text-3xl font-bold text-[#065A82] mb-2">{stats.totalAnnonces || 0}</div>
              <div className="text-gray-600">Annonces actives</div>
            </div>
            <div className="bg-white rounded-lg shadow-md p-6 text-center">
              <div className="text-3xl font-bold text-[#065A82] mb-2">{stats.zones || 0}</div>
              <div className="text-gray-600">Zones couvertes</div>
            </div>
            <div className="bg-white rounded-lg shadow-md p-6 text-center">
              <div className="text-3xl font-bold text-[#065A82] mb-2">{stats.prixMoyen || 0} FCFA</div>
              <div className="text-gray-600">Prix moyen au m²</div>
            </div>
          </div>
        </div>
      </section>

      {/* Tendances du marché */}
      <section className="py-16 bg-white">
        <div className="max-w-7xl mx-auto px-4">
          <h2 className="text-3xl font-bold text-center mb-12 text-[#065A82]">Tendances du marché</h2>
          <div className="flex justify-center space-x-8">
            {Object.entries(tendances).map(([tendance, data]) => (
              <div key={tendance} className="text-center">
                <div className={`inline-block px-6 py-3 rounded-full text-white font-semibold text-lg mb-2 ${
                  tendance === 'HAUSSE' ? 'bg-red-500' :
                  tendance === 'STABLE' ? 'bg-yellow-500' : 'bg-green-500'
                }`}>
                  {tendance}
                </div>
                <div className="text-gray-600">{data?.count || 0} zones</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Top zones */}
      <section className="py-16">
        <div className="max-w-7xl mx-auto px-4">
          <h2 className="text-3xl font-bold text-center mb-12 text-[#065A82]">Top zones les plus chères</h2>
          <div className="grid grid-cols-1 md:grid-cols-5 gap-6">
            {topZones.map((zone, index) => (
              <div key={zone.zone} className="bg-white rounded-lg shadow-md p-6 text-center">
                <div className="text-2xl font-bold text-[#065A82] mb-2">#{index + 1}</div>
                <div className="font-semibold text-lg mb-2">{zone.zone}</div>
                <div className="text-[#F59E0B] font-bold">{zone.prix_moyen_m2} FCFA/m²</div>
                <div className="text-gray-600 text-sm">{zone.nombre_annonces} annonces</div>
              </div>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
};

export default Home;