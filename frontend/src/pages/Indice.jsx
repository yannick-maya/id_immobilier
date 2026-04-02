import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { api } from '../services/api';

const Indice = () => {
  const [searchParams] = useSearchParams();
  const zoneParam = searchParams.get('zone');

  const [indices, setIndices] = useState([]);
  const [statistiques, setStatistiques] = useState([]);
  const [zones, setZones] = useState([]);
  const [selectedZone, setSelectedZone] = useState(zoneParam || '');
  const [selectedPeriode, setSelectedPeriode] = useState('all');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    loadData();
  }, []);

  useEffect(() => {
    if (selectedZone || selectedPeriode !== 'all') {
      loadFilteredData();
    }
  }, [selectedZone, selectedPeriode]);

  const loadData = async () => {
    try {
      setLoading(true);
      const [indicesRes, statsRes, zonesRes] = await Promise.all([
        api.get('/indice'),
        api.get('/statistiques'),
        api.get('/statistiques/zones')
      ]);

      setIndices(indicesRes.data || []);
      setStatistiques(statsRes.data || []);
      setZones(zonesRes.data || []);
    } catch (error) {
      console.error('Erreur lors du chargement des données:', error);
      setError('Erreur lors du chargement des données');
    } finally {
      setLoading(false);
    }
  };

  const loadFilteredData = async () => {
    try {
      const params = {};
      if (selectedZone) params.zone = selectedZone;
      if (selectedPeriode !== 'all') params.periode = selectedPeriode;

      const [indicesRes, statsRes] = await Promise.all([
        api.get('/indice', { params }),
        api.get('/statistiques', { params })
      ]);

      setIndices(indicesRes.data || []);
      setStatistiques(statsRes.data || []);
    } catch (error) {
      console.error('Erreur lors du chargement des données filtrées:', error);
    }
  };

  const formatPrix = (prix) => {
    return new Intl.NumberFormat('fr-FR').format(prix) + ' FCFA';
  };

  const formatPrixM2 = (prix) => {
    return new Intl.NumberFormat('fr-FR').format(prix) + ' FCFA/m²';
  };

  const formatPourcentage = (valeur) => {
    return `${valeur >= 0 ? '+' : ''}${valeur.toFixed(2)}%`;
  };

  const getEvolutionColor = (evolution) => {
    if (evolution > 0) return 'text-green-600';
    if (evolution < 0) return 'text-red-600';
    return 'text-gray-600';
  };

  const getPeriodeLabel = (periode) => {
    const labels = {
      'all': 'Toutes les périodes',
      '2024': '2024',
      '2024-Q1': 'Q1 2024',
      '2024-Q2': 'Q2 2024',
      '2024-Q3': 'Q3 2024',
      '2024-Q4': 'Q4 2024',
      '2023': '2023',
      '2023-Q1': 'Q1 2023',
      '2023-Q2': 'Q2 2023',
      '2023-Q3': 'Q3 2023',
      '2023-Q4': 'Q4 2023'
    };
    return labels[periode] || periode;
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#065A82]"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">Erreur de chargement</h2>
          <p className="text-gray-600">{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 py-8">
        {/* Titre */}
        <div className="text-center mb-8">
          <h1 className="text-3xl font-bold text-[#065A82] mb-2">
            Indices et Statistiques Immobilières
          </h1>
          <p className="text-gray-600">
            Suivez l'évolution des prix immobiliers par zone et période
          </p>
        </div>

        {/* Filtres */}
        <div className="bg-white rounded-lg shadow-md p-6 mb-8">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Zone géographique
              </label>
              <select
                value={selectedZone}
                onChange={(e) => setSelectedZone(e.target.value)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
              >
                <option value="">Toutes les zones</option>
                {zones.map(zone => (
                  <option key={zone} value={zone}>{zone}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Période
              </label>
              <select
                value={selectedPeriode}
                onChange={(e) => setSelectedPeriode(e.target.value)}
                className="w-full border border-gray-300 rounded-md px-3 py-2 focus:outline-none focus:ring-2 focus:ring-[#065A82]"
              >
                <option value="all">Toutes les périodes</option>
                <option value="2024">2024</option>
                <option value="2024-Q4">Q4 2024</option>
                <option value="2024-Q3">Q3 2024</option>
                <option value="2024-Q2">Q2 2024</option>
                <option value="2024-Q1">Q1 2024</option>
                <option value="2023">2023</option>
                <option value="2023-Q4">Q4 2023</option>
                <option value="2023-Q3">Q3 2023</option>
                <option value="2023-Q2">Q2 2023</option>
                <option value="2023-Q1">Q1 2023</option>
              </select>
            </div>
          </div>
        </div>

        {/* Indices */}
        <div className="mb-8">
          <h2 className="text-2xl font-bold text-[#065A82] mb-6">
            Indices de Prix
          </h2>

          {indices.length > 0 ? (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {indices.map((indice, index) => (
                <div key={index} className="bg-white rounded-lg shadow-md p-6">
                  <div className="flex justify-between items-start mb-4">
                    <div>
                      <h3 className="text-lg font-semibold text-[#065A82]">
                        {indice.zone}
                      </h3>
                      <p className="text-sm text-gray-600">
                        {getPeriodeLabel(indice.periode)}
                      </p>
                    </div>
                    <span className={`text-sm font-medium px-2 py-1 rounded ${
                      indice.evolution_mensuelle >= 0 ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                    }`}>
                      {formatPourcentage(indice.evolution_mensuelle)}
                    </span>
                  </div>

                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <span className="text-gray-600">Prix moyen/m²:</span>
                      <span className="font-semibold">{formatPrixM2(indice.prix_moyen_m2)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-600">Évolution mensuelle:</span>
                      <span className={`font-semibold ${getEvolutionColor(indice.evolution_mensuelle)}`}>
                        {formatPourcentage(indice.evolution_mensuelle)}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-600">Évolution annuelle:</span>
                      <span className={`font-semibold ${getEvolutionColor(indice.evolution_annuelle)}`}>
                        {formatPourcentage(indice.evolution_annuelle)}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-600">Nombre d'annonces:</span>
                      <span className="font-semibold">{indice.nombre_annonces}</span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-center py-12 bg-white rounded-lg shadow-md">
              <p className="text-gray-600">Aucun indice trouvé pour ces critères</p>
            </div>
          )}
        </div>

        {/* Statistiques */}
        <div>
          <h2 className="text-2xl font-bold text-[#065A82] mb-6">
            Statistiques Détaillées
          </h2>

          {statistiques.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full bg-white rounded-lg shadow-md">
                <thead className="bg-[#065A82] text-white">
                  <tr>
                    <th className="px-4 py-3 text-left">Zone</th>
                    <th className="px-4 py-3 text-left">Période</th>
                    <th className="px-4 py-3 text-right">Prix Moyen</th>
                    <th className="px-4 py-3 text-right">Prix/m² Moyen</th>
                    <th className="px-4 py-3 text-right">Surface Moyenne</th>
                    <th className="px-4 py-3 text-right">Nombre d'Annonces</th>
                  </tr>
                </thead>
                <tbody>
                  {statistiques.map((stat, index) => (
                    <tr key={index} className="border-b border-gray-200 hover:bg-gray-50">
                      <td className="px-4 py-3 font-medium">{stat.zone}</td>
                      <td className="px-4 py-3">{getPeriodeLabel(stat.periode)}</td>
                      <td className="px-4 py-3 text-right font-semibold">
                        {formatPrix(stat.prix_moyen)}
                      </td>
                      <td className="px-4 py-3 text-right font-semibold">
                        {formatPrixM2(stat.prix_moyen_m2)}
                      </td>
                      <td className="px-4 py-3 text-right">
                        {stat.surface_moyenne?.toFixed(0)} m²
                      </td>
                      <td className="px-4 py-3 text-right">
                        {stat.nombre_annonces}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-center py-12 bg-white rounded-lg shadow-md">
              <p className="text-gray-600">Aucune statistique trouvée pour ces critères</p>
            </div>
          )}
        </div>

        {/* Légende */}
        <div className="mt-8 bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-bold text-[#065A82] mb-4">
            Comprendre les indices
          </h2>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h3 className="font-semibold text-gray-800 mb-2">📊 Indice de prix</h3>
              <p className="text-sm text-gray-600">
                L'indice mesure l'évolution des prix immobiliers par rapport à une période de référence.
                Un indice supérieur à 100 indique une hausse des prix.
              </p>
            </div>

            <div>
              <h3 className="font-semibold text-gray-800 mb-2">📈 Évolution</h3>
              <p className="text-sm text-gray-600">
                L'évolution mensuelle compare les prix du mois en cours avec le mois précédent.
                L'évolution annuelle compare avec la même période de l'année précédente.
              </p>
            </div>

            <div>
              <h3 className="font-semibold text-gray-800 mb-2">🏠 Prix au m²</h3>
              <p className="text-sm text-gray-600">
                Le prix moyen au mètre carré est calculé en divisant le prix total par la surface habitable.
                Utile pour comparer des biens de tailles différentes.
              </p>
            </div>

            <div>
              <h3 className="font-semibold text-gray-800 mb-2">📅 Périodes</h3>
              <p className="text-sm text-gray-600">
                Les données sont disponibles par trimestre (Q1, Q2, Q3, Q4) et par année complète.
                Cela permet d'analyser les tendances saisonnières et annuelles.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Indice;