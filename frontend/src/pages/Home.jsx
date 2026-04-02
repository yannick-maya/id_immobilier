import React, { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { indiceService } from '../services/api';
import Navbar from '../components/Navbar';
import {
  Home as HomeIcon,
  TrendingUp,
  MapPin,
  DollarSign,
  Search,
  BarChart3,
  Shield,
  Clock,
  Users,
  Star,
  ArrowRight,
  CheckCircle
} from 'lucide-react';

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

  const features = [
    {
      icon: <Search className="w-8 h-8 text-blue-600" />,
      title: "Recherche Avancée",
      description: "Trouvez le bien parfait avec nos filtres intelligents et cartes interactives"
    },
    {
      icon: <BarChart3 className="w-8 h-8 text-green-600" />,
      title: "Analyse du Marché",
      description: "Accédez à des données fiables et des tendances actualisées en temps réel"
    },
    {
      icon: <Shield className="w-8 h-8 text-purple-600" />,
      title: "Données Fiables",
      description: "Informations vérifiées provenant de sources officielles et partenaires"
    },
    {
      icon: <Clock className="w-8 h-8 text-orange-600" />,
      title: "Mise à Jour Régulière",
      description: "Base de données constamment enrichie avec les dernières annonces"
    }
  ];

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-blue-50">
      <Navbar />

      {/* Hero Section */}
      <section className="relative overflow-hidden bg-gradient-to-br from-blue-600 via-blue-700 to-indigo-800 text-white">
        <div className="absolute inset-0 bg-black/10"></div>
        <div className="relative max-w-7xl mx-auto px-4 py-24 lg:py-32">
          <div className="text-center">
            <div className="inline-flex items-center gap-2 bg-white/10 backdrop-blur-sm rounded-full px-4 py-2 mb-6">
              <HomeIcon className="w-5 h-5" />
              <span className="text-sm font-medium">Plateforme Immobilière du Togo</span>
            </div>
            <h1 className="text-4xl md:text-6xl lg:text-7xl font-bold mb-6 leading-tight">
              Votre <span className="text-yellow-400">avenir</span> immobilier
              <br />
              commence ici
            </h1>
            <p className="text-xl md:text-2xl mb-8 max-w-3xl mx-auto text-blue-100 leading-relaxed">
              Découvrez les dernières tendances du marché immobilier togolais avec des données fiables,
              des analyses approfondies et trouvez votre bien idéal.
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center items-center">
              <Link
                to="/recherche"
                className="group bg-yellow-500 hover:bg-yellow-400 text-blue-900 px-8 py-4 rounded-xl text-lg font-semibold transition-all duration-300 transform hover:scale-105 hover:shadow-xl flex items-center gap-2"
              >
                <Search className="w-5 h-5" />
                Rechercher un bien
                <ArrowRight className="w-5 h-5 group-hover:translate-x-1 transition-transform" />
              </Link>
              <Link
                to="/indice"
                className="group bg-white/10 backdrop-blur-sm hover:bg-white/20 text-white px-8 py-4 rounded-xl text-lg font-semibold transition-all duration-300 border border-white/20"
              >
                Voir les indices
              </Link>
            </div>
          </div>
        </div>

        {/* Decorative elements */}
        <div className="absolute top-20 left-10 w-20 h-20 bg-yellow-400/20 rounded-full blur-xl"></div>
        <div className="absolute bottom-20 right-10 w-32 h-32 bg-blue-400/20 rounded-full blur-xl"></div>
      </section>

      {/* Features Section */}
      <section className="py-20 bg-white">
        <div className="max-w-7xl mx-auto px-4">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-4xl font-bold text-gray-900 mb-4">
              Pourquoi choisir ID Immobilier ?
            </h2>
            <p className="text-xl text-gray-600 max-w-2xl mx-auto">
              Une plateforme complète pour tous vos besoins immobiliers au Togo
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
            {features.map((feature, index) => (
              <div
                key={index}
                className="group bg-gradient-to-br from-white to-gray-50 p-6 rounded-2xl shadow-lg hover:shadow-xl transition-all duration-300 transform hover:-translate-y-2 border border-gray-100"
              >
                <div className="bg-gray-50 w-16 h-16 rounded-xl flex items-center justify-center mb-4 group-hover:bg-blue-50 transition-colors">
                  {feature.icon}
                </div>
                <h3 className="text-xl font-semibold text-gray-900 mb-3">{feature.title}</h3>
                <p className="text-gray-600 leading-relaxed">{feature.description}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* KPI Cards */}
      <section className="py-20 bg-gradient-to-r from-blue-50 to-indigo-50">
        <div className="max-w-7xl mx-auto px-4">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-4xl font-bold text-gray-900 mb-4">
              Chiffres clés du marché
            </h2>
            <p className="text-xl text-gray-600">
              Données actualisées en temps réel
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
            <div className="group bg-white rounded-2xl shadow-lg hover:shadow-xl p-8 text-center transition-all duration-300 transform hover:-translate-y-1 border border-gray-100">
              <div className="bg-blue-100 w-16 h-16 rounded-full flex items-center justify-center mx-auto mb-4 group-hover:bg-blue-200 transition-colors">
                <HomeIcon className="w-8 h-8 text-blue-600" />
              </div>
              <div className="text-4xl font-bold text-blue-600 mb-2">{stats.totalAnnonces || 0}</div>
              <div className="text-gray-600 font-medium">Annonces actives</div>
              <div className="text-sm text-gray-500 mt-1">Disponibles à la vente</div>
            </div>

            <div className="group bg-white rounded-2xl shadow-lg hover:shadow-xl p-8 text-center transition-all duration-300 transform hover:-translate-y-1 border border-gray-100">
              <div className="bg-green-100 w-16 h-16 rounded-full flex items-center justify-center mx-auto mb-4 group-hover:bg-green-200 transition-colors">
                <MapPin className="w-8 h-8 text-green-600" />
              </div>
              <div className="text-4xl font-bold text-green-600 mb-2">{stats.zones || 0}</div>
              <div className="text-gray-600 font-medium">Zones couvertes</div>
              <div className="text-sm text-gray-500 mt-1">Dans tout le Togo</div>
            </div>

            <div className="group bg-white rounded-2xl shadow-lg hover:shadow-xl p-8 text-center transition-all duration-300 transform hover:-translate-y-1 border border-gray-100">
              <div className="bg-yellow-100 w-16 h-16 rounded-full flex items-center justify-center mx-auto mb-4 group-hover:bg-yellow-200 transition-colors">
                <DollarSign className="w-8 h-8 text-yellow-600" />
              </div>
              <div className="text-4xl font-bold text-yellow-600 mb-2">{stats.prixMoyen || 0}</div>
              <div className="text-gray-600 font-medium">FCFA/m²</div>
              <div className="text-sm text-gray-500 mt-1">Prix moyen actuel</div>
            </div>
          </div>
        </div>
      </section>

      {/* Tendances du marché */}
      <section className="py-20 bg-white">
        <div className="max-w-7xl mx-auto px-4">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-4xl font-bold text-gray-900 mb-4">
              Tendances du marché
            </h2>
            <p className="text-xl text-gray-600">
              Évolution des prix par zone
            </p>
          </div>

          <div className="flex flex-wrap justify-center gap-6">
            {Object.entries(tendances).map(([tendance, data]) => (
              <div
                key={tendance}
                className={`group bg-gradient-to-br p-6 rounded-2xl shadow-lg hover:shadow-xl transition-all duration-300 transform hover:-translate-y-1 min-w-[200px] ${
                  tendance === 'HAUSSE' ? 'from-red-50 to-red-100 border-red-200' :
                  tendance === 'STABLE' ? 'from-yellow-50 to-yellow-100 border-yellow-200' :
                  'from-green-50 to-green-100 border-green-200'
                } border`}
              >
                <div className="text-center">
                  <div className={`inline-flex items-center justify-center w-16 h-16 rounded-full mb-4 ${
                    tendance === 'HAUSSE' ? 'bg-red-500' :
                    tendance === 'STABLE' ? 'bg-yellow-500' : 'bg-green-500'
                  }`}>
                    <TrendingUp className={`w-8 h-8 text-white ${
                      tendance === 'BAISSE' ? 'rotate-180' : ''
                    }`} />
                  </div>
                  <div className={`text-2xl font-bold mb-2 ${
                    tendance === 'HAUSSE' ? 'text-red-700' :
                    tendance === 'STABLE' ? 'text-yellow-700' : 'text-green-700'
                  }`}>
                    {tendance}
                  </div>
                  <div className="text-gray-600 font-medium">{data?.count || 0} zones</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Top zones */}
      <section className="py-20 bg-gradient-to-r from-gray-50 to-blue-50">
        <div className="max-w-7xl mx-auto px-4">
          <div className="text-center mb-16">
            <h2 className="text-3xl md:text-4xl font-bold text-gray-900 mb-4">
              Top zones les plus prisées
            </h2>
            <p className="text-xl text-gray-600">
              Découvrez les quartiers les plus dynamiques
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-5 gap-6">
            {topZones.map((zone, index) => (
              <div
                key={zone.zone}
                className="group bg-white rounded-2xl shadow-lg hover:shadow-xl p-6 text-center transition-all duration-300 transform hover:-translate-y-2 border border-gray-100"
              >
                <div className="relative">
                  <div className={`inline-flex items-center justify-center w-12 h-12 rounded-full mb-4 font-bold text-white ${
                    index === 0 ? 'bg-yellow-500' :
                    index === 1 ? 'bg-gray-400' :
                    index === 2 ? 'bg-orange-600' :
                    'bg-blue-500'
                  }`}>
                    #{index + 1}
                  </div>
                  {index < 3 && (
                    <div className="absolute -top-2 -right-2">
                      <Star className="w-6 h-6 text-yellow-500 fill-current" />
                    </div>
                  )}
                </div>
                <div className="font-semibold text-lg mb-2 text-gray-900">{zone.zone}</div>
                <div className="text-yellow-600 font-bold text-xl mb-1">{zone.prix_moyen_m2} FCFA/m²</div>
                <div className="text-gray-600 text-sm">{zone.nombre_annonces} annonces</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-20 bg-gradient-to-r from-blue-600 to-indigo-700 text-white">
        <div className="max-w-4xl mx-auto px-4 text-center">
          <h2 className="text-3xl md:text-4xl font-bold mb-6">
            Prêt à trouver votre bien idéal ?
          </h2>
          <p className="text-xl mb-8 text-blue-100">
            Rejoignez des milliers d'utilisateurs qui font confiance à notre plateforme
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Link
              to="/register"
              className="group bg-white text-blue-600 px-8 py-4 rounded-xl text-lg font-semibold hover:bg-gray-100 transition-all duration-300 transform hover:scale-105 flex items-center justify-center gap-2"
            >
              <Users className="w-5 h-5" />
              S'inscrire gratuitement
            </Link>
            <Link
              to="/recherche"
              className="group bg-transparent border-2 border-white text-white px-8 py-4 rounded-xl text-lg font-semibold hover:bg-white hover:text-blue-600 transition-all duration-300 flex items-center justify-center gap-2"
            >
              <Search className="w-5 h-5" />
              Explorer les annonces
            </Link>
          </div>
        </div>
      </section>
    </div>
  );
};

export default Home;