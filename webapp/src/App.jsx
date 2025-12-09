import React, { useState, useEffect, useRef } from 'react';
import { TrendingUp, RefreshCw, DollarSign, Activity, ArrowRightLeft, Radio, Eye } from 'lucide-react';

const ArbitrageDashboard = () => {
  const [opportunities, setOpportunities] = useState([]);
  const [stats, setStats] = useState({
    avgSpread: 0,
    maxSpread: 0,
    trackedSymbols: 0,
    activeOpportunities: 0
  });
  const [lastUpdate, setLastUpdate] = useState(new Date());
  const [isConnected, setIsConnected] = useState(false);
  const [activeTab, setActiveTab] = useState('opportunities');
  const [livePrices, setLivePrices] = useState([]);
  const [priceUpdates, setPriceUpdates] = useState([]);
  const prevUpdatesRef = useRef([]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        // Fetch opportunities
        const oppResponse = await fetch('http://localhost:8081/api/opportunities');
        const oppData = await oppResponse.json();
        
        setOpportunities(oppData.opportunities || []);
        setStats(oppData.stats || {
          avgSpread: 0,
          maxSpread: 0,
          trackedSymbols: 0,
          activeOpportunities: 0
        });
        setLastUpdate(new Date(oppData.lastUpdate));
        setIsConnected(true);

        // Fetch all prices for live feed
        const pricesResponse = await fetch('http://localhost:8081/api/prices');
        const pricesData = await pricesResponse.json();
        
        console.log('Prices API Response:', pricesData); // Debug log
        
        // Update prices table - keep all prices, don't filter by time
        if (pricesData.prices && Array.isArray(pricesData.prices)) {
          setLivePrices(pricesData.prices);
        }
        
        // Update live feed with recent updates
        if (pricesData.updates && Array.isArray(pricesData.updates)) {
          console.log('Updates received:', pricesData.updates.length); // Debug log
          
          // Only update if we have new data
          if (pricesData.updates.length > 0) {
            setPriceUpdates(pricesData.updates);
            prevUpdatesRef.current = pricesData.updates;
          }
        } else {
          console.log('No updates in response'); // Debug log
        }
        
      } catch (error) {
        console.error('Failed to fetch data:', error);
        setIsConnected(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 1000);

    return () => clearInterval(interval);
  }, []);

  const getAgeIndicator = (timestamp) => {
    const age = Date.now() - timestamp;
    if (age < 2000) return { symbol: '●', color: 'text-green-400', label: 'Fresh' };
    if (age < 5000) return { symbol: '◐', color: 'text-yellow-400', label: 'Recent' };
    return { symbol: '○', color: 'text-gray-400', label: 'Older' };
  };

  const formatTime = (date) => {
    return date.toLocaleTimeString('en-US', { 
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  };

  const formatTimeWithMs = (timestamp) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', { 
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    }) + '.' + date.getMilliseconds().toString().padStart(3, '0');
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 text-white p-6">
      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-3">
            <div className="bg-gradient-to-r from-blue-500 to-purple-600 p-3 rounded-xl">
              <TrendingUp className="w-8 h-8" />
            </div>
            <div>
              <h1 className="text-3xl font-bold">Crypto Arbitrage Monitor</h1>
              <p className="text-gray-400 text-sm">Real-time cross-exchange opportunities</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <div className={`flex items-center gap-2 px-4 py-2 rounded-lg ${isConnected ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'}`}>
              <div className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-400' : 'bg-red-400'} animate-pulse`}></div>
              <span className="text-sm font-medium">{isConnected ? 'Live' : 'Disconnected'}</span>
            </div>
          </div>
        </div>

        {/* Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div className="bg-gray-800/50 backdrop-blur-sm border border-gray-700 rounded-xl p-4">
            <div className="flex items-center gap-2 mb-2">
              <Activity className="w-5 h-5 text-blue-400" />
              <span className="text-gray-400 text-sm">Active Opportunities</span>
            </div>
            <div className="text-3xl font-bold text-blue-400">{stats.activeOpportunities}</div>
          </div>

          <div className="bg-gray-800/50 backdrop-blur-sm border border-gray-700 rounded-xl p-4">
            <div className="flex items-center gap-2 mb-2">
              <TrendingUp className="w-5 h-5 text-green-400" />
              <span className="text-gray-400 text-sm">Max Spread</span>
            </div>
            <div className="text-3xl font-bold text-green-400">{stats.maxSpread.toFixed(2)}%</div>
          </div>

          <div className="bg-gray-800/50 backdrop-blur-sm border border-gray-700 rounded-xl p-4">
            <div className="flex items-center gap-2 mb-2">
              <DollarSign className="w-5 h-5 text-yellow-400" />
              <span className="text-gray-400 text-sm">Avg Spread</span>
            </div>
            <div className="text-3xl font-bold text-yellow-400">{stats.avgSpread.toFixed(2)}%</div>
          </div>

          <div className="bg-gray-800/50 backdrop-blur-sm border border-gray-700 rounded-xl p-4">
            <div className="flex items-center gap-2 mb-2">
              <ArrowRightLeft className="w-5 h-5 text-purple-400" />
              <span className="text-gray-400 text-sm">Tracked Symbols</span>
            </div>
            <div className="text-3xl font-bold text-purple-400">{stats.trackedSymbols}</div>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-2 mb-4">
        <button
          onClick={() => setActiveTab('opportunities')}
          className={`flex items-center gap-2 px-6 py-3 rounded-lg font-semibold transition-all ${
            activeTab === 'opportunities'
              ? 'bg-gradient-to-r from-blue-600 to-purple-600 text-white'
              : 'bg-gray-800/50 text-gray-400 hover:bg-gray-700/50'
          }`}
        >
          <TrendingUp className="w-4 h-4" />
          Arbitrage Opportunities
        </button>
        <button
          onClick={() => setActiveTab('live-feed')}
          className={`flex items-center gap-2 px-6 py-3 rounded-lg font-semibold transition-all ${
            activeTab === 'live-feed'
              ? 'bg-gradient-to-r from-blue-600 to-purple-600 text-white'
              : 'bg-gray-800/50 text-gray-400 hover:bg-gray-700/50'
          }`}
        >
          <Radio className="w-4 h-4" />
          Live Price Feed
          {priceUpdates.length > 0 && (
            <span className="bg-green-400 text-gray-900 text-xs px-2 py-0.5 rounded-full font-bold">
              {priceUpdates.length}
            </span>
          )}
        </button>
      </div>

      {/* Opportunities Table */}
      {activeTab === 'opportunities' && (
        <div className="bg-gray-800/50 backdrop-blur-sm border border-gray-700 rounded-xl overflow-hidden">
          <div className="bg-gradient-to-r from-blue-600 to-purple-600 px-6 py-4">
            <div className="flex items-center justify-between">
              <h2 className="text-xl font-bold">Top 10 Opportunities by Spread</h2>
              <div className="flex items-center gap-2 text-sm">
                <RefreshCw className="w-4 h-4 animate-spin" />
                <span>Last updated: {formatTime(lastUpdate)}</span>
              </div>
            </div>
          </div>

          {opportunities.length === 0 ? (
            <div className="px-6 py-16 text-center">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-gray-700/50 mb-4">
                <Activity className="w-8 h-8 text-gray-500" />
              </div>
              <h3 className="text-xl font-semibold mb-2">No Opportunities Detected</h3>
              <p className="text-gray-400 mb-4">Waiting for price discrepancies above 0.5% threshold...</p>
              {stats.trackedSymbols > 0 && (
                <div className="text-sm text-gray-500">
                  Currently monitoring {stats.trackedSymbols} symbols. Check the "Live Price Feed" tab to see raw data.
                </div>
              )}
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-700/50">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-semibold text-gray-300 uppercase tracking-wider">Rank</th>
                    <th className="px-4 py-3 text-left text-xs font-semibold text-gray-300 uppercase tracking-wider">Symbol</th>
                    <th className="px-4 py-3 text-left text-xs font-semibold text-gray-300 uppercase tracking-wider">Buy From</th>
                    <th className="px-4 py-3 text-left text-xs font-semibold text-gray-300 uppercase tracking-wider">Sell To</th>
                    <th className="px-4 py-3 text-right text-xs font-semibold text-gray-300 uppercase tracking-wider">Buy Price</th>
                    <th className="px-4 py-3 text-right text-xs font-semibold text-gray-300 uppercase tracking-wider">Sell Price</th>
                    <th className="px-4 py-3 text-right text-xs font-semibold text-gray-300 uppercase tracking-wider">Spread</th>
                    <th className="px-4 py-3 text-right text-xs font-semibold text-gray-300 uppercase tracking-wider">Profit ($)</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-700">
                  {opportunities.map((opp, idx) => {
                    const ageInfo = getAgeIndicator(opp.timestamp);
                    return (
                      <tr key={idx} className="hover:bg-gray-700/30 transition-colors">
                        <td className="px-4 py-4">
                          <div className="flex items-center gap-2">
                            <span className={`${ageInfo.color} text-lg`} title={ageInfo.label}>
                              {ageInfo.symbol}
                            </span>
                            <span className="font-semibold text-gray-300">#{idx + 1}</span>
                          </div>
                        </td>
                        <td className="px-4 py-4">
                          <span className="font-bold text-white">{opp.symbol}</span>
                        </td>
                        <td className="px-4 py-4">
                          <span className="px-3 py-1 bg-blue-500/20 text-blue-400 rounded-lg text-sm font-medium">
                            {opp.buyExchange}
                          </span>
                        </td>
                        <td className="px-4 py-4">
                          <span className="px-3 py-1 bg-purple-500/20 text-purple-400 rounded-lg text-sm font-medium">
                            {opp.sellExchange}
                          </span>
                        </td>
                        <td className="px-4 py-4 text-right font-mono text-sm text-gray-300">
                          ${opp.buyPrice.toFixed(4)}
                        </td>
                        <td className="px-4 py-4 text-right font-mono text-sm text-gray-300">
                          ${opp.sellPrice.toFixed(4)}
                        </td>
                        <td className="px-4 py-4 text-right">
                          <span className={`font-bold text-lg ${
                            opp.spread >= 2 ? 'text-green-400' :
                            opp.spread >= 1 ? 'text-yellow-400' :
                            opp.spread >= 0.5 ? 'text-orange-400' :
                            'text-gray-400'
                          }`}>
                            {opp.spread.toFixed(2)}%
                          </span>
                        </td>
                        <td className="px-4 py-4 text-right">
                          <span className="font-mono text-green-400">
                            ${opp.priceDiff.toFixed(2)}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          <div className="px-6 py-4 bg-gray-700/30 border-t border-gray-700 flex items-center justify-between text-sm">
            <div className="flex items-center gap-6">
              <span className="text-gray-400">Legend:</span>
              <div className="flex items-center gap-2">
                <span className="text-green-400">●</span>
                <span className="text-gray-300">Fresh (&lt;2s)</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-yellow-400">◐</span>
                <span className="text-gray-300">Recent (2-5s)</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-gray-400">○</span>
                <span className="text-gray-300">Older (&gt;5s)</span>
              </div>
            </div>
            <div className="text-gray-400">
              💡 Showing opportunities &gt;0.5% spread. Green ≥2%, Yellow ≥1%, Orange ≥0.5%
            </div>
          </div>
        </div>
      )}

      {/* Live Price Feed */}
      {activeTab === 'live-feed' && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Price Updates Stream */}
          <div className="bg-gray-800/50 backdrop-blur-sm border border-gray-700 rounded-xl overflow-hidden">
            <div className="bg-gradient-to-r from-green-600 to-blue-600 px-6 py-4">
              <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold flex items-center gap-2">
                  <Radio className="w-5 h-5 animate-pulse" />
                  Live Updates Stream
                </h2>
                <span className="text-sm">Last {Math.min(priceUpdates.length, 100)} updates</span>
              </div>
            </div>
            
            <div className="h-[600px] overflow-y-auto">
              {priceUpdates.length === 0 ? (
                <div className="flex items-center justify-center h-full text-gray-500">
                  <div className="text-center">
                    <Eye className="w-12 h-12 mx-auto mb-2 opacity-50" />
                    <p>Waiting for price updates...</p>
                    <p className="text-xs mt-2">Check browser console for debug info</p>
                  </div>
                </div>
              ) : (
                <div className="divide-y divide-gray-700">
                  {priceUpdates.map((update, idx) => (
                    <div 
                      key={`${update.symbol}-${update.exchange}-${idx}`}
                      className="px-4 py-3 hover:bg-gray-700/30 transition-colors"
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                          <span className={`px-2 py-1 rounded text-xs font-bold ${
                            update.exchange === 'COINBASE' 
                              ? 'bg-blue-500/20 text-blue-400' 
                              : 'bg-purple-500/20 text-purple-400'
                          }`}>
                            {update.exchange}
                          </span>
                          <span className="font-bold text-white">{update.symbol}</span>
                        </div>
                        <div className="text-right">
                          <div className="font-mono text-green-400 font-bold">
                            ${typeof update.price === 'number' ? update.price.toFixed(4) : update.price}
                          </div>
                          <div className="text-xs text-gray-500">
                            {formatTimeWithMs(update.timestamp)}
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Current Prices Table */}
          <div className="bg-gray-800/50 backdrop-blur-sm border border-gray-700 rounded-xl overflow-hidden">
            <div className="bg-gradient-to-r from-purple-600 to-pink-600 px-6 py-4">
              <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold flex items-center gap-2">
                  <Eye className="w-5 h-5" />
                  Current Prices by Symbol
                </h2>
                <span className="text-sm">{livePrices.length} symbols</span>
              </div>
            </div>
            
            <div className="h-[600px] overflow-y-auto">
              {livePrices.length === 0 ? (
                <div className="flex items-center justify-center h-full text-gray-500">
                  <div className="text-center">
                    <Activity className="w-12 h-12 mx-auto mb-2 opacity-50" />
                    <p>No price data yet...</p>
                    <p className="text-xs mt-2">Waiting for first poll...</p>
                  </div>
                </div>
              ) : (
                <table className="w-full">
                  <thead className="bg-gray-700/50 sticky top-0">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-semibold text-gray-300 uppercase">Symbol</th>
                      <th className="px-4 py-3 text-right text-xs font-semibold text-gray-300 uppercase">Coinbase</th>
                      <th className="px-4 py-3 text-right text-xs font-semibold text-gray-300 uppercase">Binance</th>
                      <th className="px-4 py-3 text-right text-xs font-semibold text-gray-300 uppercase">Spread</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-700">
                    {livePrices.map((item) => {
                      const hasSpread = item.spreadPercent !== undefined && item.spreadPercent !== null;
                      const cbAge = item.coinbaseAge || 0;
                      const bnAge = item.binanceAge || 0;
                      const cbFresh = cbAge <= 5000;
                      const bnFresh = bnAge <= 5000;
                      const bothFresh = item.bothFresh || false;
                      
                      return (
                        <tr key={item.symbol} className={`hover:bg-gray-700/30 ${bothFresh && hasSpread && item.spreadPercent >= 0.5 ? 'bg-green-900/10' : ''}`}>
                          <td className="px-4 py-3 font-bold text-white">
                            {item.symbol}
                            {bothFresh && hasSpread && item.spreadPercent >= 0.5 && (
                              <span className="ml-2 text-xs text-green-400">⚡</span>
                            )}
                          </td>
                          <td className="px-4 py-3 text-right font-mono text-sm">
                            {item.coinbasePrice !== undefined && item.coinbasePrice !== null ? (
                              <div className="flex flex-col items-end">
                                <span className={cbFresh ? "text-blue-400" : "text-gray-600"}>${item.coinbasePrice.toFixed(4)}</span>
                                <span className="text-xs text-gray-600">{(cbAge / 1000).toFixed(1)}s</span>
                              </div>
                            ) : (
                              <span className="text-gray-600">-</span>
                            )}
                          </td>
                          <td className="px-4 py-3 text-right font-mono text-sm">
                            {item.binancePrice !== undefined && item.binancePrice !== null ? (
                              <div className="flex flex-col items-end">
                                <span className={bnFresh ? "text-purple-400" : "text-gray-600"}>${item.binancePrice.toFixed(4)}</span>
                                <span className="text-xs text-gray-600">{(bnAge / 1000).toFixed(1)}s</span>
                              </div>
                            ) : (
                              <span className="text-gray-600">-</span>
                            )}
                          </td>
                          <td className="px-4 py-3 text-right">
                            {hasSpread ? (
                              <span className={`font-mono text-sm ${
                                bothFresh && item.spreadPercent >= 0.5 ? 'text-green-400 font-bold' : 'text-gray-500'
                              }`}>
                                {item.spreadPercent.toFixed(2)}%
                              </span>
                            ) : (
                              <span className="text-gray-600">-</span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default ArbitrageDashboard;