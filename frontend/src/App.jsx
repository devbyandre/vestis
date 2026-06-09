import { useState, lazy, Suspense } from 'react'
import { BarChart2, TrendingUp, BookOpen, ArrowLeftRight, Eye, Bell, Settings, Newspaper, DollarSign } from 'lucide-react'
import { LoadingOverlay } from './components/ui'

// Lazy load each tab — only bundles what's needed
const TabPortfolio      = lazy(() => import('./pages/TabPortfolio'))
const TabRevenues       = lazy(() => import('./pages/TabRevenues'))
const TabPlanning       = lazy(() => import('./pages/TabPlanning'))
const TabTechnical      = lazy(() => import('./pages/TabTechnical'))
const TabNews           = lazy(() => import('./pages/TabNews'))
const TabTransactions   = lazy(() => import('./pages/TabTransactions'))
const TabWatchlist      = lazy(() => import('./pages/TabWatchlist'))
const TabAlerts         = lazy(() => import('./pages/TabAlerts'))
const TabSettings       = lazy(() => import('./pages/TabSettings'))

const TABS = [
  { id: 'portfolio',    label: 'Portfolio',     icon: <BarChart2 size={14} />,    component: TabPortfolio },
  { id: 'revenues',     label: 'Revenues',      icon: <DollarSign size={14} />,   component: TabRevenues },
  { id: 'planning',     label: 'Planning',      icon: <TrendingUp size={14} />,   component: TabPlanning },
  { id: 'technical',    label: 'Technical',     icon: <BookOpen size={14} />,     component: TabTechnical },
  { id: 'news',         label: 'News',          icon: <Newspaper size={14} />,    component: TabNews },
  { id: 'transactions', label: 'Transactions',  icon: <ArrowLeftRight size={14} />, component: TabTransactions },
  { id: 'watchlist',    label: 'Watchlist',     icon: <Eye size={14} />,          component: TabWatchlist },
  { id: 'alerts',       label: 'Alerts',        icon: <Bell size={14} />,         component: TabAlerts },
  { id: 'settings',     label: 'Settings',      icon: <Settings size={14} />,     component: TabSettings },
]

export default function App() {
  const [activeTab, setActiveTab] = useState('portfolio')
  const ActiveComponent = TABS.find(t => t.id === activeTab)?.component ?? TabPortfolio

  return (
    <div className="min-h-screen flex flex-col">
      {/* Header */}
      <header className="border-b border-surface-3 bg-surface-1 sticky top-0 z-40">
        <div className="max-w-screen-2xl mx-auto px-4 py-3 flex items-center gap-4">
          <div className="flex items-center gap-2 mr-4">
            <BarChart2 size={18} className="text-accent" />
            <span className="font-semibold text-gray-100 text-sm tracking-tight">Vestis</span>
          </div>

          {/* Tab navigation */}
          <nav className="flex gap-0.5 overflow-x-auto flex-1">
            {TABS.map(tab => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium whitespace-nowrap transition-all ${
                  activeTab === tab.id
                    ? 'bg-accent/15 text-accent-bright'
                    : 'text-gray-500 hover:text-gray-300 hover:bg-surface-2'
                }`}
              >
                {tab.icon}
                {tab.label}
              </button>
            ))}
          </nav>
        </div>
      </header>

      {/* Content */}
      <main className="flex-1 max-w-screen-2xl mx-auto w-full px-4 py-6">
        <Suspense fallback={<LoadingOverlay label={`Loading ${TABS.find(t => t.id === activeTab)?.label}…`} />}>
          <ActiveComponent />
        </Suspense>
      </main>
    </div>
  )
}
