import { createContext, useContext, useState, ReactNode } from 'react';

interface TabsContextType {
  activeTab: string;
  setActiveTab: (tab: string) => void;
}

const TabsContext = createContext<TabsContextType | undefined>(undefined);

interface TabsProps {
  defaultValue?: string;
  value?: string;
  onChange?: (value: string) => void;
  children: ReactNode;
  className?: string;
}

export function Tabs({ defaultValue = '', value, onChange, children, className = '' }: TabsProps) {
  const [internalActiveTab, setInternalActiveTab] = useState(defaultValue);

  // Controlled vs uncontrolled mode
  const activeTab = value !== undefined ? value : internalActiveTab;
  const setActiveTab = (newValue: string) => {
    if (onChange) {
      onChange(newValue);
    } else {
      setInternalActiveTab(newValue);
    }
  };

  return (
    <TabsContext.Provider value={{ activeTab, setActiveTab }}>
      <div className={className}>{children}</div>
    </TabsContext.Provider>
  );
}

interface TabsListProps {
  children: ReactNode;
  className?: string;
}

export function TabsList({ children, className = '' }: TabsListProps) {
  return (
    <div className={`inline-flex gap-1 p-1 bg-gray-900 rounded-lg border border-gray-800 ${className}`}>
      {children}
    </div>
  );
}

interface TabsTriggerProps {
  value: string;
  children: ReactNode;
  className?: string;
}

export function TabsTrigger({ value, children, className = '' }: TabsTriggerProps) {
  const context = useContext(TabsContext);
  if (!context) throw new Error('TabsTrigger must be used within Tabs');

  const { activeTab, setActiveTab } = context;
  const isActive = activeTab === value;

  return (
    <button
      onClick={() => setActiveTab(value)}
      className={`
        inline-flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-md transition-all duration-200
        ${isActive
          ? 'bg-gray-800 text-foreground shadow-sm'
          : 'text-gray-400 hover:text-foreground hover:bg-gray-800/50'
        }
        ${className}
      `}
    >
      {children}
    </button>
  );
}

interface TabsContentProps {
  value: string;
  children: ReactNode;
  className?: string;
}

export function TabsContent({ value, children, className = '' }: TabsContentProps) {
  const context = useContext(TabsContext);
  if (!context) throw new Error('TabsContent must be used within Tabs');

  const { activeTab } = context;

  if (activeTab !== value) return null;

  return (
    <div className={`animate-fade-in ${className}`}>
      {children}
    </div>
  );
}
