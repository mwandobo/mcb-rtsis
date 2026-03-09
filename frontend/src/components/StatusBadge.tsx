import { IconType } from 'react-icons';
import { FiCheckCircle, FiXCircle, FiLoader, FiPauseCircle, FiHelpCircle } from 'react-icons/fi';

interface StatusBadgeProps {
  status: string | null | undefined;
  size?: 'sm' | 'md';
}

const statusConfig: Record<string, { color: string; icon: IconType; label: string }> = {
  running: { color: 'bg-green-100 text-green-800', icon: FiLoader, label: 'Running' },
  idle: { color: 'bg-gray-100 text-gray-800', icon: FiPauseCircle, label: 'Idle' },
  failed: { color: 'bg-red-100 text-red-800', icon: FiXCircle, label: 'Failed' },
  unknown: { color: 'bg-gray-100 text-gray-800', icon: FiHelpCircle, label: 'Unknown' },
};

export function StatusBadge({ status, size = 'md' }: StatusBadgeProps) {
  const config = statusConfig[status || 'unknown'] || statusConfig.unknown;
  const Icon = config.icon;

  const sizeClasses = size === 'sm' ? 'px-2 py-0.5 text-xs' : 'px-3 py-1 text-sm';

  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full font-medium ${config.color} ${sizeClasses}`}>
      <Icon className={`${size === 'sm' ? 'w-3 h-3' : 'w-4 h-4'} ${status === 'running' ? 'animate-spin' : ''}`} />
      {config.label}
    </span>
  );
}