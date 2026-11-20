interface StatusBadgeProps {
  status: string | null | undefined;
  size?: 'sm' | 'md';
}

const labels: Record<string, string> = {
  running: 'Running',
  completed: 'Completed',
  idle: 'Idle',
  failed: 'Failed',
  unknown: 'Unknown',
};

export function StatusBadge({ status, size = 'md' }: StatusBadgeProps) {
  const key = status && labels[status] ? status : 'unknown';
  return (
    <span className={`badge ${key} ${size === 'sm' ? 'sm' : ''}`}>
      {labels[key]}
    </span>
  );
}
