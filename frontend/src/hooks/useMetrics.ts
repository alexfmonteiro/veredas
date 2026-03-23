import { useQuery } from '@tanstack/react-query';
import { fetchMetrics, fetchHealth, getAfterDate, type MetricsResponse, type HealthResponse, type TimeRange } from '@/lib/api';

export function useMetrics(series: string, range: TimeRange = 'ALL') {
  const after = getAfterDate(range);

  return useQuery<MetricsResponse>({
    queryKey: ['metrics', series, range],
    queryFn: () => fetchMetrics(series, after),
    staleTime: 5 * 60 * 1000,
    retry: 2,
  });
}

export function useHealth() {
  return useQuery<HealthResponse>({
    queryKey: ['health'],
    queryFn: fetchHealth,
    staleTime: 60 * 1000,
    retry: 1,
  });
}
