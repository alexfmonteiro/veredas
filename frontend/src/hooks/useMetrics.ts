import { useQuery } from '@tanstack/react-query';
import {
  fetchMetrics,
  fetchHealth,
  fetchInsightsLatest,
  fetchAnomalyInsights,
  fetchSyncStatus,
  fetchQualityLatest,
  fetchRunHistory,
  fetchQueryUsage,
  getAfterDate,
} from '@/lib/api';
import type {
  MetricsResponse,
  HealthResponse,
  InsightResponse,
  SyncStatusResponse,
  QualityLatest,
  RunHistoryResponse,
  QueryUsageResponse,
  TimeRange,
} from '@/lib/api';

export function useMetrics(series: string, range: TimeRange = 'ALL', groupBy?: string | null) {
  const after = getAfterDate(range);

  return useQuery<MetricsResponse>({
    queryKey: ['metrics', series, range, groupBy ?? null],
    queryFn: () => fetchMetrics(series, after, groupBy),
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

export function useInsights() {
  return useQuery<InsightResponse>({
    queryKey: ['insights', 'latest'],
    queryFn: fetchInsightsLatest,
    staleTime: 5 * 60 * 1000,
    retry: 2,
  });
}

export function useAnomalyInsights() {
  return useQuery<InsightResponse>({
    queryKey: ['insights', 'anomalies'],
    queryFn: fetchAnomalyInsights,
    staleTime: 5 * 60 * 1000,
    retry: 2,
  });
}

export function useSyncStatus() {
  return useQuery<SyncStatusResponse>({
    queryKey: ['sync-status'],
    queryFn: fetchSyncStatus,
    staleTime: 60 * 1000,
    retry: 1,
  });
}

export function useQualityLatest() {
  return useQuery<QualityLatest>({
    queryKey: ['quality', 'latest'],
    queryFn: fetchQualityLatest,
    staleTime: 60 * 1000,
    retry: 1,
  });
}

export function useRunHistory(limit?: number) {
  return useQuery<RunHistoryResponse>({
    queryKey: ['runs', limit],
    queryFn: () => fetchRunHistory(limit),
    staleTime: 60 * 1000,
    retry: 1,
  });
}

export function useQueryUsage() {
  return useQuery<QueryUsageResponse>({
    queryKey: ['query-usage'],
    queryFn: fetchQueryUsage,
    staleTime: 60 * 1000,
    retry: 1,
  });
}
