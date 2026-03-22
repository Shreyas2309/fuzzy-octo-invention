import { useQuery } from '@tanstack/react-query';
import { getStats, getStatsHistory } from '../api/client';

export function useStats() {
  return useQuery({
    queryKey: ['stats'],
    queryFn: getStats,
    refetchInterval: 1000,
  });
}

export function useStatsHistory() {
  return useQuery({
    queryKey: ['stats-history'],
    queryFn: getStatsHistory,
    refetchInterval: 10000,
  });
}
