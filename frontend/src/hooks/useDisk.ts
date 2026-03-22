import { useQuery } from '@tanstack/react-query';
import { getDisk } from '../api/client';

export function useDisk() {
  return useQuery({
    queryKey: ['disk'],
    queryFn: getDisk,
    refetchInterval: 2000,
  });
}
