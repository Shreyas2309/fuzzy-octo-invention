import { useQuery } from '@tanstack/react-query';
import { getMem } from '../api/client';

export function useMem() {
  return useQuery({
    queryKey: ['mem'],
    queryFn: getMem,
    refetchInterval: 1000,
  });
}
