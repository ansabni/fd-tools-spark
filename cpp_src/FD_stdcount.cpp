#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>
#include "FD.h"

//---------------------------------------------------------------------------
// stdcount:
// write and get functions
//---------------------------------------------------------------------------
int Stdcount::write(const char *stdcount_fname) {

  FILE *fp_stdcount;

  if ((fp_stdcount = fopen(stdcount_fname, "w")) == NULL) {
    fprintf(stderr, "fail to open %s to write\n", stdcount_fname);
    return 1;
  }

  fprintf(fp_stdcount, "# %lld %lld %d %lld %lld %d %lld\n", total_requests, total_served_bytes, duration, uniqueObjectCount, uniqueBytesize, peakRequests300s, peakBytes300s);
  for (map<int, stdcount_t>::iterator it = stdcount_array.begin() ; it != stdcount_array.end(); it++) {
    int count = it->first;
    stdcount_t &s = it->second;
    fprintf(fp_stdcount, "%d %lld %.1f %lld %.1f\n", 
            count, s.requests, s.requests*100.0/total_requests, s.bytes, s.bytes*100.0/total_served_bytes);
  }

  fclose(fp_stdcount);
  return 0;
}

stdcount_t
Stdcount::get(long long count) {
  map<int, stdcount_t>::iterator ret, first ;
  first = stdcount_array.begin();
  ret = stdcount_array.lower_bound(count);
  if (ret == stdcount_array.end()) {
    ret -- ;
  }
  if (ret->first>count && ret != first) ret--;
  return ret->second;
}



