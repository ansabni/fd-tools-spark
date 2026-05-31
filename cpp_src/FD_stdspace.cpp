#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>
#include "FD.h"

using namespace std ;

//---------------------------------------------------------------------------
// Stdsapce 
// write and get functions
//---------------------------------------------------------------------------
int Stdspace::write(const char *stdspace_fname) {

  FILE *fp_stdspace;
  char buf[1024*10];

  if (stdspace_array.size() == 0) {
    fprintf(stderr, "Cannot write stdspace to file %s. Stdspace is empty\n", stdspace_fname);
    return 1;
  }

  if ((fp_stdspace = fopen(stdspace_fname, "w")) == NULL) {
    fprintf(stderr, "fail to open %s to write\n", stdspace_fname);
    return 1;
  }

  sprintf(buf, "# %lld %lld %d %lld %lld %d %lld\n", total_requests, total_served_bytes, duration, uniqueObjectCount, uniqueBytesize, peakRequests300s, peakBytes300s);
  fprintf(fp_stdspace, "%s", buf) ;

  for (map<long long, stdspace_t>::iterator it = stdspace_array.begin(); it != stdspace_array.end(); it++) {
    long long mb = it->first ;
    stdspace_t &s = it->second;
    sprintf(buf, "%lld %lld %.1f %lld %.1f\n", 
            mb, s.requests, s.requests*100.0/total_requests, s.bytes, s.bytes*100.0/total_served_bytes);
    fprintf(fp_stdspace, "%s", buf) ;
  }

  fclose(fp_stdspace);

  //map<long long, stdspace_t>::iterator first = stdspace_array.begin();
  //map<long long, stdspace_t>::iterator last = --stdspace_array.end();
  //map<long long, stdspace_t>::iterator start = first ;
  //map<long long, stdspace_t>::iterator end = last ;
  //map<long long, stdspace_t>::iterator middle = getMiddle(first,last);
  //cout << "First MB is " << first->first << ", and bytes is " << first->second.bytes  << endl ;
  //cout << "Last MB is " << last->first << ", and bytes is " << last->second.bytes  << endl ;
  //cout << "Middle MB is " << middle->first << ", and bytes is " << middle->second.bytes << endl ;


  return 0;
}

stdspace_t
Stdspace::get(long long mb) {
  stdspace_t empty ;
  map<long long, stdspace_t>::iterator ret, first, last ;
  first = stdspace_array.begin();
  ret = stdspace_array.lower_bound(mb);
  if (ret == stdspace_array.end()) {
    // mb is beyond the end. 
    map<long long, stdspace_t>::reverse_iterator ri = stdspace_array.rbegin();
    return ri->second ;
    //return empty ;
    //ret--;
  }
  if (ret->first>mb && ret != first) ret--;
  return ret->second;
}

map<long long, stdspace_t>::iterator 
Stdspace::getEqualLower(long long mb) {
  map<long long, stdspace_t>::iterator ret ;
  map<long long, stdspace_t>::iterator first = stdspace_array.begin() ;
  map<long long, stdspace_t>::iterator last = stdspace_array.end() ;
  ret = stdspace_array.upper_bound(mb) ;

  if (ret == stdspace_array.end()) {
    map<long long, stdspace_t>::reverse_iterator rret = stdspace_array.rbegin() ;
    long long x = rret-> first ;
    ret = stdspace_array.find(x) ; 
  }
  if (ret->first>mb && ret != first) ret--;
  return ret;
}

map<long long, stdspace_t>::iterator
Stdspace::getMiddle(map<long long, stdspace_t>::iterator first, map<long long, stdspace_t>::iterator last) {
  int distance = std::distance(first,last);
  map<long long, stdspace_t>::iterator middle = first;
  std::advance(middle, distance/2);
  return middle;
}


double
Stdspace::getMaxHitrate() {
  if (total_served_bytes>0) {
    map<long long, stdspace_t>::reverse_iterator rit = stdspace_array.rbegin(); 
    stdspace_t &x = rit->second ;
    return x.bytes*100.0/total_served_bytes ;
  }
  else return 0 ;
}

long long 
Stdspace::findMB(double hitrate) {
  

  int found = 0 ;
  long long prev = - 1;
  map<long long, stdspace_t>::iterator first = stdspace_array.begin();
  map<long long, stdspace_t>::iterator last = --stdspace_array.end();

  // Convert hit rate to hit bytes
  long long hb = total_served_bytes * hitrate / 100.0 ;

  //cout << "hb = " << hb << endl ;

  // Looking for hit rate at or beyond the end?
  if (last->second.bytes <= hb) {
    return last->first ;  
  }
  
  // Binary search
  map<long long, stdspace_t>::iterator start = first ;
  map<long long, stdspace_t>::iterator end = last ;
  map<long long, stdspace_t>::iterator middle = getMiddle(first,last);
  

  //cout << "First MB is " << first->first << ", and bytes is " << first->second.bytes / 1e9 << endl ;
  //cout << "Middle MB is " << middle->first << ", and bytes is " << middle->second.bytes /1e9 << endl ;
  for ( ; middle != first && middle != last; ) {
    long long bytes = middle->second.bytes ;
    if ( bytes == hb || bytes == prev ) {
      // End of the loop
      found = 1;
      break;
    }
    else if ( bytes < hb ) {
      start = middle ;
    } else {
      end = middle ;
    }
    middle = getMiddle(start,end);
    //cout << "New middle MB is " << middle->first << ", and bytes is " << middle->second.bytes << endl ;
    prev = bytes ;
  }
  
  if (found) return middle->first ;
  long long hb1 = start->second.bytes ;
  long long hb2 = end->second.bytes ;
  if ( hb1-hb <= hb2-hb ) return start->first ;
  return end->first ;
 
}



