#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>
#include "FD.h"

#define MAX(x,y) (x>y?x:y)

bool FD::equalizeDurationsWhenAdding = false ;

void FD::createStdspaceStdcount(bool include1to2Transition) {
  FD_Quota quota ;
  createStdspaceStdcount(include1to2Transition, quota); 
}

void FD::createStdspaceStdcount(bool include1to2Transition, FD_Quota quota) {

  // In this function, we loop over iat in stdtime, and create prefix sums that are stdspace and stdtime
  long long size_sum = 0;
  long long count_sum = 0 ;
  long long request_sum = 0;
  long long byte_sum = 0;
  long long max_mb = 0 ;
  long long max_objcount = 0 ;

  // Phase 1
  stdspace.stdspace_array.clear(); 
  stdcount.stdcount_array.clear(); 
  for (map<int, stdtime_t>::iterator it = stdtime.stdtime_array.begin(); it != stdtime.stdtime_array.end(); it++){
    int delta_t = it->first;
    stdtime_t &st = it->second;
    request_sum += st.requests ;
    byte_sum += st.bytes ;
    int flag = 1;

    if (quota.inQuota(st.mb, st.objcount)) {
      stdspace.stdspace_array[st.mb].requests += st.requests ;
      stdspace.stdspace_array[st.mb].bytes += st.bytes;
      stdcount.stdcount_array[st.objcount].requests += st.requests;
      stdcount.stdcount_array[st.objcount].bytes += st.bytes;
      if (include1to2Transition) {
        stdspace.stdspace_array[st.mb].requests += st.Transition1to2_requests ;
        stdspace.stdspace_array[st.mb].bytes += st.Transition1to2_bytes;
        stdcount.stdcount_array[st.objcount].requests += st.Transition1to2_requests;
        stdcount.stdcount_array[st.objcount].bytes += st.Transition1to2_bytes;
      } 
      max_mb = st.mb>max_mb ? st.mb : max_mb ;
      max_objcount = st.objcount>max_objcount ? st.objcount : max_objcount ;
    } 
  }

  stdspace.total_requests = stdtime.total_requests;
  stdcount.total_requests = stdtime.total_requests;
  stdspace.total_served_bytes = stdtime.total_served_bytes ;
  stdcount.total_served_bytes = stdtime.total_served_bytes ;
  stdspace.duration = stdtime.duration;
  stdcount.duration = stdtime.duration;
  stdspace.max_stdspace_mb = max_mb ;
  stdcount.max_objcount = max_objcount ;
  stdspace.uniqueObjectCount = stdtime.uniqueObjectCount ;
  stdcount.uniqueObjectCount = stdtime.uniqueObjectCount ;
  stdspace.uniqueBytesize = stdtime.uniqueBytesize ;
  stdcount.uniqueBytesize = stdtime.uniqueBytesize ;
  stdspace.peakRequests300s = stdtime.peakRequests300s ; 
  stdspace.peakBytes300s = stdtime.peakBytes300s ; 
  stdcount.peakRequests300s = stdtime.peakRequests300s ; 
  stdcount.peakBytes300s = stdtime.peakBytes300s ; 

  // Phase 2
  request_sum = 0 ;
  byte_sum = 0 ;
  for(map<long long, stdspace_t>::iterator it = stdspace.stdspace_array.begin() ; it != stdspace.stdspace_array.end(); it++) {
    long long mb = it->first ;
    stdspace_t &st = it->second; 
    request_sum += st.requests ;
    byte_sum += st.bytes ;
    stdspace.stdspace_array[mb].requests = request_sum ;
    stdspace.stdspace_array[mb].bytes = byte_sum - stdtime.total_mib ;
  }
  request_sum = 0 ;
  byte_sum = 0 ;
  for(map<int, stdcount_t>::iterator it = stdcount.stdcount_array.begin() ; it != stdcount.stdcount_array.end(); it++) {
    int objcount = it->first ;
    stdcount_t &st = it->second; 
    request_sum += st.requests ;
    byte_sum += st.bytes ;
    stdcount.stdcount_array[objcount].requests = request_sum ;
    stdcount.stdcount_array[objcount].bytes = byte_sum - stdtime.total_mib ;
  }
}

// This operation adds two FDs with footprint addition algorithm
// To be used when the traffic of two footprint descriptors is mixed in one cache
FD 
FD::add(FD &fd1, FD &fd2) {

  FD ret ;
  int iat ;

  // First, look at time durations of the two FDs, and determine the duration scaling factor
  if (fd1.isEmpty()) {
    ret = fd2 ;
    return ret ;
  }
  if (fd2.isEmpty()) {
    ret = fd1 ;
    return ret ;
  }
  
  int maxDur = MAX(fd1.stdtime.duration, fd2.stdtime.duration) ;
  double tScale1 = 1.0 ;
  double tScale2 = 1.0 ;
  if (equalizeDurationsWhenAdding) {
    tScale1 = fd1.stdtime.duration*1.0/maxDur;
    tScale2 = fd2.stdtime.duration*1.0/maxDur;
  }
  // Get all iats.
  map<int, int> iatset ;
  for (map<int, stdtime_t>::iterator it = fd1.stdtime.stdtime_array.begin(); it != fd1.stdtime.stdtime_array.end(); it++) {
     iatset[it->first] = 1 ;
  } 
  for (map<int, stdtime_t>::iterator it = fd2.stdtime.stdtime_array.begin(); it != fd2.stdtime.stdtime_array.end(); it++) {
     iatset[it->first] = 1 ;
  }

  // Loop over iats.
  for(map<int, int>::iterator it = iatset.begin(); it != iatset.end(); it++) {
    stdtime_t x ;
    iat = it->first;
    //cerr << "delta_t = " << iat << ' ' ;
    stdtime_t st1 = fd1.stdtime.getMatching(iat);
    stdtime_t st2 = fd2.stdtime.getMatching(iat);
    x.mb = st1.mb + st2.mb;
    x.objcount = st1.objcount + st2.objcount ;
    x.requests = (long long) tScale1*st1.requests + tScale2*st2.requests ;
    x.bytes = (long long) tScale1*st1.bytes + tScale2*st2.bytes ;
    x.Transition1to2_bytes = (long long) tScale1*st1.Transition1to2_bytes + tScale2*st2.Transition1to2_bytes ;
    x.Transition1to2_requests = (long long) tScale1*st1.Transition1to2_requests + tScale2*st2.Transition1to2_requests ;
    //cerr << "(mr=0, requests=" << st1.requests << ", bytes=" << st1.bytes << ", stack-dist=" << st1.mb << ")" 
         //<< "(mr=1, requests=" << st2.requests << ", bytes=" << st2.bytes << ", stack-dist=" << st2.mb << ")" << endl 
         //<< "stdtime entry: " << iat << " " << x.mb << " " << x.requests << " " << x.bytes << endl ;
    if (x.requests == 0 && x.bytes == 0 && x.Transition1to2_bytes == 0 && x.Transition1to2_requests == 0 ) {
      // Empty entry. Don't bother
      continue ;
    } 
    ret.stdtime.stdtime_array[iat] = x ;
  }
  ret.stdtime.total_requests = tScale1*fd1.stdtime.total_requests + tScale2*fd2.stdtime.total_requests ;
  ret.stdtime.total_served_bytes = tScale1*fd1.stdtime.total_served_bytes + tScale2*fd2.stdtime.total_served_bytes ;
  ret.stdtime.total_mib = tScale1*fd1.stdtime.total_mib + tScale2*fd2.stdtime.total_mib ;
  ret.stdtime.duration = maxDur ;
  ret.stdtime.max_delta_t = iat ;
  ret.stdtime.uniqueObjectCount = fd1.stdtime.uniqueObjectCount + fd2.stdtime.uniqueObjectCount ; 
  ret.stdtime.uniqueBytesize = fd1.stdtime.uniqueBytesize + fd2.stdtime.uniqueBytesize ; 
  ret.stdtime.peakRequests300s = fd1.stdtime.peakRequests300s + fd2.stdtime.peakRequests300s ; 
  ret.stdtime.peakBytes300s = fd1.stdtime.peakBytes300s + fd2.stdtime.peakBytes300s ;

  ret.stdtime.makeMb2IatIndex(); 
  ret.stdtime.makeCumulative() ;
  return ret ;
}

FD 
FD::scale(FD &fd1, double scale, int absoluteOrTraffic ) {

  FD ret = fd1;
  ret.stdtime.scale(scale, absoluteOrTraffic);
  ret.createStdspaceStdcount();
  return ret ;
}

FD
FD::shard(FD &fd1, float n) {
  FD ret = fd1 ;
  ret.stdtime.shard(n) ;
  ret.createStdspaceStdcount();
  return ret ;
}


FD
FD::unshard(FD &fd1, float n) {
  FD ret = fd1 ;
  ret.stdtime.unshard(n) ;
  ret.createStdspaceStdcount();
  return ret ;
}


