#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>
#include <math.h>
#include "FD.h"
#include "now.h"

#define MAX(x,y) (x>y?x:y)

set<long long> Stdtime::defaultMarkerList ;


//---------------------------------------------------------------------------
// Stdtime read, write and other functions
//---------------------------------------------------------------------------
int Stdtime::read(const char *fname, float scale, int absoluteOrtraffic) {

  char s[LINE_LENGTH];
  FILE *rfp;
  int delta_t  ;
  long long mb;
  long long objcount, requests, bytes, Transition1to2_bytes, Transition1to2_requests;
  long long cum_bytes = 0 ;
  int lastIAT = 0 ;

  if (scale==0.0) {
    const char *s = "Scale of 0 is not supported\n" ;
    fprintf(stderr, "%s", s) ;
    appendToErrStr(s) ;
    return 1;
  }

  clear() ;
  long long t1 = now();

  // Open local file
  if (!fname) {
    rfp = stdin ;
  } else if ((rfp = fopen(fname, "r")) == NULL) {
    fprintf(stderr, "Fail to open %s to read\n", fname);
    appendToErrStr ("Fail to open ") ;
    appendToErrStr (fname) ;
    appendToErrStr (" to read\n") ;
    return 1;
  }

  // Get first line. It should start with a #
  s[0] = '\0' ;
  fgets(s, LINE_LENGTH, rfp);
  // Check if the file is zero length
  if (strlen(s)==0) {
    fprintf(stderr, "%s is empty\n", fname);
    appendToErrStr (fname) ;
    appendToErrStr (" is empty\n") ;
    fclose(rfp);
    return 1 ;
  }
  
  //fprintf(stderr, "Read the first line %s\n", s) ;
  total_requests = 0 ;
  total_served_bytes = 0 ;
  peakRequests300s = 0 ;
  peakBytes300s = 0 ;
  duration = 0 ;
  total_mib = 0 ;
  
  if (s[0] != '#' || 
      sscanf(s+1, "%lld%lld%d%lld%lld%lld%d%lld", &total_requests, &total_served_bytes, &duration, &total_mib, &uniqueObjectCount, &uniqueBytesize, &peakRequests300s, &peakBytes300s) != 8) {
        if (sscanf(s+1, "%lld%lld%d%lld%lld%lld", &total_requests, &total_served_bytes, &duration, &total_mib, &uniqueObjectCount, &uniqueBytesize) != 6) {
          if (s[0] != '#' || sscanf(s+1, "%lld%lld%d%lld", &total_requests, &total_served_bytes, &duration, &total_mib) != 4) {
            if (s[0] != '#' || sscanf(s+1, "%lld%lld%d", &total_requests, &total_served_bytes, &duration) != 3) {
              fprintf(stderr, "Fail to parse line in %s: %s\n", fname, s);
              appendToErrStr ("Fail to parse line in ") ;
              appendToErrStr (fname) ;
              appendToErrStr (": ") ;
              appendToErrStr (s);
              appendToErrStr ("\n") ;
              return 1;
            }
          }
      }
  }

  if (total_requests <=0 || total_served_bytes <= 0) {
    char bb [1024] ;
    sprintf(bb, "%s reports zero/negative traffic: %lld requests, %lld bytes\n", fname, total_requests, total_served_bytes);
    fprintf (stderr, "%s", bb) ;
    appendToErrStr (bb) ;
    fclose(rfp);
    return 1 ;
  }

  if (absoluteOrtraffic != 0) {
    double bps = total_served_bytes*8.0/duration ;
    scale = scale/bps ;
    //cerr << "Translating traffic scale to absolute scale of " << scale << endl ;
  }
  total_requests *= scale ;
  total_served_bytes *= scale ;
  total_mib *= scale ;
  peakRequests300s *= scale ;
  peakBytes300s *= scale ;

  // Read the rest of the file
  //fprintf(stderr, "reading %s ...\n", fname);
  long long prevmb = 0 ;
   
  // Set the markers iterator
  set<long long>::iterator mit = defaultMarkerList.begin () ; 
  long long currMarkerC = *mit ;

  while ( fgets(s, LINE_LENGTH, rfp) ) {
    //fprintf(stderr, "Read a line %s, with length %d\n", s, strlen(s)) ;
    //if (strlen(s) == 1 ) {
      //fprintf(stderr, "one-char line has value %d\n", (int)s[0]) ;
    //}
    if (sscanf(s, "%d%lld%lld%lld%lld%lld%lld", &delta_t, &mb, &objcount, &requests, &bytes, &Transition1to2_bytes, &Transition1to2_requests) != 7) {
      char bb[1024] ;
      sprintf(bb, "Fail to parse line in %s: %s\n", fname, s);
      fprintf(stderr, "%s", bb) ;
      appendToErrStr(bb) ;
      return 1;
    }
    cum_bytes += (scale*bytes) ;
    delta_t = (int) (delta_t * scale) ;
    stdtime_array[delta_t].mb = mb ;
    stdtime_array[delta_t].objcount = objcount ;
    stdtime_array[delta_t].requests += (scale*requests) ;
    stdtime_array[delta_t].bytes += (scale*bytes) ;
    stdtime_array[delta_t].cum_bytes += cum_bytes ;
    stdtime_array[delta_t].Transition1to2_bytes += (scale*Transition1to2_bytes) ;
    stdtime_array[delta_t].Transition1to2_requests += (scale*Transition1to2_requests) ;

    // Update max 
    if ( delta_t > max_delta_t ) max_delta_t = delta_t ;
    if ( mb > max_mb ) max_mb = mb ;

    // Update the markers, as necessary
    if (currMarkerC < mb && markerList.find(currMarkerC) == markerList.end() ) {
      markerList[currMarkerC] = delta_t ;
      lastIAT = delta_t ;
      //cerr << "currMarkerC = " << currMarkerC << ", mb = " << mb << ", setting iat = " << delta_t << endl ;
      if (mit != defaultMarkerList.end() ) {
        mit ++ ;
      }
      if (mit != defaultMarkerList.end() ) { 
        currMarkerC = *mit ;
      }
    }
  }

  if (stdtime_array.size() == 0) {
    fprintf(stderr, "%s has header line but no body. Invalid FD\n", fname) ;
    appendToErrStr(fname) ;
    appendToErrStr(" has header line but no body. Invalid FD\n") ;
    fclose(rfp);
    return 1 ;
  }

  fclose(rfp);
  //max_delta_t = delta_t ;

  // Remaining markers in the default list are too big for this FD. Set them to the last iat
  markerList.clear(); 
  for (set<long long>::iterator mit = defaultMarkerList.begin () ; mit != defaultMarkerList.end() ; mit++ ) {
    long long m = *mit ;
    if ( markerList.find(m) == markerList.end() ) {
      markerList[m] = delta_t ;
      //cerr << "currMarkerC = " << m << ", mb = " << mb << ", setting iat = " << delta_t << endl ;
    }
  }

  long long t2 = now();
  long long dur1 = t2 - t1 ;
  //cerr << "Time to read stdtime from " << fname << " is "  << dur1 << endl ;
  return 0;
}

void Stdtime::scale(double scale, int absoluteOrTraffic) {
  map<int, stdtime_t> x ;
  if (absoluteOrTraffic != 0) {
    double myBps = total_served_bytes*8/duration ;
    scale = scale/myBps ;
  }

  total_requests *= scale ;
  total_served_bytes *= scale ;
  total_mib *= scale ;
  peakRequests300s *= scale ;
  peakBytes300s *= scale ;

  for (map<int, stdtime_t>::iterator it = stdtime_array.begin() ; it != stdtime_array.end(); it++) {
    int delta_t = (it->first)/fabs(scale) ;
    stdtime_t xx = it->second;
    if (x.find(delta_t) != x.end()) {
      // New entry
      xx.requests *= scale ;
      xx.bytes *= scale ;
      xx.cum_bytes *= scale ;
      xx.Transition1to2_bytes *= scale ;
      xx.Transition1to2_requests *= scale ;
      x[delta_t] = xx ;
    }
    else {
      // Add fields to existing entry
      x[delta_t].mb = MAX(x[delta_t].mb, xx.mb);
      x[delta_t].objcount = MAX(x[delta_t].objcount, xx.objcount);
      x[delta_t].requests += scale * xx.requests ;
      x[delta_t].bytes += scale * xx.bytes ;
      x[delta_t].cum_bytes += scale * xx.cum_bytes ;
      x[delta_t].Transition1to2_bytes += scale * xx.Transition1to2_bytes ;
      x[delta_t].Transition1to2_requests += scale * xx.Transition1to2_requests ;
    }
  }
  stdtime_array.clear(); 
  stdtime_array = x ;

  makeMb2IatIndex() ;  
}

void Stdtime::shard(float n) {
  
  map<int, stdtime_t> x ;

  if (n==0) return ;

  // Divide traffic and cache space by n. Keep IAT untouched
  total_requests /= n ;
  total_served_bytes /= n ;
  total_mib /= n ;
  uniqueObjectCount /= n ;
  uniqueBytesize /= n ; 
  peakRequests300s /= n ;
  peakBytes300s /= n ;
  
  for (map<int, stdtime_t>::iterator it = stdtime_array.begin() ; it != stdtime_array.end(); it++) {
    int delta_t = it->first ;
    stdtime_t xx = it->second ;
    xx.mb /= n ;
    xx.objcount /= n ;
    xx.requests /= n ;
    xx.bytes /= n ;
    xx.cum_bytes /= n ;
    xx.Transition1to2_bytes /= n ;
    xx.Transition1to2_requests /= n ;
    x[delta_t] = xx ;
  }  
  stdtime_array.clear(); 
  stdtime_array = x ;
  makeMb2IatIndex() ;  
}

void Stdtime::unshard(float n) {
  
  map<int, stdtime_t> x ;

  if (n==0) return ;

  // Divide traffic and cache space by n. Keep IAT untouched
  total_requests *= n ;
  total_served_bytes *= n ;
  total_mib *= n ;
  uniqueObjectCount *= n ;
  uniqueBytesize *= n ; 
  peakRequests300s *= n ;
  peakBytes300s *= n ;
  
  // TBD: figure out how to update mb_marker and delta_t_marker

  for (map<int, stdtime_t>::iterator it = stdtime_array.begin() ; it != stdtime_array.end(); it++) {
    int delta_t = it->first ;
    stdtime_t xx = it->second ;
    xx.mb *= n ;
    xx.objcount *= n ;
    xx.requests *= n ;
    xx.bytes *= n ;
    xx.cum_bytes *= n ;
    xx.Transition1to2_bytes *= n ;
    xx.Transition1to2_requests *= n ;
    x[delta_t] = xx ;
  }  
  stdtime_array.clear(); 
  stdtime_array = x ;
  makeMb2IatIndex() ;  
}

int Stdtime::write(const char *stdtime_fname, double pctJump) {
  
  FILE *fp_stdtime;
  char buf[1024*10];
  /*
  cerr << "Opening " << stdtime_fname << " to write on " ;
  if (fh) {
    cerr << "hdfs" << endl ; 
  } else {
    cerr << "localfs" << endl ; 
  }
  */

  // Skip if empty or invalid
  if (isEmpty()) {
    fprintf(stderr, "Skipping writing an empty stdtime\n") ;
    appendToErrStr("Skipping writing an empty stdtime\n") ;
    return 0 ;
  }
  if (!isValid()) {
    char bb[1024] ;
    sprintf(bb, "Skipping writing an invalid stdtime file (%lld requests, %lld bytes, %d body lines)\n", 
                     total_requests, total_served_bytes, (int)stdtime_array.size() ) ;
    fprintf(stderr, "%s", bb) ;
    appendToErrStr(bb) ;
    return 1 ;
  }

  if ((fp_stdtime = fopen(stdtime_fname, "w")) == NULL) {
    fprintf(stderr, "fail to open %s to write\n", stdtime_fname);
    appendToErrStr("fail to open ") ;
    appendToErrStr(stdtime_fname) ;
    appendToErrStr(" to write\n") ;
    return 1;
  }

  sprintf(buf, "# %lld %lld %d %lld %lld %lld %d %lld\n", total_requests, total_served_bytes, duration, total_mib, uniqueObjectCount, uniqueBytesize, peakRequests300s, peakBytes300s);
  fprintf(fp_stdtime, "%s", buf) ;
  //fprintf(stderr, "Wrote header to file %s\n", stdtime_fname);

  // Thinning related stuff
  // Requests, bytes, 1-2Transition Requests, 1-2Transition Bytes, cumulative sum since t=0
  long long R, B, TR, TB ;
  R = B = TR = TB = 0 ;

  // For the rows that were not printed due to thinning, how much was skipped
  long long skippedR, skippedB, skipped_transition_bytes, skipped_transition_requests ;
  skippedR = skippedB = skipped_transition_bytes = skipped_transition_requests = 0 ;

  // Pct increment since last written line, for each of delta-t, stack-distance-bytes, 
  // stack-distance-objects, request hit rate, byte hit rate
  double dtinc, stdinc, stdoinc, rhrinc, bhrinc ;
  dtinc = stdinc = stdoinc = rhrinc = bhrinc = 0 ;

  // The values written on the previous printed line for delta-t, stack-distance-bytes, stack-distance-objects, Requests, Bytes, 
  // 1-2Transition Requests, 1-2Transition Bytes, Request hit rate, byte hit rate
  long long pdt, pstd, pstdo;
  double prhr, pbhr ;
  pdt = pstd = pstdo = 0;
  prhr = pbhr = 0 ;

  // Must print the first line and the last line. 
  int firstTime = 1 ;

  // Last known timestamp with valid dataA
  int lkt ;

  // Last known stack distance and object count
  long long lks, lko ;

  for (map<int, stdtime_t>::iterator it = stdtime_array.begin() ; it != stdtime_array.end(); it++) {
    int delta_t = it->first;
    stdtime_t &s = it->second;

    lkt = delta_t;
    lks = s.mb;
    lko = s.objcount;

    // Compute hit rates based on cumulative info since the beginning
    R += s.requests ; 
    B += s.bytes ;  
    TB += s.Transition1to2_bytes ;
    TR += s.Transition1to2_requests; 
    double rhr = R * 100.0 /  total_requests ;
    double bhr = (B - total_mib) * 100.0 /  total_served_bytes ;

    // Compute the % differences from the previously written line (if any)
    if (firstTime) {
      // Nothing written before. Initialize to indicate 100% change.
      dtinc = 100 ;
      stdinc = 100 ;     
      stdoinc = 100 ; 
      rhrinc = 100 ;
      bhrinc = 100 ;
    } else {
      dtinc = (delta_t-pdt)*100.0/pdt ;
      stdinc = (s.mb-pstd)*100.0/pstd ;
      stdoinc = (s.objcount-pstdo)*100.0/pstdo ;  
      rhrinc = (rhr-prhr)*100.0/prhr ;
      bhrinc = fabs((bhr-pbhr)*100.0/pbhr) ;
    }

    //if (dtinc>=pctJump||stdinc>=pctJump||stdoinc>=pctJump||rhrinc>=pctJump||bhrinc>=pctJump) {
    if (fabs(dtinc)>=pctJump||fabs(stdinc)>=pctJump) {
      sprintf (buf, "%d %lld %lld %lld %lld %lld %lld\n",
	       delta_t, s.mb, s.objcount, s.requests+skippedR, s.bytes+skippedB, s.Transition1to2_bytes+skipped_transition_bytes, s.Transition1to2_requests+skipped_transition_requests) ;
      fprintf(fp_stdtime, "%s", buf) ;
      firstTime = 0 ;
      skippedR = skippedB = skipped_transition_bytes = skipped_transition_requests = 0 ;
      pdt = delta_t ; pstd = s.mb; pstdo = s.objcount; prhr = rhr ; pbhr = bhr ;
    } else {
      // Skip it
      skippedR += s.requests;
      skippedB += s.bytes;
      skipped_transition_requests += s.Transition1to2_requests;
      skipped_transition_bytes += s.Transition1to2_bytes;
    }
  }

  // Check if the last line should be written
  if (lkt!=pdt && skippedR >0) {
    sprintf(buf, "%d %lld %lld %lld %lld %lld %lld\n", 
            lkt, lks, lko,skippedR, skippedB, skipped_transition_bytes, skipped_transition_requests);
    fprintf(fp_stdtime, "%s", buf) ;
  }

  //fprintf(stderr, "Wrote lines to file %s\n", stdtime_fname);
  fclose(fp_stdtime);
  
  //fprintf(stderr, "Closed file %s upon writing\n", stdtime_fname);
  return 0;
}

map<int, stdtime_t>::iterator
Stdtime::getMiddle(map<int, stdtime_t>::iterator first, map<int, stdtime_t>::iterator last) {
  int distance = std::distance(first,last);
  map<int, stdtime_t>::iterator middle = first;
  std::advance(middle, distance/2);
  return middle;
}

stdtime_t Stdtime::getMatching(int delta_t) {
  FD_Quota q;   // Empty
  return getMatching(delta_t, q);
}

stdtime_t Stdtime::getMatching(int delta_t, FD_Quota quota) {
  map<int, stdtime_t>::iterator it = getEqualLower(delta_t, quota);
  stdtime_t st = it->second; 
  if (it->first != delta_t) {
    st.requests = 0 ;
    st.bytes = 0 ;
    st.Transition1to2_bytes = 0 ;
    st.Transition1to2_requests = 0 ;
  }
  return st ;
}

map<int, stdtime_t>::iterator
Stdtime::getEqualLower(int delta_t) {
  FD_Quota q; 	// Empty
  return getEqualLower(delta_t, q);
}


map<int, stdtime_t>::iterator
Stdtime::getEqualLower(int delta_t, FD_Quota quota) {
  // Get delta_t equal or lower to what was requested
  map<int, stdtime_t>::iterator ret;
  map<int, stdtime_t>::iterator first = stdtime_array.begin();
  map<int, stdtime_t>::iterator last = --stdtime_array.end();
  ret = stdtime_array.upper_bound(delta_t);
  if (ret == stdtime_array.end() ){ 
    ret -- ;
  }
  if (ret->first>delta_t && ret != first) ret--;

  // If there's max quota, adjust for that
  if (quota.max_quota_mb || quota.max_quota_count) {
    for (; ret != first ; ret--) {
      if ((ret->second).mb<=quota.max_quota_mb && (ret->second).objcount<=quota.max_quota_count) break;
    } 
  }
  if (quota.min_quota_mb||quota.min_quota_count) {
    for(; ret != last; ret++){
      if((ret->second).mb>=quota.min_quota_mb && (ret->second).objcount>=quota.min_quota_count) break;
    }
  }  
  
  return ret;
}

long long 
Stdtime::getEqualLowerMB(int delta_t) {
  map<int, stdtime_t>::iterator it = getEqualLower(delta_t);
  if (it!=stdtime_array.end()) {
    //Valid value
    return it->second.mb ;
  }
  else {
    return 0 ;
  }
}

long long 
Stdtime::getEqualLowerMB(int delta_t, FD_Quota quota) {
  map<int, stdtime_t>::iterator it = getEqualLower(delta_t, quota);
  if (it!=stdtime_array.end()) {
    //Valid value
    return it->second.mb ;
  }
  else {
    return 0 ;
  }
}
 

int 
Stdtime::findIAT(long long mb) {

  int found = 0 ;
  int pm = - 1;
  map<int , stdtime_t>::iterator first = stdtime_array.begin();
  map<int, stdtime_t>::iterator last = --stdtime_array.end();
  
  // Lookign for stack disance beyond the end of this stdtime?
  if ( (last->second).mb <= mb ) {
    return last->first ;
  }

  map<int, stdtime_t>::iterator start = first ;
  map<int, stdtime_t>::iterator end = last ;

  map<int, stdtime_t>::iterator middle = getMiddle(first,last);

  for ( ; middle != first && middle != last; ) {
    long long m = (middle->second).mb; 
    if (m == mb || m == pm) {
      found = 1;
      break;
    }
    else if (m<mb) {
      start = middle ;
    }
    else {
      end = middle ;
    }
    middle = getMiddle(start,end);
    pm = m ;
  }

  if (found) return middle->first ; 
  long long mb1 = (start->second).mb;  
  long long mb2 = (end->second).mb;  
  if (mb1-mb <= mb2-mb) return start->first;
  else return end->first;
  
}

// Make index and also update markers
void Stdtime::makeMb2IatIndex() {
  int iat  ;
  long long mb ;

  mb2iatIndex.clear() ;
  markerList.clear() ; 

  set<long long>::iterator mit = defaultMarkerList.begin () ;
  long long currMarkerC = *mit ;

  for (map<int, stdtime_t>::iterator it = stdtime_array.begin() ; it != stdtime_array.end(); it++) {
    iat = it->first ;
    mb = (it->second.mb) ;
    (mb2iatIndex[mb]).insert(iat);
    if (mit != defaultMarkerList.end() && currMarkerC < mb && markerList.find(currMarkerC) == markerList.end() ) {
      //cerr << "MakerList[" << currMarkerC << "] = " << iat << endl ;
      markerList[currMarkerC] = iat ;
      mit ++ ;
      if (mit != defaultMarkerList.end() ) {
        currMarkerC = *mit ;
      }
    }
  }

  // Remaining markers
  for (set<long long>::iterator mit = defaultMarkerList.begin () ; mit != defaultMarkerList.end() ; mit++ ) {
    long long m = *mit ;
    if ( markerList.find(m) == markerList.end() ) {
      markerList[m] = iat ;
    }
  }
}

set<int>
Stdtime::findMatchingIAT(long long mb) {
  if (mb2iatIndex.find(mb) != mb2iatIndex.end()) {
    return mb2iatIndex[mb] ;
  } else {
    set<int> ret ;
    return ret ;
  }
}

void Stdtime::makeCumulative(bool print) {

  long long cum_bytes = 0 ;
  for (map<int, stdtime_t>::iterator it = stdtime_array.begin() ; it != stdtime_array.end(); it++) {
    stdtime_t &s = it->second;
    cum_bytes += s.bytes ;
    s.cum_bytes = cum_bytes ;
    if (print) { 
      cerr << it->first << ", " << it->second.cum_bytes << endl ;
    }
  }
}

void Stdtime::stretchTimeDuration (int newDuration) {
  float tscale = newDuration*1.0/duration ;
  for (map<int, stdtime_t>::iterator it = stdtime_array.begin() ; it != stdtime_array.end(); it++) {
    stdtime_t &s = it->second;
    s.requests *= tscale ;
    s.bytes *= tscale ;
  }
  total_requests *= tscale ;
  total_served_bytes *= tscale ;
  total_mib *= tscale ;
  peakRequests300s *= tscale; 
  peakBytes300s *= tscale ;
  duration = newDuration ;
  
}

// Take a footprint, and imagine that it's split into N virtual footprints that have 1/Nth stack distance and 1/Nth traffic both
// What is left in this stdtime is 1/Nth of the original stdtime
void Stdtime::splitIntoVirtualFootprints (int num) {

  map<int, stdtime_t> x ;
  for (map<int, stdtime_t>::iterator it = stdtime_array.begin() ; it != stdtime_array.end(); it++) {
    int iat = (it->first)*num ;
    //cerr << "iat = " << iat << endl ;
    stdtime_t s = it->second;
    stdtime_t s1;
    if (x.find(iat) != x.end()) {
      //cerr << "Dupl" << endl ;
      s1 = x[iat];
    }
    s1.requests += s.requests/num ;
    s1.bytes += s.bytes/num ;
    s1.Transition1to2_bytes += s.Transition1to2_bytes/num ;
    s1.Transition1to2_requests += s.Transition1to2_requests/num ;
    s1.mb = MAX(s.mb/num,s1.mb) ;
    s1.objcount = MAX(s.objcount/num,s1.objcount) ;
    x[iat] =  s1 ;
  }
  stdtime_array.clear();
  stdtime_array = x ;
  total_requests /= num ;
  total_served_bytes /= num ;
  total_mib /= num ;
  peakRequests300s /= num ;
  peakBytes300s /= num ;
}

