#ifndef FD_STDSPACE_H
#define FD_STDSPACE_H

#include <map>
#include <set>
#include <string>
#include "FD_Quota.h"

using namespace std;


class stdspace_t {
  public: 
    stdspace_t() {clear();}
    void clear() {requests = bytes = 0 ; }
    long long requests;
    long long bytes;
} ;

class Stdspace {
  public:
    Stdspace() { clear(); }
    void clear() { 
      total_requests = total_served_bytes = max_stdspace_mb = 0 ;
      duration = 0 ;
      uniqueObjectCount = uniqueBytesize = 0 ;
      peakRequests300s = peakBytes300s = 0; 
      stdspace_array.clear(); 
      errStr = NULL ;
    }
    void setErrStrPtr(string *s) {
      errStr = s ;
    }
    void appendToErrStr (const char *s) {
      if (errStr) {
        (*errStr) += s  ;
      }
    }
    void appendToErrStr (const string &s) {
      if (errStr) {
        (*errStr) += s  ;
      }
    }

    int read(const char *fname) ;
    int write(const char *fname); 
    double getMaxHitrate() ;


    stdspace_t get(long long mb);
    double getByteHitratePct(long long mb) {
      stdspace_t x = get(mb) ;
      return x.bytes * 100.0 / total_served_bytes ;
    }
    map<long long, stdspace_t>::iterator getEqualLower(long long mb) ;

    map<long long, stdspace_t>::iterator
    getMiddle(map<long long, stdspace_t>::iterator first, map<long long, stdspace_t>::iterator last) ;

    long long findMB(double hitrate) ;

    void getAllKeys(set<long long> &keys) {
      for (map<long long, stdspace_t>::iterator it = stdspace_array.begin(); it != stdspace_array.end(); it++) {
        keys.insert(it->first) ;
      }
    }

    bool operator < (const Stdspace &other)  const {
      return this->stdspace_array.rbegin()->first < other.stdspace_array.rbegin()->first ;
    }

    bool operator > (const Stdspace &other)  const {
      return this->stdspace_array.rbegin()->first > other.stdspace_array.rbegin()->first ;
    }


    map<long long, stdspace_t> stdspace_array; // Indexed by mb
    long long total_requests ;
    long long total_served_bytes ;
    int duration ;
    long long max_stdspace_mb;

    // Unique footprint info
    long long uniqueObjectCount;
    long long uniqueBytesize ;
    
    // Peak traffic info: for 300s
    int peakRequests300s ;
    long long peakBytes300s ;

    // Append error strings to this string, if non-NULL
    // This string is owned by someone else, do not try to free it
    string *errStr ;

} ;

#endif



