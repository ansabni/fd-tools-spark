#ifndef FD_STDCOUNT_H 
#define FD_STDCOUNT_H

#include <map>
#include <string>
#include "FD_Quota.h"

using namespace std;

class stdcount_t {
  public:
    stdcount_t() {clear();}
    void clear() {requests = bytes = 0; }
    long long requests;
    long long bytes;
} ;

class Stdcount {
  public:
    Stdcount() { clear();} 
    void clear() {
      total_requests = total_served_bytes =  0 ;
      duration = 0 ;
      max_objcount = uniqueObjectCount = uniqueBytesize = 0 ;
      peakRequests300s = peakBytes300s = 0; 
      stdcount_array.clear(); 
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
    stdcount_t get(long long objcount);


    map<int, stdcount_t> stdcount_array; // Indexed by objcount
    long long total_requests ;
    long long total_served_bytes ;
    int duration ;
    long long max_objcount;

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
