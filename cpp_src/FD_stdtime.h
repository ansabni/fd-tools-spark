#ifndef FD_STDTIME_H
#define FD_STDTIME_H

#include <iostream>
#include <map>
#include <set>
#include <vector>
#include <string>
#include "FD_Quota.h"

using namespace std;

#define MIN(x,y) (x<y?x:y)

class stdtime_t {
  public: 
    stdtime_t() { clear(); }
    void clear() {mb = objcount = requests = bytes = cum_bytes = Transition1to2_bytes = Transition1to2_requests = 0;}
    void operator += (stdtime_t &s) {
     // We do not touch mb and objectcount in this function. We just add traffic numbers
     requests += s.requests ;
     bytes += s.bytes ;
     cum_bytes += s.cum_bytes ;
     Transition1to2_bytes += s.Transition1to2_bytes ; 
     Transition1to2_requests += s.Transition1to2_requests ;
    }
    long long mb;
    long long objcount;
    long long requests;
    long long bytes;
    long long cum_bytes;
    long long Transition1to2_bytes;
    long long Transition1to2_requests;
} ;

class Stdtime {
  public:
    Stdtime() {clear();}
    void clear() {
	total_requests = total_served_bytes = total_mib = 0 ; 
        duration = 0 ;
        max_delta_t = 0;
        max_mb = 0 ;
        uniqueObjectCount = uniqueBytesize = 0 ;
        mb_marker = 0 ;
        delta_t_marker =  0 ; 
        peakRequests300s = 0 ;
        peakBytes300s = 0 ;
        stdtime_array.clear();
        mb2iatIndex.clear(); 
        markerList.clear(); 
        mb_marker = 0 ; delta_t_marker = 0 ;
	errStr = NULL ;
        //fh = NULL ;
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
    bool isEmpty() {return (total_requests == 0) ; }
    bool isValid() { return !(stdtime_array.size() == 0 || total_requests <=0 || total_served_bytes <= 0 ) ;}
    int read(const char *fname, float scale = 1.0, int absoluteOrTraffic=0) ;
    int write(const char *fname, double pctJump = 0.0); 
    void makeCumulative(bool print=false) ;

    // get the entry for equal or lower value of iat
    std::map<int, stdtime_t>::iterator getEqualLower(int delta_t) ;
    std::map<int, stdtime_t>::iterator getEqualLower(int delta_t, FD_Quota quota) ;
    long long getEqualLowerMB(int delta_t) ;
    long long getEqualLowerMB(int delta_t, FD_Quota quota) ;

    // Get entry only if matching
    stdtime_t getMatching(int delta_t);
    stdtime_t getMatching(int delta_t, FD_Quota quota);

    // Get max IAT AND MB
    int getMaxIAT() {return max_delta_t; }
    long long getMaxMB() {return max_mb ;}

    // Find closest IAT for a given stack distance
    int findIAT(long long mb);
    std::map<int, stdtime_t>::iterator getMiddle(std::map<int, stdtime_t>::iterator first, std::map<int, stdtime_t>::iterator last);

    // Make Index
    void makeMb2IatIndex() ;
    set<int> findMatchingIAT(long long mb) ;
    int findEqualLowerIAT(long long mb) ;

    void getAllStackDistances(set<long long> &keys) {
      for (map<int, stdtime_t>::iterator it = stdtime_array.begin(); it != stdtime_array.end(); it++) {
        keys.insert((it->second).mb) ;
      }
    }

    // Marker business
    void activateMarker ( long long c ) {
      if ( markerList.find(c) != markerList.end() )  {
        mb_marker = c ;
        delta_t_marker = markerList[c] ;
      } else {
        mb_marker = 0 ;
        delta_t_marker = 0 ;
      }
    }

    void showMarkers() {
      for ( map<long long, int>::iterator it = markerList.begin() ; it != markerList.end(); it++ ) {
        cout << "Marker MB = " << it->first << ", iat = " << it->second << endl ;
      }
    }

    bool operator < (const Stdtime &other)  const {
      return total_served_bytes < other.total_served_bytes ;
      //return stdtime_array.rbegin()->second.mb*1.0/total_served_bytes < other.stdtime_array.rbegin()->second.mb*1.0/other.total_served_bytes ;
    }

    bool operator > (const Stdtime &other)  const {
      return total_served_bytes > other.total_served_bytes ;
      //return stdtime_array.rbegin()->second.mb*1.0/total_served_bytes > other.stdtime_array.rbegin()->second.mb*1.0/other.total_served_bytes ;
    }

    void scale(double scale, int absoluteOrTraffic=0) ;

    // Shard and unshard
    void shard(float n) ;
    void unshard(float n) ;
    
    // Get traffic info
    int getAvgHitsPerSecond() {return (int)(total_requests/duration);}
    int getPeakHitsPerSecond() {return (int)(peakRequests300s/300);}
    int getAvgGbps() {return total_served_bytes*8/duration/1e9;}
    double getPeakGbps() {return peakBytes300s*8/300/1e9;}

    //---------------------------------------------------------------------------
    // Some funny functions that meet specific needs
    //---------------------------------------------------------------------------
    
    // What to do if we wanted to take an FD computed for 4 days and turn it into an FD computed for 7days
    void stretchTimeDuration (int newDuration) ; 
    // Take a footprint, and imagine that it's split into N virtual footprints that have 1/Nth stack distance and 1/Nth traffic both
    void splitIntoVirtualFootprints (int num) ;

    map<int, stdtime_t> stdtime_array; // Indexed by delta_t
    map<long long, set<int> > mb2iatIndex ;
    long long total_requests ;
    long long total_served_bytes ;
    long long total_mib ;
    int duration ;
    int max_delta_t ;
    long long max_mb ;

    // Unique footprint info
    long long uniqueObjectCount;
    long long uniqueBytesize ;

    // All markers and active markers
    map<long long, int> markerList ;
    long long mb_marker;
    int delta_t_marker ;
    
    // Various and Default Markers
    static set<long long> defaultMarkerList ;

    // Peak traffic info: for 300s
    int peakRequests300s ;
    long long peakBytes300s ;
    
    // Append error strings to this string, if non-NULL
    // This string is owned by someone else, do not try to free it
    string *errStr ;
} ;



#endif


