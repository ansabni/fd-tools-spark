#ifndef FD_H
#define FD_H

#include <iostream>
#include <string.h>
#include "FD_Quota.h"
#include "FD_stdspace.h"
#include "FD_stdcount.h"
#include "FD_stdtime.h"
#include "FD_QuickAdd.h"

using namespace std;

#define LINE_LENGTH 1024
#define SIZELEN  (1024*1024*100)

typedef enum {FD_REQUEST, FD_BYTE} FD_WEIGHT;

class FD {
  public: 
    FD() { strcpy(name,""); }
    char name[10] ;
    Stdtime stdtime;
    Stdspace stdspace;
    Stdcount stdcount;
    void setErrStrPtr(string *s) {
      stdtime.setErrStrPtr(s) ;
      stdspace.setErrStrPtr(s) ;
      stdcount.setErrStrPtr(s) ;
    }
    bool isEmpty() {return stdtime.isEmpty() ; }
    void createStdspaceStdcount(bool include1to2Transition=false); // stdtime has enough info to be able to create stdspace and stdcount from it.
    void createStdspaceStdcount(bool include1to2Transition, FD_Quota quota); // stdtime has enough info to be able to create stdspace and stdcount from it.
    static FD add(FD &fd1, FD &fd2) ;
    static FD scale(FD &fd1, double scale, int absoluteOrTraffic) ;
    static FD shard(FD &fd1, float n) ;
    static FD unshard(FD &fd1, float n) ;
    static FD_QuickAddResult qadd(FD &fd1, FD &fd2, long long c) ;
    static FD_QuickAddResult lookupQuickAddResult (FD &fd, long long c) ;

    static bool equalizeDurationsWhenAdding ;

    void setName (char *n) {
      strncpy(name, n, 10) ;
    }

    bool operator < (const FD& other) const {
	return stdtime < other.stdtime ;
    }
    bool operator > (const FD& other) const {
	return stdtime > other.stdtime ;
    }
} ;

#endif


