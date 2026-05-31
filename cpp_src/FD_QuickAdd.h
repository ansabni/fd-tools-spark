#ifndef FD_QUICKADD_H
#define FD_QUICKADD_H

#include <utility>

class FD_QuickAddResult {
  public:
    FD_QuickAddResult() {
      hitBytes = servedBytes = iat = 0 ;
    }
  long long hitBytes ;
  long long servedBytes ;
  int iat ;
} ;

#endif


