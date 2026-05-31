#include <stdlib.h>
#include <iostream>
#include "FD.h"
#include "FD_QuickAdd.h"


FD_QuickAddResult 
FD::lookupQuickAddResult (FD &fd, long long c) {

  FD_QuickAddResult ret ;
  fd.stdtime.activateMarker(c) ;
  //fd.stdtime.showMarkers(); 
  //cout << "Activated marker " << c << ", delta_t marker is " << fd.stdtime.delta_t_marker << endl ;
  std::map<int, stdtime_t>::iterator it = fd.stdtime.getEqualLower(fd.stdtime.delta_t_marker) ;
  stdtime_t &st = it->second ;
  ret.servedBytes = fd.stdtime.total_served_bytes ;
  ret.hitBytes = st.cum_bytes - fd.stdtime.total_mib ;
  ret.iat = it->first ;
  //cout << "Quckadd result is iat = " << ret.iat << endl ;
  return ret ; 
}

FD_QuickAddResult 
FD::qadd (FD &fd1, FD &fd2, long long c) {

  // Is one of them empty?
  if (fd1.isEmpty()) {
    return lookupQuickAddResult (fd2, c) ;
  } 
  if (fd2.isEmpty()) {
    return lookupQuickAddResult (fd1, c) ;
  } 
 
 // Both are non-empty 
 fd1.stdtime.activateMarker(c) ;
 fd2.stdtime.activateMarker(c) ;

  // Must have markers set
  if (fd1.stdtime.mb_marker==0 || fd2.stdtime.mb_marker==0) {
    if (fd1.stdtime.mb_marker == 0) {
      cout << "Missing marker for FD1 " << fd1.name << endl ;
      exit(1) ;
    } else {
      cout << "Missing marker for FD2 " << fd2.name << endl ;
      exit(1) ;
    }
    FD_QuickAddResult ret ;
    ret.hitBytes = -1 ;
    ret.servedBytes = -1 ;
    ret.iat = -1 ;  
    return ret ;
  }
  
  int pil = -1;
  int pir = -1; 
  int iat1 = fd1.stdtime.delta_t_marker ;
  int iat2 = fd2.stdtime.delta_t_marker ;
  int iatRight = MIN(iat1, iat2) ;

  int i1 = fd1.stdtime.findIAT(fd1.stdtime.mb_marker/2) ;
  int i2 = fd2.stdtime.findIAT(fd2.stdtime.mb_marker/2) ;
  int iatLeft = MIN(i1,i2) ;

  while (1) {

    int iatCenter = (iatLeft+iatRight)/2 ;
    long long std1 = fd1.stdtime.getMatching(iatCenter).mb ;
    long long std2 = fd2.stdtime.getMatching(iatCenter).mb ;
    long long stdCenter = std1 + std2; 

    if (iatLeft == pil && iatRight == pir) {
      // Arrived at the final point
      double b1 = fd1.stdtime.getMatching(iatCenter).cum_bytes - fd1.stdtime.total_mib;
      double b2 = fd2.stdtime.getMatching(iatCenter).cum_bytes - fd2.stdtime.total_mib;
      double totalB = fd1.stdtime.total_served_bytes+fd2.stdtime.total_served_bytes ;
      //cout << "Finished quickadd. IAT = " << iatCenter << ", fd1 hit bytes = " << b1 << ", fd2 hit bytes = " << b2 << ", served bytes = " << totalB << endl;
      FD_QuickAddResult ret ;
      ret.hitBytes = b1+b2 ;
      ret.servedBytes = totalB ;
      ret.iat = iatCenter ;
      return ret ;
    }
    else {
      pil = iatLeft ;
      pir = iatRight ;
    }

    if ( stdCenter > c ) {
      iatRight = iatCenter ;
    }
    else if ( stdCenter < c ) {
      iatLeft = iatCenter ;
    }
    else {
      // Finished search
      double b1 = fd1.stdtime.getMatching(iatCenter).cum_bytes - fd1.stdtime.total_mib;
      double b2 = fd2.stdtime.getMatching(iatCenter).cum_bytes - fd2.stdtime.total_mib;
      double totalB = fd1.stdtime.total_served_bytes+fd2.stdtime.total_served_bytes ;
      //cout << "Finished quickadd. IAT = " << iatCenter << ", fd1 hit bytes = " << b1 << ", fd2 hit bytes = " << b2 << ", served bytes = " << totalB << endl;
      FD_QuickAddResult ret ;
      ret.hitBytes = b1+b2 ;
      ret.servedBytes = totalB ;
      ret.iat = iatCenter ;
      return ret ;
    }
  }    
}
