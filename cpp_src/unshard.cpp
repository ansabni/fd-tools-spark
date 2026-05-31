#include <iostream>
#include <stdio.h>
#include "FD.h"

int main(int argc, char **argv) {

  if (argc<4) {
    cerr << "Usage: " << argv[0] << " <original stdtime input file pathname> <unshard factor> <unsharded stdtime output filename>" << endl;   
    return 1;
  }

  FD a ;
  float factor = 1.0 ;
  
  if (sscanf(argv[2], "%f", &factor) != 1) {
    cerr << "Unshard factor must be a real number" << endl ;
    return 1;
  }
     
  a.stdtime.read(argv[1]) ;
  a.stdtime.unshard(factor) ;
  a.stdtime.write(argv[3]) ;
  return 0 ;
}
