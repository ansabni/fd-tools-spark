#include <iostream>
#include <libgen.h>
#include "FD.h"

int main(int argc, char **argv) {

  if (argc < 2) {
    cerr << "Usage: " << argv[0] << " [-icp] <stdtime input file> <stdcount output file>" << endl;
    return 1;
  }

  FD a ;
  if (!strcmp(argv[1], "-icp") ) {
    a.stdtime.read(argv[2]) ;
    a.createStdspaceStdcount(true) ;
    a.stdcount.write(argv[3]) ;
  } else {
    a.stdtime.read(argv[1]) ;
    a.createStdspaceStdcount(false) ;
    a.stdcount.write(argv[2]) ;
  }
}
