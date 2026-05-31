#include <cstddef>
#include <sys/time.h>

long long now() {

  struct timeval tv ;
  gettimeofday (&tv,NULL) ;
  return tv.tv_sec*1000000 + tv.tv_usec ;

}

