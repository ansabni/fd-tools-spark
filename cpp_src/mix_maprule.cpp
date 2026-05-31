#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <libgen.h>
#include "FD_Calculus.h"

void print_usage(char *x) {
      fprintf(stderr,
              "Usage: %s [options...]\n"
              "Options:\n"
              "-c <config>     Configuration file name\n"
              "-I <Input Directory>  Directory to pick input FDs from\n"
              "-O <Output Directory>  Directory to write output FDs into\n"
              "-o <output>     Optional output filenme suffix, default stdspace.<mix>\n"
	      "-j <percent>    Thinning percentage jump, default no thinning\n",
              x );
}

int main(int argc, char **argv) {

  int opt;
  FD_Calculus_Config fcc; 
  int ok = 0;
  int haveConfig = 0 ;
  
  // First check the environment
  char *idir = getenv("MIX_MAPRULE_IDIR") ;
  if (idir != NULL) {
    fcc.setInputDirectory(idir) ;
  } 
  char *odir = getenv("MIX_MAPRULE_ODIR") ;
  if (odir != NULL) {
    fcc.setOutputDirectory(odir) ;
  } 
  char *suffix = getenv("MIX_MAPRULE_SUFFIX") ;
  if (suffix != NULL) {
    fcc.setAnswerSuffix(suffix) ;
  } 
  char *configFile = getenv("MIX_MAPRULE_CONFIG") ;
  if (configFile != NULL) {
    haveConfig = 1 ;
    ok = !(fcc.readCommands(configFile)) ;
  } 
  char *thinpct = getenv("MIX_MAPRULE_THINNING_PCT_JUMP") ;
  if (thinpct != NULL) {
    fcc.setThinningPctJump(atof(thinpct));
  } 

  configFile = getenv("mapreduce_map_input_file");
  if (configFile != NULL) {
    if (suffix==NULL) {
      string suff = string(basename(configFile)) ;
      fcc.setAnswerSuffix(suff);
    }
    // Read commands, as mapreduce has put the config file into stdin 
    haveConfig = 1 ;
    ok = !(fcc.readCommands()) ;
  }

  while ((opt = getopt(argc, argv, "c:o:I:O:j:h")) != -1) {
    switch (opt) {
      case 'c':
        haveConfig = 1 ;
        ok = !(fcc.readCommands(optarg));
        break;
      case 'o':
        fcc.setAnswerSuffix(optarg);
        break;
      case 'I': 
	fcc.setInputDirectory(optarg); 
        break ;
      case 'O':
	fcc.setOutputDirectory(optarg); 
        break ;
      case 'j':
	fcc.setThinningPctJump(atof(optarg)); 
        break ;
      case 'h':
      default:
        print_usage(argv[0]); 
        exit(1);
    }
  }
  if (!haveConfig) {
    print_usage(argv[0]);
    exit(1) ;
  }

  if (ok) {
   if (fcc.process()==0) {
    ok = fcc.saveAnswerFD();
   } 
  } else {
    cout << "Skipping processing" << endl ;
  }
  return 0 ;
}

