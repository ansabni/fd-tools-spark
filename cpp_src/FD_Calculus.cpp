#include <stdio.h>
#include <iostream>
#include <string.h>
#include <cstdlib>
#include <sstream>
#include "FD_Calculus.h"
#include "now.h"

using namespace std ;

void FD_Calculus_Config::addCommand(FD_Operation &command) {
  commands[numCommands] = command ;
  numCommands ++ ;
} 

int FD_Calculus_Config::parseACommandLine(char *s) {
  FD_Quota q; 
  FD_Operation fop; 
  char action[LINE_LENGTH];

  // Read the 7 fields on the line
  std::istringstream iss(s) ;
  iss >> fop.trafficClass;
  iss >> action;

  // Meaning of scale 
  // If it's just a number, then use it as an absolute scale
  // If it's a number followed by [GMK], then it says to scale to traffic volume of the given Gbps, Mbps, Kbps
  char sc[256];
  iss >> sc ;
  int sclen = strlen(sc);
  if (isalpha(sc[sclen-1])) {
    if (sc[sclen-1] == 'G') {
      sc[sclen-1] = '\0';
      fop.scale = atof(sc) * 1e9 ;
      fop.scaleType = FD_TRAFFICVOLUME ;
    } else 
    if (sc[sclen-1] == 'M') {
      sc[sclen-1] = '\0';
      fop.scale = atof(sc) * 1e6 ;
      fop.scaleType = FD_TRAFFICVOLUME ;
    } else 
    if (sc[sclen-1] == 'K') {
      sc[sclen-1] = '\0';
      fop.scale = atof(sc) * 1e3 ;
      fop.scaleType = FD_TRAFFICVOLUME ;
    } 
  }
  else {
      fop.scale = atof(sc) ;
      fop.scaleType = FD_ABSOLUTE ;
  }
  // See if there's shard info
  if (!iss.eof()) {
    string x ;
    char y[128] ;
    iss >> x ;
    stringstream s(x) ;
    s.getline(y, 128, '/') ;
    fop.numShards = atoi(y) ;
    s.getline(y, 128 , ' ') ;
    fop.totalShards = atoi(y) ;
  }
    
  // See if there's quota info
  if (!iss.eof()) iss >> fop.quota.max_quota_mb;
  if (!iss.eof()) iss >> fop.quota.min_quota_mb;
  if (!iss.eof()) iss >> fop.quota.max_quota_count;
  if (!iss.eof()) iss >> fop.quota.min_quota_count;
  
  if (strcasecmp(action, "add") == 0) { fop.operation = FD_ADD; }
  else if (strcasecmp(action, "sub") == 0) { fop.operation = FD_SUBTRACT; }
  else if (strcasecmp(action, "concat") == 0) { fop.operation = FD_CONCAT; }
  else {
    fprintf(stderr, "Bad action in config: %s\n", s);
    file_error_messages += "Bad action in config: " ;
    file_error_messages += s ;
    file_error_messages += "\n" ;
    return 1 ;
  }
  addCommand(fop) ;
  return 0 ;
}

int FD_Calculus_Config::readCommands() {
  // Reads commands from stdin
  char s[LINE_LENGTH];
  while (fgets(s, LINE_LENGTH, stdin)) {
    int x = parseACommandLine(s) ;
    if (x) {
      return 1 ;
    }
  }
  return 0 ;
}

int FD_Calculus_Config::readCommands(const char *fname) {

  FILE *fp ;
  char s[LINE_LENGTH]; 
  
  if ((fp = fopen(fname, "r")) == NULL) {
    fprintf(stderr, "fail to open %s to read\n", fname);
    file_error_messages += "Fail to open " ;
    file_error_messages += fname ;
    file_error_messages += " to read\n" ;
    return 1 ;
  }

  while ( fgets(s, LINE_LENGTH, fp) ) {
    if (s[0] == '#') { continue; }
    int x = parseACommandLine(s) ;
    if (x) {
      return 1 ;
    }
  }

  fclose(fp);
  return 0 ;
}

void FD_Calculus_Config::print() {
  for (int i = 0 ; i < numCommands ; i++ ) {
    cerr << "Command " << i << ": " << commands[i].trafficClass << " " 
         << commands[i].numShards << "/" << commands[i].totalShards << " shards, "
         << commands[i].operation << " " << commands[i].scale << endl ;
  }
}

FD 
FD_Calculus_Config::processSharding ( FD &fd, int numShards, int totalShards ) {

  // Do we have to, or is it a no-op
  if ( totalShards > 1 && numShards != totalShards ) {
    FD fd1 = FD::shard ( fd, totalShards ) ;
    if (numShards>1) {
      return FD::unshard ( fd1 , numShards ) ;
    } else {
      return fd1 ;
    }
  }
  else {
    return fd ;
  }
}

void FD_Calculus_Config::prepareFD ( FD_Operation &fdo, FD &ifd ) {
  if ( fdo.totalShards > 1 && fdo.numShards != fdo.totalShards ) {
    ifd = processSharding (ifd, fdo.numShards, fdo.totalShards ) ;
  }
  float scale = fdo.scale * ( fdo.operation == FD_SUBTRACT ? -1 : 1 ); 
  int stype = fdo.scaleType==FD_ABSOLUTE?0:1 ;
  ifd = FD::scale(ifd, scale, stype) ;
  
}

int FD_Calculus_Config::process() {

  if (numCommands==0) { return 1 ;}
  long long t1 = now();

  string dir = iDir ;

  // Read the FDs and perform operations
  bool readFirstValidFD = false ;
  for (int i=0; i != numCommands; i++) {
    FD nfd ;
    nfd.setErrStrPtr(&file_error_messages) ;
    FD_Operation &fdo = commands[i];
    string fname = dir + "/stdtime." + fdo.trafficClass ;

    // Read, Shard-unshard, and Scale
    if (nfd.stdtime.read(fname.c_str()) != 0) {
      // This stdtime cannot be read. Screw it, try next
      // return 1;
      file_error_messages += "Could not read from file " + fname + ", skipping it.\n" ; 
      fprintf (stderr, "Skipping file %s\n", fname.c_str()) ;
      continue ;
    } 
    prepareFD(fdo, nfd) ;

    if (!readFirstValidFD) {
      answer = nfd ;
      answer.setErrStrPtr(&file_error_messages) ;
      readFirstValidFD = true ;
      continue ;
    }
    
    if (fdo.operation == FD_SUBTRACT || fdo.operation == FD_ADD) {
      FD fd2 ;
      fd2.setErrStrPtr(&file_error_messages) ;
      fd2 = FD::add(answer, nfd); 
      answer = fd2;
      answer.setErrStrPtr(&file_error_messages) ;
    } 
  }
  long long t2 = now();
  long long dur1 = t2 - t1 ;
  cerr << "Time to process = " << dur1 << endl ;
  return 0 ;
}

int FD_Calculus_Config::saveAnswerFD() {
  int ret = 0;
  char fname[100] ;

  if (answer.isEmpty()) {
    file_error_messages +=  "Empty output, skipping\n"  ;
    cerr << "Empty output, skipping" << endl ;
  }
  else {
    sprintf(fname, "%s/stdtime.%s", oDir.c_str(), answerSuffix.c_str());
    ret = answer.stdtime.write(fname, pctJump) ;
  }
  return ret;
}
 
