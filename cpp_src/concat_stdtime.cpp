#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <vector>
#include <iostream>
#include <sstream>
#include <climits>
#include "FD_Calculus.h"

#define MAXNAMELEN 256

char answersuffix[MAXNAMELEN] ;
int cmax = 4*1024*1024 ;
char *iDir ;
char *oDir ;
char *suffix ;
int  minGhost = 1 ;
bool weighByTraffic = false ;

void print_usage(char *x) {
      fprintf(stderr,
              "Usage: %s [options...]\n"
              "Options:\n"
              "-I: <Input Directory>\n"
              "-O: <Output Directory>\n"
              "-c <config>     Configuration file name\n"
              "-o <output>     Optional output filenme suffix, default stdspace.<mix>\n"
              "-n <minghost>   Optional minghost count. Default 1\n"
              "-m optional max cache space per machine, in MB, default 4TB, 0 means unlimited\n"
	      "-j <percent>    Thinning percentage jump, default no thinning\n",
              x );
}

FD concat (vector<FD> &fds) {

  FD answer ;

  // Get the max duration seen in all these FDs
  // Make a set of all the stack distance values across all the FDs
  int maxdur = 0 ;
  set<long long> allMB ; 
  for (vector<FD>::iterator it = fds.begin(); it != fds.end() ; it++ ) {
    FD &f = *it ;
    if (maxdur < f.stdtime.duration) maxdur = f.stdtime.duration ;
    f.stdtime.makeMb2IatIndex() ;
    f.stdtime.getAllStackDistances(allMB);
  }
  answer.stdspace.duration = maxdur ;
  answer.stdtime.duration = maxdur ;
  answer.stdcount.duration = maxdur ;

  // Initialize default prev values.
  map<int,long long> prevMBs;
  map<int,long long> prevObjct;
  map<int,int> prevIAT;
  map<int,long long> prevWeight;
  for (int i = 0 ; i < fds.size(); i++ ) {
    prevMBs[i] = 0 ;
    prevObjct[i] = 0 ;
    prevIAT[i] = -1 ;
    prevWeight[i] = 0 ;
  }

  // For each known mb, calculate a concatenated stdtime_t
  for (set<long long>::iterator it = allMB.begin() ; it != allMB.end(); it++ ) {
    stdtime_t sThis ;
    double iatsum = 0 ;
    double weightsum = 0 ;
    int iatCount = 0 ;
    long long mb = *it ;
    char buf[65535] ;
    long long mbsum = 0 ;

    if (mb>cmax) { 
      sprintf (buf, "Not considering MB %lld:", mb) ;
      break ;
    }
    sprintf (buf, "Considering MB %lld:", mb) ;
    

    for (int i = 0 ; i < fds.size(); i++ ) {
      int thisIAT = 0 ;
      double isum = 0 ; 
      int ict = 0 ;

      // Find if there are one or more entrries in stdtime that have this mb
      sprintf (buf+strlen(buf), "g%d(", i) ;

      set<int> iats ;
      iats = fds[i].stdtime.findMatchingIAT(mb) ;
      if (iats.size()==0) {
        // no exact match. 
        // Can't use this mb as it may be beyond maxMB. Use the prev known MB and IAT 
        if (mb > fds[i].stdtime.getMaxMB() ) {
          mbsum += prevMBs[i] ;
        } else {
          mbsum += mb ;
        }

        if (prevIAT[i]>-1) { 
          double prevWeigtedIAT = (double) prevIAT[i] * (double) prevWeight[i];
          iatsum += prevWeigtedIAT ;
          weightsum += prevWeight[i] ; 
          iatCount ++ ;
          sprintf (buf+strlen(buf), "%d", prevIAT[i] ) ;
        } 
        else {
          sprintf (buf+strlen(buf), "-") ;
        }
      } else {
        // There is at least one entry in this FD to be added 
        for (set<int>::iterator it2 = iats.begin(); it2 != iats.end(); it2++) {
          int iat = *it2 ;
          //sprintf (buf+strlen(buf), " %d ", iat) ;
          isum += iat ;
          ict ++ ;
          stdtime_t sThat = fds[i].stdtime.getMatching(iat);
          sThis += sThat ;
        }
        //if ( isum>0 && ict>0 ) {
        if ( ict>0 ) {
          // Find the traffic weight for this IAT
    
          long long weight = 1 ;
          if (weighByTraffic) {
            weight = fds[i].stdtime.total_served_bytes ;
          }
          //std::map<int, stdtime_t>::iterator x = fds[i].stdtime.getEqualLower(isum/ict) ;
          //long long weight = (x->second).cum_bytes ;

          // Update the two prev data structs
          prevIAT[i] = isum/ict ; 
          prevMBs[i] = mb ;
          prevWeight[i] = weight ;

          sprintf (buf+strlen(buf), ": %d", (int)(isum/ict)) ;

          iatsum += weight*isum/ict ; 
          weightsum += weight ;
          iatCount ++ ;
          mbsum += mb ;
        }
      }
      sprintf (buf+strlen(buf), ")") ;
    }
    int iatAvg = 0 ;
    if (iatCount>0) {
      if (weightsum > 0) {
        iatAvg = iatsum/weightsum ;
      }
      sprintf (buf+strlen(buf), "%f/%f /%lld, iatavg = %d", iatsum, weightsum, mbsum, iatAvg) ;
      stdtime_t &s = answer.stdtime.stdtime_array[iatAvg] ;
      s += sThis ;    
      s.mb = mbsum ;
    }
    //cout << buf << endl ;
  }

  // Compute total bytes and hits served
  for (int i = 0 ; i < fds.size(); i++ ) {
    answer.stdtime.total_requests += (fds[i].stdtime.total_requests) ; 
    answer.stdtime.total_served_bytes += (fds[i].stdtime.total_served_bytes) ;  
    answer.stdtime.total_mib += (fds[i].stdtime.total_mib) ;
    answer.stdtime.uniqueObjectCount += (fds[i].stdtime.uniqueObjectCount) ;
    answer.stdtime.uniqueBytesize += (fds[i].stdtime.uniqueBytesize) ;
    answer.stdtime.peakRequests300s += (fds[i].stdtime.peakRequests300s) ;
    answer.stdtime.peakBytes300s += (fds[i].stdtime.peakBytes300s) ;
  } 
  answer.stdtime.uniqueObjectCount /= minGhost ;
  answer.stdtime.uniqueBytesize /= minGhost ;
  
  return answer ;
}

void readCommands ( const char *fname, vector<FD_Operation> &commands) {

  FILE *fp ;
  char s[LINE_LENGTH], action[LINE_LENGTH];
  int lc = 0 ;
  FD_Operation fop;

  if ((fp = fopen(fname, "r")) == NULL) {
    fprintf(stderr, "fail to open %s to read\n", fname);
    exit(1);
  }

  while ( fgets(s, LINE_LENGTH, fp) ) {

    if (s[0] == '#') { continue; }
    FD_Quota q;

    // Read the 2 fields on the line
    std::istringstream iss(s) ;
    iss >> fop.trafficClass;
    iss >> fop.scale ;
    fop.operation = FD_CONCAT; 
    if (!iss) {
      fprintf(stderr, "Bad action in config: %s\n", s);
      exit(1);
    }
    commands.push_back(fop);
  }

  fclose(fp);
}

void readAllFDs( vector<FD_Operation> &commands, vector<FD> &fds) {

  for ( vector<FD_Operation>::iterator it = commands.begin() ; it != commands.end(); it ++) {
    FD_Operation &fdo = *it ;
    FD x ;

    string fname ; 
    if (iDir) {
      fname = string(iDir) + "/stdtime." + fdo.trafficClass ;
    } else {
      fname = "stdtime." + fdo.trafficClass ;
    }
    float scale = fdo.scale ;
    if (x.stdtime.read(fname.c_str(), scale) !=0 ) {
      cout << "Error reading " << fname.c_str() << endl ;
      exit(1);
    }
    //x.stdtime.makeCumulative() ;
    fds.push_back(x) ;
  }
}


int main(int argc, char **argv) {

  int opt;
  int ok = 0;
  vector<FD_Operation> commands ;
  vector<FD> fds;
  char fname[MAXNAMELEN] ;
  double pctJump=0 ;

  strcpy(answersuffix, "mix") ;
  
  // First check the environment
  iDir = getenv("CONCAT_IDIR") ;
  oDir = getenv("CONCAT_ODIR") ;
  if (oDir == NULL) {
    oDir = strdup(".") ;
  }
  suffix = getenv("CONCAT_SUFFIX") ;
  if (suffix != NULL) {
    strcpy(answersuffix, suffix) ;
  }
  char *x ;
  x = getenv("CONCAT_MINGHOST") ;
  if (x != NULL) {
    minGhost = atoi(x) ;
  } 

  x = getenv("CONCAT_THINNING_PCT_JUMP") ;
  if (x != NULL) {
    pctJump = atof(x);
  } 
  x = getenv("CONCAT_CMAX") ;
  if (x!= NULL) {
    cmax = atoi(x) ;
    if (cmax<=0) cmax = INT_MAX ;
  }
  char *configFile = getenv("CONCAT_CONFIG") ;
  if (configFile != NULL) {
    readCommands(configFile,commands) ;
    ok = 1 ;
  }
 
  while ((opt = getopt(argc, argv, "m:c:o:h:w:I:O:n:j:")) != -1) {
    switch (opt) {
      case 'c':
        readCommands(optarg, commands);
        ok = 1 ;
        break;
      case 'o':
        strcpy(answersuffix,optarg);
        break;
      case 'm':
        cmax = atoi(optarg);
        if (cmax<=0) cmax = INT_MAX ;
        break;
      case 'n': 
        minGhost = atoi(optarg) ;
        if (minGhost<=0) minGhost = 1 ;
        break ;
      case 'w':
	weighByTraffic = true ;
        break ;
      case 'I':
        iDir=strdup(optarg) ;
        break ;
      case 'O':
        oDir=strdup(optarg) ;
        break ;
      case 'j':
	pctJump = atof(optarg); 
        break ;
      case 'h':
      default:
        print_usage(argv[0]); 
        exit(1);
    }
  }
  if (!ok) {
    print_usage(argv[0]);
    exit(1);
  }

  readAllFDs(commands, fds) ;
  FD answer = concat(fds) ;
  //answer.createStdspaceStdcount();

  //sprintf(fname, "stdspace.%s", answersuffix);
  //answer.stdspace.write(fname) ;
  
  cout << "Writing to " ;
  sprintf(fname, "%s/stdtime.%s", oDir, answersuffix);
  cout << fname << endl ;
  answer.stdtime.write(fname, pctJump) ;
  cout << "Done" << endl ;
  
  return 0 ;
}

