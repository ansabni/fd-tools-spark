#ifndef FD_CALCULUS_H
#define FD_CALCULUS_H

#include <string>
#include "FD.h"


#define MAX_COMMANDS 5000

typedef enum {FD_ABSOLUTE, FD_TRAFFICVOLUME} FD_SCALE_TYPE;
typedef enum {FD_ADD, FD_SUBTRACT, FD_CONCAT, FD_CONCAT_NOICP, FD_THIN} FD_OP;

class FD_Operation {
  public:
    string trafficClass;
    FD_OP operation;
    float scale ;
    int numShards ;
    int totalShards ;
    FD_Quota quota ;
    FD_SCALE_TYPE scaleType ;    
}; 

class FD_Calculus_Config {
  public:

    FD_Calculus_Config() {
      answerSuffix = string("mix");
      numCommands = 0 ;
      iDir = string(".") ;
      oDir = string(".") ;
      pctJump = 0;
      file_error_messages.clear(); 
    }

    ~FD_Calculus_Config() {
    }

    // You can either add command by command, or give it a file with the commands
    void addCommand(FD_Operation &command) ;
    int readCommands() ;
    int readCommands(const char *fname) ;
    int parseACommandLine(char *s) ;
    void print(); // Prints the sequence of commands

    // Process the sequence of commands
    int process();
    FD processSharding ( FD &fd, int numShards, int totalShards ) ;
    void prepareFD ( FD_Operation &fdo, FD &ifd ) ;

    // Get the answer
    FD getAnswerFD() { return answer;}

    // Save the answer
    void setAnswerSuffix(string suff) { answerSuffix = suff ; }
    int saveAnswerFD();

    // Input and Output directories
    void setInputDirectory(char *d) { iDir = string(d) ; }
    void setOutputDirectory(char *d) { oDir = string(d) ; }

    // Thinning Percentage Jump
    void setThinningPctJump(double pct) { pctJump = pct ; }


  private:
    FD answer;
    string answerSuffix; 
    string iDir ;
    string oDir ;
    string file_error_messages ;
    double pctJump;
    FD_Operation commands[MAX_COMMANDS];
    int numCommands ;
    vector<string> tmpFiles ;
} ;


#endif

