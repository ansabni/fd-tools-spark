#ifndef FD_QUOTA_H
#define FD_QUOTA_H

class FD_Quota {
  public:
    FD_Quota() { clear();}
    void clear() {
      max_quota_mb = min_quota_mb = max_quota_count = min_quota_count = 0 ;
    }
    int inSizeQuota(long long mb) {
      if (max_quota_mb>0 && mb > max_quota_mb) return 0;
      if (min_quota_mb>0 && mb < min_quota_mb) return 0; 
      return 1 ;
    }
    int inCountQuota(long long count) {
      if (max_quota_count>0 && count > max_quota_count) return 0 ;
      if (min_quota_count>0 && count < min_quota_count) return 0 ;
      return 1 ;
    }
    int inQuota(long long mb, long long count) {
      return inSizeQuota(mb) && inCountQuota(count) ;
    }
    // 0 means no quota
    unsigned long long max_quota_mb;
    unsigned long long min_quota_mb;
    unsigned long long max_quota_count;
    unsigned long long min_quota_count;
};

#endif


