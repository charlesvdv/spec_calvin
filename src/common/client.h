#ifndef COMMON_CLIENTS_H
#define COMMON_CLIENTS_H

#include "proto/txn.pb.h"

class TxnGetterClient {
  public:
    virtual ~TxnGetterClient() {}
    virtual void GetTxn(TxnProto **txn, int txn_id) = 0;
};

class Client: public TxnGetterClient {
  public:
    virtual ~Client() {}
    virtual void GotTxnExecuted(int txn_id) = 0;
};

#endif
