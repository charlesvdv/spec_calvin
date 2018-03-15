#ifndef _DB_SEQUENCER_SEQUENCER_H_
#define _DB_SEQUENCER_SEQUENCER_H_

#include "scheduler/deterministic_scheduler.h"
#define COLD_CUTOFF 990000

class AbstractSequencer {
public:
    virtual ~AbstractSequencer() {}
    virtual void output(DeterministicScheduler *scheduler) = 0;
    virtual void WaitForStart() {}
};

#endif // _DB_SEQUENCER_SEQUENCER_H_
