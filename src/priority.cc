#include "priority.h"

#include <stdlib.h>
#include <sys/time.h>

// uint64_t GDSFPriority::new_priority(uint64_t old_priority, const ObjHeader *
// header) {
//     double prio = ((double)header->cost * (header->freq + 1)) /
//     (header->key_size + header->val_size) + L_; return *(uint64_t *)&prio;
// }

// uint64_t GDSFPriority::merge_priority(uint64_t old_prio,
//         const ObjHeader * header, uint64_t ts) {
//     double prio = ((double)header->cost * (header->freq + 1)) /
//     (header->key_size + header->val_size) + L_; return *(uint64_t *)&prio;
// }

// uint64_t GDSPriority::new_priority(uint64_t old_priority, const ObjHeader *
// header) {
//     double prio = ((double)header->cost) / (header->key_size +
//     header->val_size) + L_; return *(uint64_t *)&prio;
// }

// uint64_t GDSPriority::merge_priority(uint64_t old_prio,
//         const ObjHeader * header, uint64_t ts) {
//     double prio = ((double)header->cost) / (header->key_size +
//     header->val_size) + L_; return *(uint64_t *)&prio;
// }

// uint64_t LIRSPriority::new_priority(uint64_t old_priority, const ObjHeader *
// header) {
//     uint64_t cur_ts = new_ts();
//     uint64_t irr = cur_ts - header->last_ts;
//     return irr;
// }

// uint64_t LIRSPriority::merge_priority(uint64_t old_priority,
//         const ObjHeader * header, uint64_t ts) {
//     if (ts > header->last_ts) {
//         return new_ts() - ts;
//     }
//     return old_priority;
// }

// uint64_t LRFUPriority::new_priority(uint64_t old_priority, const ObjHeader *
// header) {
//     uint64_t cur_ts = new_ts();
//     uint64_t interval = cur_ts - header->last_ts;
//     double new_prio = 1 + f(interval) * old_priority;
//     return *(uint64_t *)&new_prio;
// }

// uint64_t LRFUPriority::merge_priority(uint64_t old_priority,
//         const ObjHeader * header, uint64_t ts) {
//     if (ts <= header->last_ts) {
//         return old_priority;
//     }
//     uint64_t interval = new_ts() - ts;
//     double new_prio = 1 + f(interval) * old_priority;
//     return *(uint64_t *)&new_prio;
// }

// uint64_t FIFOPriority::new_priority(uint64_t old_priority, const ObjHeader *
// header) {
//     if (old_priority == 0) {
//         return new_ts();
//     }
//     return old_priority;
// }

// uint64_t FIFOPriority::merge_priority(uint64_t old_priority,
//         const ObjHeader * header, uint64_t ts) {
//     return old_priority;
// }

// uint64_t LFUDAPriority::new_priority(uint64_t old_priority, const ObjHeader *
// header) {
//     return header->freq + L_;
// }

// uint64_t LFUDAPriority::merge_priority(uint64_t old_priority,
//         const ObjHeader * header, uint64_t ts) {
//     return header->freq + L_;
// }

// uint64_t LRUKPriority::new_priority(uint64_t old_priority, const ObjHeader *
// header) {
//     if ((header->freq + 1) % K_ == 0) {
//         return new_ts();
//     }
//     return old_priority;
// }

// uint64_t LRUKPriority::merge_priority(uint64_t old_priority,
//         const ObjHeader * header, uint64_t ts) {
//     if ((header->freq + 1) % K_ == 0) {
//         return old_priority > ts ? old_priority : ts;
//     }
//     return old_priority;
// }

// uint64_t SIZEPriority::new_priority(uint64_t old_priority, const ObjHeader *
// header) {
//     return header->key_size + header->val_size;
// }

// uint64_t SIZEPriority::merge_priority(uint64_t old_priority,
//         const ObjHeader * header, uint64_t ts) {
//     return header->key_size + header->val_size;
// }

// uint64_t MRUPriority::new_priority(uint64_t old_priority, const ObjHeader *
// header) {
//     return new_ts();
// }

// uint64_t MRUPriority::merge_priority(uint64_t old_priority,
//         const ObjHeader * header, uint64_t ts) {
//     return old_priority > ts ? old_priority : ts;
// }

// uint64_t HyperbolicPriority::new_priority(uint64_t old_priority, const
// ObjHeader * header) {
//     uint64_t new_prio = 0;
//     if (old_priority == 0) {
//         new_prio = new_ts32();
//     }
//     new_prio = old_priority + ((uint64_t)1 << 32);
//     return new_prio;
// }

// uint64_t HyperbolicPriority::merge_priority(uint64_t old_priority,
//         const ObjHeader * header, uint64_t ts) {
//     uint64_t new_prio = 0;
//     if (old_priority == 0) {
//         new_prio = new_ts32();
//     }
//     new_prio = old_priority + ((uint64_t)1 << 32);
//     return new_prio;
// }

// double HyperbolicPriority::parse_priority(uint64_t priority) {
//     uint32_t freq = (uint32_t)(priority >> 32);
//     uint32_t ins_ts = (uint32_t)(priority & 0xFFFFFFFF);
//     uint32_t cur_ts = new_ts32();
//     double ret = (double)freq / (cur_ts - ins_ts) * 1e10;
//     return ret;
// }