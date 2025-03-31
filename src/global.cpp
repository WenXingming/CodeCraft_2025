#include "global.h"

// 全局变量定义
// ===================================================================================
int T, M, N, V, G;
int TIMESTAMP;

vector<Tag> tags;
vector<Object> objects;     // (4 + 4 + 4 + 4*3 + 3*5) * MAX_OBJECT_NUM = 3.9 * 10^7 B ≈ 39 MB
vector<Disk> disks;         // MAX_DISK_NUM * (MAX_DISK_SIZE + 13B) ≈ 10 * 16384 = 1.6 * 10^5 ≈ 0.16 MB