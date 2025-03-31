#include "global.h"

// 定义

int T, M, N, V, G;
int TIMESTAMP;

vector<Tag> tags;
vector<int> tagIdToTagsIndex; 
// f(x): tagId 为 x 的对象，其所属 tag 对象在 tags 中的下标。注：这里 tagIdToTagsIndex(M+1) 没用！因为 M 还未确定呢
// 按 read 量进行排序，read 多的放在磁盘前面。因为后续每个标签的分配区间满了的话，需要从磁盘的后向前插入对象。为了根据 tagId 快速找到所属的 tag 对象，需要维护一个 hash 表。
vector<Object> objects;     // (4 + 4 + 4 + 4*3 + 3*5) * MAX_OBJECT_NUM = 3.9 * 10^7 B ≈ 39 MB
vector<Disk> disks;         // MAX_DISK_NUM * (MAX_DISK_SIZE + 13B) ≈ 10 * 16384 = 1.6 * 10^5 ≈ 0.16 MB
vector<int> tagIdRequestNum; // f(x): tagId 为 x 的请求数量