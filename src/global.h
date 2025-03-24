// 预编译
// ===================================================================================
#pragma once
#include <cstdio>
#include <cassert>
#include <cstdlib>
#include <cmath>
#include <iostream>
#include <vector>
#include <deque>
#include <queue>
#include <string>
#include <algorithm>

using namespace std;

// 宏定义
// ===================================================================================

#define MAX_DISK_NUM (10 + 1)
#define MAX_DISK_SIZE (16384 + 1)
#define MAX_REQUEST_NUM (30000000 + 1)
#define MAX_OBJECT_NUM (100000 + 1)
#define REP_NUM (3)
#define FRE_PER_SLICING (1800)
#define EXTRA_TIME (105)

// 全局变量
// ===================================================================================

int T; // T：时间片、M：对象的标签数、N：硬盘个数、V：每个硬盘的存储单元个数、G：每个磁头每个时间片最多消耗的令牌数
int M;
int N;
int V;
int G;
int TIMESTAMP;

// 结构体定义
// ===================================================================================

struct Request{
    int id;                 // 请求 id
    int objectId;           // 请求的对象 id

    int arriveTime;         // 请求到达的时刻，暂未使用
    vector<bool> hasRead;   // f(i)：对象的第 i 个块是否读取

    Request(){}
    Request(int _id, int _objectId, int _arriveTime, int _objectSize){ // 默认参数只能自右往左写
        this->id = _id;
        this->objectId = _objectId;
        this->arriveTime = _arriveTime;
        this->hasRead.assign(_objectSize + 1, false);    // 默认未被读
    }
};

struct Object{  
    int id;         // 对象 id
    int size;       // 对象 size
    int tagId;      // 对象 tag
    vector<int> replicaDiskId;              // f(i) = 第 i 个副本的所在磁盘号
    vector<vector<int>> replicaBlockUnit;   // f(i,j) = 第 i 个副本、第 j 个块所在的磁盘（块）单元号

    deque<Request> requests;                // 未完成的请求队列挂在对象上（用队列，是为了先来先处理）
    queue<Request> timeoutRequests;         // 这里放超时的 request

    Object(){}
    Object(int _id, int _size, int _tagId){
        this->id = _id;
        this->size = _size;
        this->tagId = _tagId;

        this->replicaDiskId.assign(REP_NUM + 1, 0);
        this->replicaBlockUnit.assign(REP_NUM + 1, vector<int>(size + 1, 0));
    }
};

struct Tag{
    int id;
    // 管理标签写入磁盘
    int writeMainDiskId;    // 拥有此 id 标签的对象写入主分区时的磁盘号
    int writeRandomDiskId;
    // NOTE: [)
    int startUnit;          // 此 id 标签的对象在磁盘分区（主分区）上的起始位置
    int endUnit;            // 此 id 标签的对象在磁盘分区（主分区）上的终止位置

    vector<int> freDel;     // NOTE: 下标从 1 开始
    vector<int> freWrite;
    vector<int> freRead;
    // 非必要不提供默认构造函数。可以使用【默认参数】
    Tag(/* int& _id,  */int _writeMainDiskId = 1, int _writeRandomDiskId = 1, int _startUnit = 1, int _endUnit = 1) {
        /* this->id = _id; */
        this->writeMainDiskId = _writeMainDiskId;
        this->writeRandomDiskId = _writeRandomDiskId;
        this->startUnit = _startUnit;
        this->endUnit = _endUnit;

        this->freDel.assign((T - 1) / FRE_PER_SLICING + 2, 0);
        this->freWrite.assign((T - 1) / FRE_PER_SLICING + 2, 0);
        this->freRead.assign((T - 1) / FRE_PER_SLICING + 2, 0);
    }

    // 得到当前写入的主分区所在磁盘
    int update_main_disk_id(){
        int tmp = writeMainDiskId;
        writeMainDiskId = writeMainDiskId % N + 1;
        return tmp;
    }
    // 随机写入的磁盘进行轮转
    int update_random_disk_id(){
        int tmp = writeRandomDiskId;
        writeRandomDiskId = writeRandomDiskId % N + 1;
        return tmp;
    }
};

struct DiskPoint{
    int position;       // 磁头位置
    int remainToken;    // 剩余 Token：因为移动需要消耗 Token
    char preAction;     // 上一个动作、上一个消耗的 Token：便于计算本次 Token
    int preCostToken;   

    string cmd;         // 每个磁头的命令

    DiskPoint(int _position = 1, int _remainToken = 0, char _preAction = 0, int _preCostToken = 0, string _cmd = ""){ // 写了默认参数，就不必写默认构造，否则编译报错（多个默认构造函数）
        this->position = _position;
        this->remainToken = _remainToken;
        this->preAction = _preAction;
        this->preCostToken = _preCostToken;
        this->cmd = _cmd;
    }
};

struct Disk{
    vector<int> diskUnits;       // MAX_DISK_SIZE = 16384 B
    DiskPoint diskPoint;    // 4 + 4 + 1 + 4 = 13 B 

    Disk(int& _V = V){
        this->diskUnits.assign(_V + 1, 0);    // 0 代表为空
        this->diskPoint = DiskPoint(1, 0, 0, 0);
    }
};

// 具体数据结构定义
// ===================================================================================

vector<Tag> tags;
vector<int> tagIdToTagsIndex; 
// f(x): tagId 为 x 的对象，其所属 tag 对象在 tags 中的下标。注：这里 tagIdToTagsIndex(M+1) 没用！因为 M 还未确定呢
// 按 read 量进行排序，read 多的放在磁盘前面。因为后续每个标签的分配区间满了的话，需要从磁盘的后向前插入对象。为了根据 tagId 快速找到所属的 tag 对象，需要维护一个 hash 表。

vector<Object> objects;     // (4 + 4 + 4 + 4*3 + 3*5) * MAX_OBJECT_NUM = 3.9 * 10^7 B ≈ 39 MB
vector<Disk> disks;         // MAX_DISK_NUM * (MAX_DISK_SIZE + 13B) ≈ 10 * 16384 = 1.6 * 10^5 ≈ 0.16 MB

vector<int> tagIdRequestNum; // f(x): tagId 为 x 的请求数量
int preTag = 1;
// int preTimestamp; // 未使用
