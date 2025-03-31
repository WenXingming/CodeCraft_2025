// 预编译
// ===================================================================================
#pragma once
#include <cstdio>
#include <cassert>
#include <cstdlib>
#include <climits>
#include <cmath>
#include <iostream>
#include <vector>
#include <deque>
#include <queue>
#include <string>
#include <algorithm>
#include <numeric>
#include <chrono>

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

// T：时间片、M：对象的标签数、N：硬盘个数、V：每个硬盘的存储单元个数、G：每个磁头每个时间片最多消耗的令牌数
extern int T, M, N, V, G;
extern int TIMESTAMP;

// 结构体定义
// ===================================================================================

struct Request{
    int id;                 // 请求 id
    int objectId;           // 请求的对象 id

    int arriveTime;         // 请求到达的时刻
    vector<bool> hasRead;   // f(i)：对象的第 i 个块是否读取，i ∈ [1, object.size]

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
    queue<Request> timeoutRequests;         // 这里放超时的 request, 读取块时维护

    Object(){}
    Object(int _id, int _size, int _tagId){
        this->id = _id;
        this->size = _size;
        this->tagId = _tagId;

        this->replicaDiskId.assign(REP_NUM + 1, 0);
        this->replicaBlockUnit.assign(REP_NUM + 1, vector<int>(size + 1, 0));
    }
};

const int NEAR_NUM = 45; //相邻的 NEAR_NUM  个对象写入同一个磁盘
struct Tag{
    int id;
    // 管理标签写入磁盘
    int writeMainDiskId;    // 拥有此 id 标签的对象写入主分区时的磁盘号, [1, N]
    int writeRandomDiskId;
    // NOTE: [)
    int startUnit;          // 此 id 标签的对象在磁盘分区（主分区）上的起始位置, [1, V]
    int endUnit;            // 此 id 标签的对象在磁盘分区（主分区）上的终止位置

    int NearNum;
    int updateNum;

    vector<int> freDel;     // NOTE: 下标从 1 开始
    vector<int> freWrite;
    vector<int> freRead;

    // 非必要不提供默认构造函数。可以使用【默认参数】
    Tag(int _writeMainDiskId = 1, int _writeRandomDiskId = 1, int _startUnit = 1, int _endUnit = 1, int _updateNum = 0) {
        // id 待留运行时初始化
        this->writeMainDiskId = _writeMainDiskId;
        this->writeRandomDiskId = _writeRandomDiskId;
        this->startUnit = _startUnit;
        this->endUnit = _endUnit;

        this->NearNum = 0;
        this->updateNum = _updateNum;

        this->freDel.assign((T - 1) / FRE_PER_SLICING + 2, 0);
        this->freWrite.assign((T - 1) / FRE_PER_SLICING + 2, 0);
        this->freRead.assign((T - 1) / FRE_PER_SLICING + 2, 0);
    }

    // 得到当前写入的主分区所在磁盘
    int update_main_disk_id(){
        int tmp = writeMainDiskId;
        writeMainDiskId = writeMainDiskId % N + 1;

        // 将相邻时刻到达的同标签对象尽可能写入同样的三个磁盘（便于串行读取）
        /// TODO: 调参
        if (updateNum != -1) {
            ++updateNum;
            if ((updateNum % 3 == 0) && (updateNum % (NearNum*3) != 0)) { // 相邻的 45 个对象写入同一个磁盘, 每个对象 3 个副本
                writeMainDiskId = (writeMainDiskId - 3 + N) % N;
                if (!writeMainDiskId) writeMainDiskId = N;
            }
        }

        return tmp;
    }
    // 随机写入的磁盘进行轮转
    int update_random_disk_id(){
        int tmp = writeRandomDiskId;
        writeRandomDiskId = writeRandomDiskId % N + 1;

        updateNum = -1; // 当发生了该标签的主分区写不下（此时大概 2w 个时间片 +），该标签就不再使用 updateNum 策略，即将相邻时刻到达的同标签对象尽可能写入同样的三个磁盘

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

extern vector<Tag> tags;
extern vector<Object> objects;     // (4 + 4 + 4 + 4*3 + 3*5) * MAX_OBJECT_NUM = 3.9 * 10^7 B ≈ 39 MB
extern vector<Disk> disks;         // MAX_DISK_NUM * (MAX_DISK_SIZE + 13B) ≈ 10 * 16384 = 1.6 * 10^5 ≈ 0.16 MB
extern vector<int> tagIdRequestNum; // f(x): tagId 为 x 的请求数量

int cal_block_id(const int& diskId, const int& unitId);
bool request_need_this_block(const int& diskId, const int& unitId);