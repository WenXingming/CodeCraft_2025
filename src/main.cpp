#include "global.h"

// init tags
void pre_process_tags(){
    for (int k = 0; k < 3; ++k){
        for (int i = 1; i <= M; i++) {
            for (int j = 1; j <= (T - 1) / FRE_PER_SLICING + 1; j++) {
                if(k == 0) scanf("%d", &tags[i].freDel[j]);
                else if(k == 1) scanf("%d", &tags[i].freWrite[j]);
                else if(k == 2) scanf("%d", &tags[i].freRead[j]);
            }
        }
    }
    printf("OK\n");
    fflush(stdout);
    /* // test
    /* printf("================================\n");
    printf("%d %d %d %d %d\n", T, M, N, V, G);
    for (int k = 0; k < 3; ++k){
        for (int i = 1; i < tags.size(); i++) {
            for (int j = 1; j < tags[i].freDel.size(); j++) {
                if(k == 0) printf("%d ", tags[i].freDel[j]);
                else if(k == 1) printf("%d ", tags[i].freWrite[j]);
                else if(k == 2) printf("%d ", tags[i].freRead[j]);
            }
            printf("\n");
        }
        printf("\n");
    }
    printf("================================\n"); */

    // Do：计算 startUnit、endUnit
    // 计算每个标签占的空间、计算所有标签占的总容量
    vector<int> tagSpaces(tags.size());
    int totalSpace = 0;
    for (int i = 1; i < tags.size(); ++i){
        for (int j = 1; j < tags[i].freDel.size(); ++j) {
            tagSpaces[i] += tags[i].freWrite[j];
            tagSpaces[i] -= tags[i].freDel[j];
        }
        totalSpace += tagSpaces[i];
    }
    /* // test
    for (int i = 1; i < tags.size(); ++i){
        printf("==============================\n");
        printf("%d\n", tags[i].writeSpace);
    } */
        
    // 根据每个标签的百分比，计算应该在磁盘上分配的容量；NOTE: 10% 剩余。并计算每个标签的区间
    vector<int> allocSpaces(tags.size());
    for (int i = 1; i < tags.size(); ++i){
        allocSpaces[i] = V * 0.9 * (static_cast<double>(tagSpaces[i]) / totalSpace);
        tags[i].startUnit = tags[i - 1].endUnit;
        tags[i].endUnit = tags[i].startUnit + allocSpaces[i];
    }
    /* // TEST:
    for (int i = 1; i < tags.size(); ++i){
        printf("tag %d's startUnit: %d, endUnit: %d\n", i, tags[i].startUnit, tags[i].endUnit);
    } */
}

void delete_one_object(int objectId)
{
    const Object& object = objects[objectId];
    for (int i = 1; i <= REP_NUM; ++i){
        for (int j = 1; j <= object.size; ++j){
            int diskId = object.replicaDiskId[i];
            int unitId = object.replicaBlockUnit[i][j];

            Disk& disk = disks[diskId];
            disk.diskUnits[unitId] = 0;
        }
    }
}

void delete_action()
{
    // 处理输入
    static vector<int> deleteObjects(MAX_OBJECT_NUM); // 10^6 * 4 = 4MB
    int nDelete;
    scanf("%d", &nDelete);
    for (int i = 1; i <= nDelete; i++) {
        scanf("%d", &deleteObjects[i]);
    }

    // 磁盘上进行删除
    for (int i = 1; i <= nDelete; i++) {
        int objectId = deleteObjects[i];
        delete_one_object(objectId);
    }

    // 判题机交互（并维护请求数据结构）
    int abortNum = 0;
    for (int i = 1; i <= nDelete; ++i){
        int objcetId = deleteObjects[i];
        Object& object = objects[objcetId];

        abortNum += object.requests.size();
    }
    printf("%d\n", abortNum);

    // 删除请求
    for (int i = 1; i <= nDelete; ++i){
        int objcetId = deleteObjects[i];
        Object& object = objects[objcetId];         // 无法加 const，后面修改 requests
        queue<Request>& requests = object.requests;

        int size = requests.size();
        for (int i = 0; i < size; ++i){
            Request request = requests.front();
            requests.pop();
            printf("%d\n", request.id);
        }
    }

    fflush(stdout);
}

bool try_to_write_main_partition(int diskId, int objectId, int replicaId){
    vector<int>& diskUnits = disks[diskId].diskUnits;
    Object& object = objects[objectId];
    const Tag& tag = tags[object.tagId];

    // 副本不能写入重复磁盘
    for (int i = 1; i <= REP_NUM; ++i){
        if(object.replicaDiskId[i] == diskId) return false;
    }

    // 判断主分区剩余空间
    int restSpace = 0;
    for (int i = tag.startUnit; i < tag.endUnit; ++i){
        if(diskUnits[i] == 0) restSpace ++;
        if(restSpace == object.size) break;
    }
    if(restSpace != object.size) return false;

    // 写入磁盘
    for (int i = tag.startUnit, cnt = 0; i < tag.endUnit && cnt < object.size; ++i){
        if(diskUnits[i] == 0) {
            diskUnits[i] = objectId;
            cnt++;

            // 写入时注意要维护 object 信息
            object.replicaDiskId[replicaId] = diskId;
            object.replicaBlockUnit[replicaId][cnt] = i; // 注意 cnt++ 了
        }  
    }
    return true;
}

bool try_to_write_free_partition(int diskId, int objectId, int replicaId){
    vector<int>& diskUnits = disks[diskId].diskUnits;
    Object& object = objects[objectId];
    Tag& tag = tags[object.tagId];

    // 副本不能写入重复磁盘
    for (int i = 1; i <= REP_NUM; ++i){
        if(object.replicaDiskId[i] == diskId) return false;
    }

    // 判断空余分区剩余空间
    int restSpace = 0;
    for (int i = tags[tags.size() - 1].endUnit; i < V + 1; ++i){
        if(diskUnits[i] == 0) restSpace ++;
        if(restSpace == object.size) break;
    }
    if(restSpace != object.size) return false;

    // 写入磁盘
    for(int i = tags[tags.size() - 1].endUnit, cnt = 0; i < V + 1 && cnt < object.size; ++i){
        if(diskUnits[i] == 0) {
            diskUnits[i] = objectId;
            cnt++;

            // 写入时注意要维护 object 信息
            object.replicaDiskId[replicaId] = diskId;
            object.replicaBlockUnit[replicaId][cnt] = i;
        }
    }
    return true;
}

bool try_to_write_random_partition(int diskId, int objectId, int replicaId){
    vector<int>& diskUnits = disks[diskId].diskUnits;
    Object& object = objects[objectId];
    Tag& tag = tags[object.tagId];

    // 副本不能写入重复磁盘
    for (int i = 1; i <= REP_NUM; ++i){
        if(object.replicaDiskId[i] == diskId) return false;
    }

    // 判断整块磁盘的剩余空间
    int restSpace = 0;
    for (int i = V; i >= tags[1].startUnit; --i){
        if(diskUnits[i] == 0) restSpace ++;
        if(restSpace == object.size) break;
    }
    if(restSpace != object.size) return false;

    // 写入磁盘：见缝插针（tricks: 从后到前见缝插针）
    for(int i = V, cnt = 0; i >= tags[1].startUnit && cnt < object.size; --i){
        if(diskUnits[i] == 0) {
            diskUnits[i] = objectId;
            cnt++;

            // 写入时注意要维护 object 信息
            object.replicaDiskId[replicaId] = diskId;
            object.replicaBlockUnit[replicaId][cnt] = i;
        }
    }
    return true;
}

bool write_one_object(int objectId){
    Object& object = objects[objectId];
    Tag& tag = tags[object.tagId];

    // 有 3 个副本
    for (int k = 1; k <= REP_NUM; ++k){
        // 遍历所有磁盘，尝试写入主分区
        bool isWriteSucess = false;
        for (int i = 1; i <= N; ++i) {
            int writeDiskId = tag.update_main_disk_id();
            if (try_to_write_main_partition(writeDiskId, objectId, k)) {
                isWriteSucess = true;
                break;
            }
        }
        if(isWriteSucess) continue;

        // 遍历所有磁盘，尝试写入空余分区
        for (int i = 1; i <= N; ++i) {
            int writeDiskId = tag.update_free_disk_id();
            if (try_to_write_free_partition(writeDiskId, objectId, k)) {
                isWriteSucess = true;
                break;
            }
        }
        if(isWriteSucess) continue;

        // 无奈，只能随机找位置写入
        for (int i = 1; i <= N; ++i){
            int writeDiskId = tag.update_random_disk_id();
            if(try_to_write_random_partition(writeDiskId, objectId, k)){
                isWriteSucess = true;
                break;
            }
        }
        assert(isWriteSucess == true);
    }
    return true;
}

void write_action(){
    // 处理输入
    static vector<int> writeObjects(MAX_OBJECT_NUM); // 10^6 * 4 = 4MB; 存放 objectId
    int nWrite;
    scanf("%d", &nWrite);
    for (int i = 1; i <= nWrite; ++i){
        int objectId, objectSize, objectTag;
        scanf("%d%d%d", &objectId, &objectSize, &objectTag);
        writeObjects[i] = objectId;

        Object& object = objects[objectId];
        object.id = objectId;
        object.size = objectSize;
        object.tagId = objectTag;
        object.replicaDiskId.assign(REP_NUM + 1, 0);
        object.replicaBlockUnit.assign(REP_NUM + 1, vector<int>(object.size + 1, 0));
    }

    // 写入磁盘
    for (int i = 1; i <= nWrite; ++i){
        int objectId = writeObjects[i];
        write_one_object(objectId);
    }

    // 与判题机交互
    for (int i = 1; i <= nWrite; ++i){
        int objectId = writeObjects[i];
        const Object& object = objects[objectId];   // 引用是个很危险的使用，它可以提高效率，但也有更改原始数据的风险。所以最好加 const

        printf("%d\n", object.id);
        for (int i = 1; i <= REP_NUM; ++i){
            printf("%d", object.replicaDiskId[i]);
            const vector<int>& blocks = object.replicaBlockUnit[i];
            for (int j = 1; j <= object.size; j++){
                printf(" %d", blocks[j]);
            }
            printf("\n");
        }
    }

    fflush(stdout);
}

void read_action()
{
    int nRead;
    int requestId, objectId;
    scanf("%d", &nRead);
    for (int i = 1; i <= nRead; i++) {
        scanf("%d%d", &requestId, &objectId);

        Object& object = objects[objectId];   // 这里不可以用 const...，C++还挺安全
        queue<Request>& requests = object.requests;

        Request request;
        request.id = requestId;
        request.objectId = objectId;
        request.arriveTime = TIMESTAMP;
        request.hasRead = vector<bool>(object.size + 1, false);
        object.requests.push(request);
    }
    
    for (int i = 1; i < disks.size(); ++i){
        printf("#\n");
    }

    int finishRequestNum = 0;
    printf("%d\n", finishRequestNum);

    fflush(stdout);
}


void timestamp_action()
{
    scanf("%*s%d", &TIMESTAMP);
    printf("TIMESTAMP %d\n", TIMESTAMP);

    fflush(stdout);
}

int main()
{
    scanf("%d%d%d%d%d", &T, &M, &N, &V, &G);

    // 初始化全局变量（vector 分配空间）
    tags.assign(M + 1, Tag());              // Tag 没有默认构造函数，使用默认参数
    for(int i = 1; i < tags.size(); ++i) {
        tags[i].id = i;
    }

    objects.resize(MAX_OBJECT_NUM + 1);     // 待留写入时初始化每个 object 对象
    disks.assign(N + 1, Disk());


    pre_process_tags();
    
    // for (int i = 1; i <= N; i++) {
    //     disk_point[i] = 1;
    // }

    for (int t = 1; t <= T + EXTRA_TIME; t++) {
        timestamp_action();
        delete_action();
        write_action();
        read_action();
    }

    return 0;
}