#include "global.h"

// 初始化全局变量（vector 分配空间）
void init_global_container(){
    tags.assign(M + 1, Tag());              // Tag 没有默认构造函数，使用默认参数
    for(int i = 1; i < tags.size(); ++i) {
        tags[i].id = i;
    }
    tagIdToTagsIndex.assign(M + 1, 0); // 运行时 M 有值，分配内存就要写在运行时。写在全局区没用会有 bug！


    objects.resize(MAX_OBJECT_NUM + 1);     // 待留写入时初始化每个 object 对象
    disks.assign(N + 1, Disk());

    tagIdRequestNum.assign(M + 1, 0);
}

// init tags
void pre_input_process(){
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
    printf("================================\n");
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
    } */
}

void sort_tags(){ // 根据 read 总量进行排序，高的分区在前面。因为 write_to_random_partition 从后向前
    // 根据阅读量排序
    std::sort(tags.begin() + 1, tags.end(), [](const Tag& a, const Tag& b) {
        int totalRead1 = 0, totalRead2 = 0;
        for (int i = 1; i < a.freRead.size(); ++i){
            totalRead1 += a.freRead[i];
            totalRead2 += b.freRead[i];
        }
        return totalRead1 >= totalRead2;
    });
    /* // Test
    printf("Test: ========================\n");
    for (int i = 1; i < tags.size(); ++i){
        printf("%d\n", tags[i].id);
    } */

    // 维护 hash 表
    for (int i = 1; i < tagIdToTagsIndex.size(); ++i) {
        for (int j = 1; j < tags.size(); ++j) {
            const Tag& tag = tags[j];
            if (tag.id == i) {
                tagIdToTagsIndex[i] = j;
                break;
            }
        }
    }
    /* // Test
    printf("Test: ========================\n");
    for (int i = 1; i < tagIdToTagsIndex.size(); ++i){
        printf("%d\n", tagIdToTagsIndex[i]);
    } */
}

void do_partition(){ // Do：计算 startUnit、endUnit
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
        
    // 根据每个标签的百分比，计算应该在磁盘上分配的容量，并计算得到每个标签的区间。NOTE: 10% 剩余（已取消）。
    vector<int> allocSpaces(tags.size());
    for (int i = 1; i < tags.size(); ++i){
        // allocSpaces[i] = V * 0.9 * (static_cast<double>(tagSpaces[i]) / totalSpace);
        allocSpaces[i] = V * (static_cast<double>(tagSpaces[i]) / totalSpace);
        tags[i].startUnit = tags[i - 1].endUnit;
        tags[i].endUnit = tags[i].startUnit + allocSpaces[i];

        if(i == M && tags[i].endUnit > V) tags[i].endUnit = V + 1;  // 不用 *0.9 分空闲分区（用所有硬盘空间分区）的话，就要检查
    }
    /* // TEST:
    printf("Test: ===========================\n");
    for (int i = 1; i < tags.size(); ++i){
        printf("%d\n", tagSpaces[i]);
        printf("tag %d's startUnit: %d, endUnit: %d\n", tags[i].id, tags[i].startUnit, tags[i].endUnit);
    } */
}

void timestamp_action() // 时间片对齐操作
{
    scanf("%*s%d", &TIMESTAMP);
    printf("TIMESTAMP %d\n", TIMESTAMP);

    fflush(stdout);
}

void delete_one_object(const int& objectId)
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
    // 判题机交互
    // 计算撤销请求数量
    int abortNum = 0;   
    for (int i = 1; i <= nDelete; ++i){
        int objcetId = deleteObjects[i];
        Object& object = objects[objcetId];

        abortNum += object.requests.size();
    }
    printf("%d\n", abortNum);
    // 打印撤销请求 id（并维护请求数据结构）
    for (int i = 1; i <= nDelete; ++i){ 
        int objcetId = deleteObjects[i];
        Object& object = objects[objcetId];     // 无法加 const，后面修改 requests
        deque<Request>& requests = object.requests;

        /* int size = requests.size();
        for (int i = 0; i < size; ++i){
            Request request = requests.front();
            requests.pop();
            printf("%d\n", request.id);
        } */
       while(!requests.empty()){
            Request& request = requests.front();
            requests.pop_front();
            printf("%d\n", request.id);
       }
    }

    fflush(stdout);
}

bool write_to_main_partition(int diskId, int objectId, int replicaId){
    vector<int>& diskUnits = disks[diskId].diskUnits;
    Object& object = objects[objectId];
    int tagIndex = tagIdToTagsIndex[object.tagId];
    const Tag& tag = tags[tagIndex];
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

/* bool write_to_free_partition(int diskId, int objectId, int replicaId){
    vector<int>& diskUnits = disks[diskId].diskUnits;
    Object& object = objects[objectId];
    int tagIndex = tagIdToTagsIndex[object.tagId];
    const Tag& tag = tags[tagIndex];

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
} */

bool write_to_random_partition(int diskId, int objectId, int replicaId){
    vector<int>& diskUnits = disks[diskId].diskUnits;
    Object& object = objects[objectId];
    int tagIndex = tagIdToTagsIndex[object.tagId];
    const Tag& tag = tags[tagIndex];
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
            // TEST
            if (write_to_main_partition(writeDiskId, objectId, k)) {
                isWriteSucess = true;
                break;
            }
        }
        if(isWriteSucess) continue;

        /* // 遍历所有磁盘，尝试写入空余分区
        for (int i = 1; i <= N; ++i) {
            int writeDiskId = tag.update_free_disk_id();
            if (write_to_free_partition(writeDiskId, objectId, k)) {
                isWriteSucess = true;
                break;
            }
        }
        if(isWriteSucess) continue; */

        // 无奈，只能随机找位置写入
        for (int i = 1; i <= N; ++i){
            int writeDiskId = tag.update_random_disk_id();
            if(write_to_random_partition(writeDiskId, objectId, k)){
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
            for (int j = 1; j <= object.size; j++){
                printf(" %d", object.replicaBlockUnit[i][j]);
            }
            printf("\n");
        }
    }

    fflush(stdout);
}

void update_disk_point(){ // 每个时间片初始化所有磁头令牌为 G、还有命令
    for(int i = 1; i < disks.size(); ++i){
        disks[i].diskPoint.remainToken = G;
        disks[i].diskPoint.cmd = "";
    }
    assert(disks[1].diskPoint.remainToken == G);
}

bool do_pass(const int& diskId){
    Disk& disk = disks[diskId];
    DiskPoint& diskPoint = disk.diskPoint;

    if(diskPoint.remainToken < 1) return false;
    diskPoint.position = diskPoint.position % V + 1;
    diskPoint.preAction = 'p';
    diskPoint.preCostToken = 1;
    diskPoint.remainToken -= 1;
    diskPoint.cmd += 'p';
    return true;
}

bool do_jump(const int& diskId, const int& unitId){
    Disk& disk = disks[diskId];
    DiskPoint& diskPoint = disk.diskPoint;

    if(diskPoint.remainToken < G) return false;
    diskPoint.position = unitId;
    diskPoint.preAction = 'j';
    diskPoint.preCostToken = G;
    diskPoint.remainToken = 0;
    diskPoint.cmd = "j " + std::to_string(unitId);
    return true;
}

bool do_read(int diskId){
    Disk& disk = disks[diskId];
    DiskPoint& diskPoint = disk.diskPoint;
    const auto& diskUnits = disk.diskUnits;
    // 计算花费
    int cost = 0;
    if(diskPoint.preAction != 'r' || TIMESTAMP == 1) cost = 64;
    else cost = std::max(16, static_cast<int>(std::ceil(diskPoint.preCostToken * 0.8)));

    if(diskPoint.remainToken < cost) return false;
    diskPoint.position = diskPoint.position % V + 1;
    diskPoint.preAction = 'r';
    diskPoint.preCostToken = cost;
    diskPoint.remainToken -= cost;
    diskPoint.cmd += 'r';
    return true;
}

void update_most_request_tag_and_disk_point(int _preTag = preTag){
    if(TIMESTAMP % 15 != 0) return; // Important：10 是需要调参的，确保这个间隔可以遍历完一个区间
    // 更新读取的 tag
    int mostRequestTag = preTag;
    for (int i = 1; i < tagIdRequestNum.size(); ++i){
        if(tagIdRequestNum[i] >= tagIdRequestNum[preTag]){
            mostRequestTag = i;
        }
    }
    preTag = mostRequestTag;
    // 移动磁头到该 tag 的区间
    const int& tagsIndex = tagIdToTagsIndex[preTag];
    const Tag& tag = tags[tagsIndex];
    const int& startUnit = tag.startUnit;
    // 对于每一个磁头，计算消耗，判断是用 j or p
    for (int i = 1; i < disks.size(); ++i){
        Disk& disk = disks[i];
        DiskPoint& diskPoint = disk.diskPoint;
        int distance = ((startUnit - diskPoint.position) + V) % V; // 计算 pass 的步数。磁头只能向后 pass，startUnit - position > or < 0
        
        if(distance >= diskPoint.remainToken){ // jump
            if(!do_jump(i, startUnit)) assert(false);
            continue;
        }
        while(distance--){  // 非 jump 就 pass
            if(!do_pass(i)) assert(false);
        }
    }
}

int cal_block_id(const int& objectId, const int& diskId, const int& unitId){
    const Object& object = objects[objectId];
    // 得到块的副本号
    int replicaId = 0;
    for (int i = 1; i < object.replicaDiskId.size(); ++i){
        if(object.replicaDiskId[i] == diskId){
            replicaId = i;
            break;
        }
    }
    assert(replicaId != 0);
    // 得到块的 blockId
    int blockId = 0;
    for (int i = 1; i < object.replicaBlockUnit[replicaId].size(); ++i){
        assert(object.size + 1 == object.replicaBlockUnit[replicaId].size());
        if(object.replicaBlockUnit[replicaId][i] == unitId){
            blockId = i;
            break;
        }
        /* // 打印
        printf("TEST: =============================\n");
        printf("unitId: %d\n", unitId);
        for(int j = 1; j < object.replicaBlockUnit[replicaId].size(); ++j){
            printf("%d ", object.replicaBlockUnit[replicaId][j]);
        }
        printf("\n"); */
    }
    assert(blockId != 0);
    return blockId;
}

bool need_read(const int& diskId, const int& unitId, const int& objectId){
    const Object& object = objects[objectId];
    const auto& requests = object.requests;
    if(!requests.empty()){
        return true; // TODO: 应该遍历 request 判断这个块是否需要读取（但是队列不好遍历...我改成了双端队列）
    }else{
        return false;
    }
}

bool check_request_is_done(const Request& _request){
    for (int i = 1; i < _request.hasRead.size(); ++i){
        if(_request.hasRead[i] == false) return false;
    }
    return true;
}

void read_action()
{
    // 处理输入。维护请求队列
    int nRead;
    int requestId, objectId;
    scanf("%d", &nRead);
    for (int i = 1; i <= nRead; i++) {
        scanf("%d%d", &requestId, &objectId);

        deque<Request>& requests = objects[objectId].requests;
        Request request;
        request.id = requestId;
        request.objectId = objectId;
        request.arriveTime = TIMESTAMP;
        request.hasRead = vector<bool>(objects[objectId].size + 1, false);
        requests.push_back(request);

        // 每来一个请求，维护当前请求趋势图
        const int& tagId = objects[objectId].tagId;
        tagIdRequestNum[tagId]++;
    }

    // 开始读取
    update_disk_point();
    update_most_request_tag_and_disk_point();

    vector<int> finishRequests;
    for(int i = 1; i < disks.size(); ++i){ // 每个磁头，串行开始读取
        const auto& diskUnits = disks[i].diskUnits;
        DiskPoint& diskPoint = disks[i].diskPoint;
        // 令牌未到山穷水尽之地就要一直尝试消耗
        while(true){ 
            const int& objectId = diskUnits[diskPoint.position];
            int unitId = diskPoint.position;
            // 需要 r、p 但令牌不够，这个磁盘磁头的动作结束
            if(!need_read(i, diskPoint.position, diskUnits[diskPoint.position])){
                if(!do_pass(i)) break;
                else continue;
            }
            if(!do_read(i)) break; 
            // 累积上报：读一个块，可以把 requests 队列中的 request 的所有相应位置置 true
            // int blockId = cal_block_id(objectId, i, diskPoint.position); // ！！注意，读之后磁头后移了，找了一下午 bug！！！
            int blockId = cal_block_id(objectId, i, unitId);
            auto& requests = objects[objectId].requests;
            for (auto it = requests.begin(); it != requests.end(); ){
                Request& request = *it;
                request.hasRead[blockId] = true;
                if(check_request_is_done(request)){ // TODO：如果某一次检查没过，其实就不必检查了
                    finishRequests.push_back(request.id);
                    it++; // 防在 pop_front() 前面，以防不测...或者使用 erase()
                    requests.pop_front();

                    // 每走一个请求，更新请求趋势图
                    const int& tagId = objects[objectId].tagId;
                    tagIdRequestNum[tagId]--;
                }else{
                    it++;
                }
            }
        }
    }

    // 输出 cmd、上报完成的请求
    for (int i = 1; i < disks.size(); ++i){
        const Disk& disk = disks[i];
        const DiskPoint& diskPoint = disk.diskPoint;
        const string& cmd = diskPoint.cmd;
        for (int i = 0; i < cmd.size(); ++i){
            printf("%c", cmd[i]);
        }
        if(cmd[0] != 'j') printf("#\n");
        else printf("\n");
    }
    printf("%d\n", finishRequests.size());
    for (int i = 0; i < finishRequests.size(); ++i){
        printf("%d\n", finishRequests[i]);
    }

    fflush(stdout);
}




int main()
{
    scanf("%d%d%d%d%d", &T, &M, &N, &V, &G);
    init_global_container();

    pre_input_process();
    sort_tags();
    do_partition();

    for (int t = 1; t <= T + EXTRA_TIME; t++) {
        timestamp_action();
        delete_action();
        write_action();
        read_action();
    }

    return 0;
}