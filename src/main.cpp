#include "global.h"
#define GAP 129 // 更新 read 的起始 Tag（其区间的 startPoint）。Todo：应该根据 preTag 的区间大小确定更新磁头位置的间隔时间
#define CONTINUE_READ_BLOCK_NUM 8   // 保证连续阅读的调参

const bool USE_LEFT_SHIFT = true;   // 使用逆序写

// 下面是初始化操作
// =============================================================================================

/// @brief 初始化全局变量（vector 分配空间）
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

/// @brief init tags
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
}

/// @brief 根据 read 总量进行排序，高的分区放在磁盘前面。因为 write_to_random_partition 从后向前
void sort_tags(){ 
    /// DO: 根据阅读量排序
    std::sort(tags.begin() + 1, tags.end(), [](const Tag& a, const Tag& b) {
        int totalRead1 = 0, totalRead2 = 0;
        for (int i = 1; i < a.freRead.size(); ++i){
            totalRead1 += a.freRead[i];
            totalRead2 += b.freRead[i];
        }
        return totalRead1 >= totalRead2;
    });
    /// DO: 维护 hash 表，快速根据 tagId 找到相应 tag 对象在 tags 的索引
    for (int i = 1; i < tagIdToTagsIndex.size(); ++i) {
        for (int j = 1; j < tags.size(); ++j) {
            const Tag& tag = tags[j];
            if (tag.id == i) {
                tagIdToTagsIndex[i] = j;
                break;
            }
        }
    }
}

/// @brief 计算每个分区的 startUnit、endUnit
void do_partition(){
    /// DO: 计算每个标签占的空间、计算所有标签占的总容量
    vector<int> tagSpaces(tags.size());
    int totalSpace = 0;
    for (int i = 1; i < tags.size(); ++i){
        for (int j = 1; j < tags[i].freDel.size(); ++j) {
            tagSpaces[i] += tags[i].freWrite[j];
            tagSpaces[i] -= tags[i].freDel[j];
        }
        totalSpace += tagSpaces[i];
    }
    /// DO: 根据每个标签的百分比，计算应该在磁盘上分配的容量，并计算得到每个标签的区间。NOTE: 10% free 分区剩余（已取消）。
    vector<int> allocSpaces(tags.size());
    for (int i = 1; i < tags.size(); ++i){
        // allocSpaces[i] = V * 0.9 * (static_cast<double>(tagSpaces[i]) / totalSpace);
        allocSpaces[i] = V * (static_cast<double>(tagSpaces[i]) / totalSpace);
        tags[i].startUnit = tags[i - 1].endUnit;
        tags[i].endUnit = tags[i].startUnit + allocSpaces[i];

        if(i == M) tags[i].endUnit = V + 1; // 避免浮点数导致分区越界；同时也避免尾部少量空间未被利用
    }
}

/// @brief 时间片对齐操作
void timestamp_action(){ 
    scanf("%*s%d", &TIMESTAMP);
    printf("TIMESTAMP %d\n", TIMESTAMP);

    fflush(stdout);
}

// 下面是删除操作
// =============================================================================================

/// @brief 磁盘上删除对象 
void delete_one_object(const int& objectId){
    const Object& object = objects[objectId];
    for (int i = 1; i <= REP_NUM; ++i){
        for (int j = 1; j <= object.size; ++j){
            int diskId = object.replicaDiskId[i];
            Disk& disk = disks[diskId];
            int unitId = object.replicaBlockUnit[i][j];
            
            disk.diskUnits[unitId] = 0;
        }
    }
}

/// @brief 删除操作
void delete_action(){
    /// DO: 处理输入
    static vector<int> deleteObjects(MAX_OBJECT_NUM); // 10^6 * 4 = 4MB
    int nDelete;
    scanf("%d", &nDelete);
    for (int i = 1; i <= nDelete; i++) {
        scanf("%d", &deleteObjects[i]);
    }
    /// DO: 磁盘上进行删除
    for (int i = 1; i <= nDelete; i++) {
        int objectId = deleteObjects[i];
        delete_one_object(objectId);
    }
    /// DO: 判题机交互
    // 计算撤销请求数量
    int abortNum = 0;   
    for (int i = 1; i <= nDelete; ++i){
        int objcetId = deleteObjects[i];
        Object& object = objects[objcetId];

        abortNum += object.requests.size() + object.timeoutRequests.size();
    }
    printf("%d\n", abortNum);
    // 打印撤销请求 id（并维护请求数据结构）
    for (int i = 1; i <= nDelete; ++i){ 
        int objcetId = deleteObjects[i];
        Object& object = objects[objcetId];     // 无法加 const，后面修改 requests
        deque<Request>& requests = object.requests;
        queue<Request>& timeoutRequests = object.timeoutRequests;
        while (!requests.empty() || !timeoutRequests.empty()) {
            if(!requests.empty()){
                Request& request = requests.front();
                requests.pop_front();
                printf("%d\n", request.id);
            }else if(!timeoutRequests.empty()){
                Request& request = timeoutRequests.front();
                timeoutRequests.pop();
                printf("%d\n", request.id);
            }
        }
    }

    fflush(stdout);
}

// 下面是写操作
// =============================================================================================

/// @brief 对象尝试写入主分区
bool write_to_main_partition(const int& diskId, const int& objectId, const int& replicaId){
    vector<int>& diskUnits = disks[diskId].diskUnits;
    Object& object = objects[objectId];
    int tagIndex = tagIdToTagsIndex[object.tagId];
    const Tag& tag = tags[tagIndex];
    // 检查副本不能写入重复磁盘
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
    /// DO: 写入磁盘: 先尝试找连续的块写，不行再零碎写入，利用双指针找大块空间
    // 尝试找连续空间：要找能放下对象的最小连续块（即 >= object.size 但又最小的连续块）
    int index = tag.startUnit, size = INT_MAX; // 记录当前连续块的起始位置及大小
    for(int i = tag.startUnit; i + object.size <= tag.endUnit; ++i){
        if(diskUnits[i] != 0) continue;

        int j = i;
        while (j < tag.endUnit && diskUnits[j] == 0) {
            j++;
        }

        if(j-i == object.size){ index = i; break;} // 找到了大小最合适的连续块
        else if(j-i < object.size){ i = j; continue;}
        else{
            if(j-i >= size) {i = j; continue;}
            else{
                index = i;
                size = j - i;

                i = j;
            }
        }
    }
    // 写入（从 index 写入）
    for (int i = index, cnt = 0; i < tag.endUnit && cnt < object.size; ++i){
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

/// @brief 主分区写不下，尝试从后向前插入磁盘空隙中
bool write_to_random_partition(const int& diskId, const int& objectId, const int& replicaId){
    vector<int>& diskUnits = disks[diskId].diskUnits;
    Object& object = objects[objectId];
    const int& tagIndex = tagIdToTagsIndex[object.tagId];
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

bool write_one_object(const int& objectId){
    Object& object = objects[objectId];
    Tag& tag = tags[object.tagId];
    // 有 3 个副本
    for (int k = 1; k <= REP_NUM; ++k){
        // 遍历所有磁盘，尝试写入主分区
        bool isWriteSucess = false;
        for (int i = 1; i <= N; ++i) {
            int writeDiskId = tag.update_main_disk_id();
            if (write_to_main_partition(writeDiskId, objectId, k)) {
                isWriteSucess = true;
                break;
            }
        }
        if(isWriteSucess) continue;
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

void print_common(const int& _objectId){
    // printf("Common order: ==============================\n");
    const Object& object = objects[_objectId]; // 引用是个很危险的使用，它可以提高效率，但也有更改原始数据的风险。所以最好加 const
    printf("%d\n", object.id);
    for (int i = 1; i <= REP_NUM; ++i) {
        printf("%d", object.replicaDiskId[i]);
        for (int j = 1; j <= object.size; j++) {
            printf(" %d", object.replicaBlockUnit[i][j]);
        }
        printf("\n");
    }
}

/// @brief 与判题机交互逆序写。注：本地 object 维护的信息不变，只是告诉判题机块的写入顺序变了。搭配 cal_block_id() 使用
void print_left_shift(const int& _objectId){
    // printf("Inverse order: ==============================\n");
    const Object& object = objects[_objectId];
    printf("%d\n", object.id);
    for (int i = 1; i <= REP_NUM; ++i){
        printf("%d", object.replicaDiskId[i]);

        // 左移输出
        vector<int> vec = object.replicaBlockUnit[i];
        // int leftShiftNum = i - 1;
        // auto mid = vec.begin() + 1 + leftShiftNum; // 注：vec.size() = object.size + 1
        int leftShiftNum = ((object.size - 1) / (REP_NUM - 1)) * (i - 1); // 通过偏移计算左移位数。总偏移 / 需要偏移 k 次
        auto mid = vec.begin() + 1 + leftShiftNum;
        if(mid >= vec.begin() + 1 && mid < vec.end()){  // 函数要求 mid ∈ [first, last)
            std::rotate(vec.begin() + 1, mid, vec.end());
        } // 否则不旋转
        
        for (int j = 1; j < vec.size(); ++j){
            printf(" %d", vec[j]);
        }
        printf("\n");
    }
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
        if(USE_LEFT_SHIFT) print_left_shift(objectId);
        else print_common(objectId);
    }

    fflush(stdout);
}

// 下面是读操作
// =============================================================================================

/// @brief 每个时间片初始化所有磁头令牌为 G、还有命令
void update_disk_point(){
    for(int i = 1; i < disks.size(); ++i){
        disks[i].diskPoint.remainToken = G;
        disks[i].diskPoint.cmd = "";
    }
    assert(disks[1].diskPoint.remainToken == G);
}

int cost_token(const int& _diskId, const char& _action){
    int costToken = 0;

    if(_action == 'p') costToken = 1;
    else if(_action == 'j') costToken = G;
    else if(_action == 'r'){
        const Disk& disk = disks[_diskId];
        const DiskPoint& diskPoint = disk.diskPoint;
        if(diskPoint.preAction != 'r' || TIMESTAMP == 1) costToken = 64;
        else costToken = std::max(16, static_cast<int>(std::ceil(diskPoint.preCostToken * 0.8)));

    }else assert(false);
    
    return costToken;
}

/// @brief 磁头 pass
bool do_pass(const int& diskId){
    Disk& disk = disks[diskId];
    DiskPoint& diskPoint = disk.diskPoint;

    char action = 'p';
    int cost = cost_token(diskId, action);

    if(diskPoint.remainToken < cost) return false;
    diskPoint.position = diskPoint.position % V + 1;
    diskPoint.preAction = action;
    diskPoint.preCostToken = cost;
    diskPoint.remainToken -= cost;
    diskPoint.cmd += action;
    return true;
}

/// @brief 磁头 jump
bool do_jump(const int& diskId, const int& unitId){
    Disk& disk = disks[diskId];
    DiskPoint& diskPoint = disk.diskPoint;

    char action = 'j';
    int cost = cost_token(diskId, action);

    if(diskPoint.remainToken < G) return false;
    diskPoint.position = unitId;
    diskPoint.preAction = action;
    diskPoint.preCostToken = cost;
    diskPoint.remainToken -= cost;
    diskPoint.cmd = string(1, action) + " " + std::to_string(unitId); // 注意 char 不能直接加 string
    return true;
}

/// @brief 磁头 read
bool do_read(const int& diskId){
    Disk& disk = disks[diskId];
    DiskPoint& diskPoint = disk.diskPoint;
    const auto& diskUnits = disk.diskUnits;
    // 计算花费
    int action = 'r';
    int cost = cost_token(diskId, action);

    if(diskPoint.remainToken < cost) return false;
    diskPoint.position = diskPoint.position % V + 1;
    diskPoint.preAction = action;
    diskPoint.preCostToken = cost;
    diskPoint.remainToken -= cost;
    diskPoint.cmd += action;
    return true;
}

/// @brief 每隔 【GAP】 根据 tag 的请求趋势图尝试更新（重置）所有磁头的起始 read 位置
/// TODO: GAP 是需要调参的，确保这个间隔可以遍历完一个区间
/// DONE: 设置 3 个或多个 hotTag；并移动磁头到相应位置。经测试，设置 N 个得分最高！
void update_hot_tags_and_disk_point_position(){ 
    // if (TIMESTAMP % GAP != 0) return;
    if(TIMESTAMP < GAP){
        if(TIMESTAMP % 20 != 0) return;
    }else{
        if(TIMESTAMP % GAP != 0) return;
    }

    static vector<pair<int, int>> hotTags(M + 1);   // pair<int, int>: {tagId, requestNum}
    /// DO: 更新 hotTags
    for (int i = 1; i < hotTags.size(); ++i){
        hotTags[i] = { i, tagIdRequestNum[i] };
    }
    std::sort(hotTags.begin(), hotTags.end(), [](const pair<int, int>& x, const pair<int, int>& y) {
        return x.second > y.second;
    });
    /// DO: 每一个磁头移动到相应 hotTag 的区间起始位置
    for (int i = 1; i < disks.size(); ++i){
        /// WARNING: 每个磁盘头都移动到一个 tag 的 startUnit，最小的数据集上，3 个磁盘只有 2 个 tag，不够分，所以报错！跑不了小数据集
        /// SOLVE: 避免 hotTag 的数量少于 磁盘数量 造成越界访问
        const int& tagId = i < hotTags.size() ? hotTags[i].first : hotTags[hotTags.size() - 1].first; // const int& tagId = hotTags[(i+1)/2].first;   // (i+2)/3
        const int& tagsIndex = tagIdToTagsIndex[tagId];
        const Tag& tag = tags[tagsIndex];
        const int& startUnit = tag.startUnit;
        // 对于每一个磁头，计算消耗，判断是用 j or p
        Disk& disk = disks[i];
        DiskPoint& diskPoint = disk.diskPoint;
        int distance = ((startUnit - diskPoint.position) + V) % V; // 计算 pass 的步数。磁头只能向后 pass：startUnit - position > or < 0
        // jump
        if(distance >= diskPoint.remainToken){ 
            if(!do_jump(i, startUnit)) assert(false);
            continue;
        }
        // 非 jump 就 pass
        while(distance--){  
            if(!do_pass(i)) assert(false);
        }
    }
}

/// @brief 读一个块时，需要判断其是第几个块，以便于把请求的 hasRead 相应位置置 true
int cal_block_id(const int& diskId, const int& unitId, const int& objectId){
    assert(objectId != 0);  // 确保 object 不为 0，以防万一。主要是这个特况需要函数外部确认
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

    int blockId = 0;
    if(!USE_LEFT_SHIFT){
        // 得到块的 blockId（未使用左移打印写）
        for (int i = 1; i < object.replicaBlockUnit[replicaId].size(); ++i) {
            assert(object.size + 1 == object.replicaBlockUnit[replicaId].size());
            if (object.replicaBlockUnit[replicaId][i] == unitId) {
                blockId = i;
                break;
            }
        }
        assert(blockId != 0); // 确保传入的 diskId、unitId、objectId 对的上，在 object 中有记录
    }else{
        // 使用逆序写（左移打印写）时，读取时也要左移（不是逆运算，右移！）相应位数，确定读取的块是第几块
        auto vec = object.replicaBlockUnit[replicaId];
        // int leftShiftNum = replicaId - 1;
        // auto mid = vec.begin() + 1 + leftShiftNum; // 注：vec.size() = object.size + 1
        int leftShiftNum = ((object.size - 1) / (REP_NUM - 1)) * (replicaId - 1); // 通过偏移计算左移位数。总偏移 / 需要偏移 k 次
        auto mid = vec.begin() + 1 + leftShiftNum;
        if (mid >= vec.begin() + 1 && mid < vec.end()) { // 函数要求 mid ∈ [first, last)
            std::rotate(vec.begin() + 1, mid, vec.end());
        } // 否则不旋转

        for (int i = 1; i < vec.size(); ++i) {
            if (vec[i] == unitId) {
                blockId = i;
                break;
            }
        }
        assert(blockId != 0); // 确保传入的 diskId、unitId、objectId 对的上，在 object 中有记录
    }
    return blockId;
}

/// @brief 简单判断一个块是否需要读？这里逻辑比较简单：依据请求队列中是否需要这个块。
bool request_need_this_block(const int& diskId, const int& unitId, const int& objectId){
    if(objectId == 0) return false; // bug，特况，先要判断 objectId ！= 0，否则 cal_block_id 会有问题

    const Object& object = objects[objectId];
    const deque<Request>& requests = object.requests;
    for (auto it = requests.crbegin(); it != requests.crend(); it++){   // 我用成 [crend(), crbegin())了...用反了
        const Request& request = *it;
        const auto& hasRead = request.hasRead;
        int blockId = cal_block_id(diskId, unitId, objectId);

        if (hasRead[blockId] == false) return true; // for 内部只执行一次，因为只需访问最后一个 request 即可得出答案。队列不可遍历或随机访问我改成了双端队列
        else return false;
    }
    return false;
}   

/// @brief 某一个块可能并不需要，但是为了保持连续阅读，有时也需要 read
/// TODO: 调参！！！中间断多少个块需要连续读（最好是能够计算出来，从而做出最优决策。然而感觉难以计算最优，只能估计，可以在某一个区间内估计）
bool continue_read(const int& _diskId, const int& _unitId, const int& _objectId){
    if(request_need_this_block(_diskId, _unitId, _objectId)) return true;

    const Disk& disk = disks[_diskId];
    const DiskPoint& diskPoint = disk.diskPoint;

    if(diskPoint.preAction != 'r') return false;
    if(diskPoint.preCostToken == 64) return false;
    
    /// TODO: 待优化。后 N 块只要有 1 块需要读，我就继续读
    int unitId = _unitId;
    for (int i = 0; i < CONTINUE_READ_BLOCK_NUM; ++i){ /// TODO: 调参！！！
        unitId = unitId % V + 1;
        const int objectId = disk.diskUnits[unitId];
        if(request_need_this_block(_diskId, unitId, objectId)) return true;  // cal_block_id 中的 3 个参数必须匹配
    }
    return false;
}

/// @brief 检查一个 request 的 hasRead 数组，判断 request 是否完成
bool check_request_is_done(const Request& _request){
    for (int i = 1; i < _request.hasRead.size(); ++i){
        if(_request.hasRead[i] == false) return false;
    }
    return true;
}

/// TODO: 是否可以预读取？
void read_action(){
    // 处理输入
    int nRead;
    int requestId, objectId;
    scanf("%d", &nRead);
    for (int i = 1; i <= nRead; i++) {
        scanf("%d%d", &requestId, &objectId);
        // 维护请求队列
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
    update_hot_tags_and_disk_point_position();
    vector<int> finishRequests;
    for(int i = 1; i < disks.size(); ++i){ // 每个磁头，串行开始读取
        const auto& diskUnits = disks[i].diskUnits;
        DiskPoint& diskPoint = disks[i].diskPoint;
        // 令牌未到山穷水尽之地就要一直尝试消耗
        while(true){ 
            const int unitId = diskPoint.position;      // unitId 不可用引用，因为后面 cal_block_id 时磁头移动了
            const int& objectId = diskUnits[unitId];    // 注意： objectId 可能为 0。不知道为什么没判断居然没报错？
            // 需要 p、r 但令牌不够，这个磁盘磁头的动作结束         
            if(!continue_read(i, unitId, objectId)){
                if(!do_pass(i)) break;
                else continue;
            }         
            if(!do_read(i)) break; 
            
            if(objectId == 0) continue; // 为了连续 read，空块也读。空块无需更新请求队列、上报请求等

            bool preCheck = true;
            // 遍历 requests 累积上报：每读一个块，就把 requests 队列中的 request 的所有相应位置置 true
            int blockId = cal_block_id(i, unitId, objectId); // unitId 不可换为 diskPoint.position！注意，读之后磁头后移了，找了一下午 bug！！！
            auto& requests = objects[objectId].requests;
            for (auto it = requests.begin(); it != requests.end(); ){                
                Request& request = *it;
                // 每次读取块时检查,超时的请求直接扔了丢入超时队列，也无需上报了（或许可以减轻 requests 队列，帮助 need_read 判断）
                if(TIMESTAMP - request.arriveTime > EXTRA_TIME){
                    it++;
                    requests.pop_front();
                    objects[objectId].timeoutRequests.push(request);
                    // 更新请求趋势图
                    const int& tagId = objects[objectId].tagId;
                    tagIdRequestNum[tagId]--;
                    continue;
                }

                request.hasRead[blockId] = true;
                if(preCheck && check_request_is_done(request)){ // 如果某一次检查没过，其实就不必检查了，使用 preCheck 记录
                    finishRequests.push_back(request.id);
                    it++; // 防在 pop_front() 前面，以防不测...或者使用 erase()
                    requests.pop_front();

                    // 每走一个请求，更新请求趋势图
                    const int& tagId = objects[objectId].tagId;
                    tagIdRequestNum[tagId]--;
                }else{
                    preCheck = false;
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