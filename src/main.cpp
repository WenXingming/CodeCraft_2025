#include "global.h"

// CMD: python .\2025-code-craft\run.py .\2025-code-craft\interactor\windows\interactor.exe .\2025-code-craft\data\practice.in .\src\code_craft.exe

// 调参
constexpr bool USE_DFS = false;
constexpr int DFS_DEPTH = 19;           // [1, DFS_DEPTH)
constexpr int GAP = 45;

// =============================================================================================
// 下面是初始化操作

// 初始化全局变量（vector 分配空间）
void init_global_container() {
	tags.assign(M + 1, Tag()); // Tag 没有默认构造函数，使用默认参数
	for (int i = 1; i < tags.size(); ++i) {
		tags[i].id = i;
	}
	objects.resize(MAX_OBJECT_NUM + 1); // 待留写入时初始化每个 object 对象
	disks.assign(N + 1, Disk());

	tagIdRequestNum.assign(M + 1, 0);
}

// init tags
void pre_input_process() {
	for (int k = 0; k < 3; ++k) {
		for (int i = 1; i <= M; i++) {
			for (int j = 1; j <= (T - 1) / FRE_PER_SLICING + 1; j++) {
				if (k == 0) scanf("%d", &tags[i].freDel[j]);
				else if (k == 1) scanf("%d", &tags[i].freWrite[j]);
				else scanf("%d", &tags[i].freRead[j]);
			}
		}
	}
	printf("OK\n");
	fflush(stdout);
}

// 计算每个分区的 startUnit、endUnit。NOTE: 可以按「峰值容量」or「实际容量」进行分区; 经测试「峰值容量」磁盘碎片更少，分数更高
void do_partition() {
	// 计算每个标签的实际、峰值容量
	vector<int> tagSpaces(tags.size(), 0);
	vector<int> maxTagSpaces(tags.size(), 0);
	for (int i = 1; i < tags.size(); ++i) {
		for (int j = 1; j < tags[i].freDel.size(); ++j) {
			tagSpaces[i] += tags[i].freWrite[j];
			maxTagSpaces[i] = std::max(maxTagSpaces[i], tagSpaces[i]);
			tagSpaces[i] -= tags[i].freDel[j];
		}
	}
	int totalSpace = std::accumulate(tagSpaces.begin(), tagSpaces.end(), 0);
	int totalMaxSpace = std::accumulate(maxTagSpaces.begin(), maxTagSpaces.end(), 0);
	// 根据每个标签的百分比，计算应该在磁盘上分配的容量，并计算得到每个标签的区间。NOTE: 10% free 分区剩余（已取消）。
	vector<int> allocSpaces(tags.size());
	for (int i = 1; i < tags.size(); ++i) {
		// 可选按「峰值容量」or「实际容量」进行分区
		allocSpaces[i] = V * (static_cast<double>(maxTagSpaces[i]) / totalMaxSpace);
		tags[i].startUnit = tags[i - 1].endUnit;
		tags[i].endUnit = tags[i].startUnit + allocSpaces[i];

		if (i == M) tags[i].endUnit = V + 1; // 避免浮点数导致分区越界；同时也避免尾部少量空间未被利用
	}
	// 初始化 NearNum
	for (int i = 1; i < tags.size(); ++i) {
		tags[i].NearNum = (tags[i].endUnit - tags[i].startUnit) / 20;   // 相邻比例为 k 的对象写入同一个磁盘
	}
}

// 时间片对齐操作
void timestamp_action() {
	scanf("%*s%d", &TIMESTAMP);
	printf("TIMESTAMP %d\n", TIMESTAMP);
	fflush(stdout);
}

// =============================================================================================
// 下面是删除操作

// 磁盘上删除对象 
void delete_one_object(const int& objectId) {
	const Object& object = objects[objectId];
	for (int i = 1; i <= REP_NUM; ++i) {
		for (int j = 1; j <= object.size; ++j) {
			int diskId = object.replicaDiskId[i];
			Disk& disk = disks[diskId];
			int unitId = object.replicaBlockUnit[i][j];

			disk.diskUnits[unitId] = 0;
		}
	}
}

// 删除操作
void delete_action() {
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
	// 判题机交互: 计算撤销请求数量
	int abortNum = 0;
	for (int i = 1; i <= nDelete; ++i) {
		int objcetId = deleteObjects[i];
		Object& object = objects[objcetId];

		abortNum += object.requests.size() + object.timeoutRequests.size();
	}
	printf("%d\n", abortNum);
	// 判题机交互: 打印撤销请求 id（并维护请求数据结构）
	for (int i = 1; i <= nDelete; ++i) {
		int objcetId = deleteObjects[i];
		Object& object = objects[objcetId];     // 无法加 const，后面修改 requests
		deque<Request>& requests = object.requests;
		queue<Request>& timeoutRequests = object.timeoutRequests;
		while (!requests.empty() || !timeoutRequests.empty()) {
			if (!requests.empty()) {
				Request& request = requests.front();
				requests.pop_front();
				printf("%d\n", request.id);
				// 请求趋势图也要更新
				const int& tagId = object.tagId;
				tagIdRequestNum[tagId]--;
				// assert(tagIdRequestNum[tagId] >= 0);
			} else if (!timeoutRequests.empty()) {
				Request& request = timeoutRequests.front();
				timeoutRequests.pop();
				printf("%d\n", request.id);
				// 请求趋势图也要更新, 超时的已经在插入请求到请求队列时更新了
			}
		}
	}
	fflush(stdout);
}

// =============================================================================================
// 下面是写操作

// 对象尝试写入主分区
bool write_to_main_partition(const int& diskId, const int& objectId, const int& replicaId) {
	vector<int>& diskUnits = disks[diskId].diskUnits;
	Object& object = objects[objectId];
	const Tag& tag = tags[object.tagId];
	// 检查副本不能写入重复磁盘
	for (int i = 1; i <= REP_NUM; ++i) {
		if (object.replicaDiskId[i] == diskId) return false;
	}
	// 判断主分区剩余空间
	int restSpace = 0;
	for (int i = tag.startUnit; i < tag.endUnit; ++i) {
		if (diskUnits[i] == 0) restSpace++;
		if (restSpace == object.size) break;
	}
	if (restSpace != object.size) return false;
	// 写入磁盘: 先尝试找连续的块写，不行再零碎写入，利用双指针找大块空间，要找能放下对象的最小连续块（即 >= object.size 但又最小的连续块）
	int index = tag.startUnit, size = INT_MAX; // 记录当前连续块的起始位置及大小
	for (int i = tag.startUnit; i + object.size <= tag.endUnit; ++i) {
		if (diskUnits[i] != 0) continue;

		int j = i;
		while (j < tag.endUnit && diskUnits[j] == 0) {
			j++;
		}
		if (j - i == object.size) { index = i; break; } // 找到了大小最合适的连续块（另一种策略是找第一个能放下对象的连续块）
		else if (j - i < object.size) { i = j; continue; } else {
			if (j - i >= size) { i = j; continue; } else {
				index = i;
				size = j - i;

				i = j;
			}
		}
	}
	// 写入（从 index 写入）
	for (int i = index, cnt = 0; i < tag.endUnit && cnt < object.size; ++i) {
		if (diskUnits[i] == 0) {
			diskUnits[i] = objectId;
			cnt++;
			// 写入时注意要维护 object 信息
			object.replicaDiskId[replicaId] = diskId;
			object.replicaBlockUnit[replicaId][cnt] = i; // 注意 cnt++ 了
		}
	}
	return true;
}

// 不采用尾部写入峰值对象，而是从分区首部往两边找空闲位置插入对象
bool write_to_two_sides_partition(const int& diskId, const int& objectId, const int& replicaId) {
	vector<int>& diskUnits = disks[diskId].diskUnits;
	Object& object = objects[objectId];
	const Tag& tag = tags[object.tagId];

	// 副本不能写入重复磁盘
	for (int i = 1; i <= REP_NUM; ++i) {
		if (object.replicaDiskId[i] == diskId) return false;
	}
	// 判断整块磁盘的剩余空间
	int restSpace = 0;
	for (int i = V; i >= 1; --i) {
		if (diskUnits[i] == 0) restSpace++;
		if (restSpace == object.size) break;
	}
	if (restSpace != object.size) return false;
	// 双指针填入
	int leftUnit = tag.startUnit;
	int rightUnit = leftUnit + 1;

	vector<int> freeUnit;
	while ((leftUnit >= 1 || rightUnit <= V) && (freeUnit.size() < object.size)) {
		if (leftUnit >= 1 && (freeUnit.size() < object.size)) {
			if (diskUnits[leftUnit] == 0) {
				freeUnit.push_back(leftUnit);
			}
			leftUnit--;
		}

		if (rightUnit <= V && (freeUnit.size() < object.size)) {
			if (diskUnits[rightUnit] == 0) {
				freeUnit.push_back(rightUnit);
			}
			rightUnit++;
		}
	}

	for (int i = 1; i <= object.size; i++) {
		int unitId = freeUnit[i - 1];

		diskUnits[unitId] = objectId;
		object.replicaBlockUnit[replicaId][i] = unitId;
	}
	object.replicaDiskId[replicaId] = diskId;

	return true;
}

bool write_one_object(const int& objectId) {
	Object& object = objects[objectId];
	Tag& tag = tags[object.tagId];
	// 有 3 个副本
	for (int k = 1; k <= REP_NUM; ++k) {
		// 遍历所有磁盘，尝试写入主分区
		bool isWriteSucess = false;
		for (int i = 1; i <= N * tag.NearNum; ++i) {
			int writeDiskId = tag.update_main_disk_id();
			if (write_to_main_partition(writeDiskId, objectId, k)) {
				isWriteSucess = true;
				break;
			}
		}
		if (isWriteSucess) continue;
		// 无奈，处理峰值对象，向两边扩散插入磁盘
		for (int i = 1; i <= N; ++i) {
			int writeDiskId = tag.update_random_disk_id();
			if (write_to_two_sides_partition(writeDiskId, objectId, k)) {
				isWriteSucess = true;
				break;
			}
		}
		assert(isWriteSucess == true);
	}
	return true;
}

void write_action() {
	// 处理输入
	static vector<int> writeObjects(MAX_OBJECT_NUM); // 10^6 * 4 = 4MB; 存放 objectId
	int nWrite;
	scanf("%d", &nWrite);
	for (int i = 1; i <= nWrite; ++i) {
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
	for (int i = 1; i <= nWrite; ++i) {
		int objectId = writeObjects[i];
		write_one_object(objectId);
	}
	// 与判题机交互
	for (int i = 1; i <= nWrite; ++i) {
		int objectId = writeObjects[i];
		const Object& object = objects[objectId]; // 引用是个很危险的使用，它可以提高效率，但也有更改原始数据的风险。所以最好加 const
		printf("%d\n", object.id);
		for (int i = 1; i <= REP_NUM; ++i) {
			printf("%d", object.replicaDiskId[i]);
			for (int j = 1; j <= object.size; j++) {
				printf(" %d", object.replicaBlockUnit[i][j]);
			}
			printf("\n");
		}
	}

	fflush(stdout);
}

// =============================================================================================
// 下面是读操作

// 每个时间片初始化所有磁头令牌为 G、还有命令
void update_disk_point() {
	for (int i = 1; i < disks.size(); ++i) {
		disks[i].diskPoint.remainToken = G;
		disks[i].diskPoint.cmd = "";
	}
	assert(disks[1].diskPoint.remainToken == G);
}

// 计算消耗令牌
int cost_token(const int& _diskId, const char& _action) {
	int costToken = 0;

	if (_action == 'p') costToken = 1;
	else if (_action == 'j') costToken = G;
	else if (_action == 'r') {
		const Disk& disk = disks[_diskId];
		const DiskPoint& diskPoint = disk.diskPoint;
		if (diskPoint.preAction != 'r' || TIMESTAMP == 1) costToken = 64;
		else costToken = std::max(16, static_cast<int>(std::ceil(diskPoint.preCostToken * 0.8)));

	} else assert(false);

	return costToken;
}

// 磁头 pass
bool do_pass(const int& diskId) {
	Disk& disk = disks[diskId];
	DiskPoint& diskPoint = disk.diskPoint;

	char action = 'p';
	int cost = cost_token(diskId, action);

	if (diskPoint.remainToken < cost) return false;
	diskPoint.position = diskPoint.position % V + 1;
	diskPoint.preAction = action;
	diskPoint.preCostToken = cost;
	diskPoint.remainToken -= cost;
	diskPoint.cmd += action;
	return true;
}

// 磁头 jump
bool do_jump(const int& diskId, const int& unitId) {
	Disk& disk = disks[diskId];
	DiskPoint& diskPoint = disk.diskPoint;

	char action = 'j';
	int cost = cost_token(diskId, action);

	if (diskPoint.remainToken < G) return false;
	diskPoint.position = unitId;
	diskPoint.preAction = action;
	diskPoint.preCostToken = cost;
	diskPoint.remainToken -= cost;
	diskPoint.cmd = string(1, action) + " " + std::to_string(unitId); // 注意 char 不能直接加 string
	return true;
}

// 磁头 read
bool do_read(const int& diskId) {
	Disk& disk = disks[diskId];
	DiskPoint& diskPoint = disk.diskPoint;
	const auto& diskUnits = disk.diskUnits;
	// 计算花费
	int action = 'r';
	int cost = cost_token(diskId, action);

	if (diskPoint.remainToken < cost) return false;
	diskPoint.position = diskPoint.position % V + 1;
	diskPoint.preAction = action;
	diskPoint.preCostToken = cost;
	diskPoint.remainToken -= cost;
	diskPoint.cmd += action;
	return true;
}

// 计算磁盘一个位置的价值（等同于对象存储块的价值）
double compute_block_value(const int& diskId, const int& unitId) {
	double val = 0.0;

	if (disks[diskId].diskUnits[unitId] == 0) return val; // 空块，磁盘该位置未存储对象

	const int& objectId = disks[diskId].diskUnits[unitId];
	const Object& object = objects[objectId];
	const deque<Request>& requests = object.requests;
	for (const Request& request : requests) {  // 遍历所有有该块的请求，计算该块价值
		int blockId = cal_block_id(diskId, unitId);
		if (request.hasRead[blockId]) continue;

		int duration = TIMESTAMP - request.arriveTime;
		if (duration >= 0 && duration <= 10) {
			val += (-0.005 * duration + 1.0);
		} else if (duration > 10 && duration <= 105) {
			val += (-0.01 * duration + 1.05);
		} else { continue; }
	}

	return val;
}

// 计算指定磁盘上指定区间的读取价值
double compute_range_value(int diskId, const pair<int, int>& initRange) {
	double rangeValue = 0.0;
	const int leftRange = initRange.first;  // 左边界
	const int rightRange = initRange.second;  // 右边界
	for (int i = leftRange; i < rightRange; i++) {
		rangeValue += compute_block_value(diskId, i);
	}
	return rangeValue;
}

// 每次使用 tagIdRequestNum 前，可以利用该函数遍历整个磁盘检查超时请求，得到最新的 tagIdRequestNum
void traverse_all_disks_update_requests_num() {
	for (int i = 1; i < disks.size(); ++i) {
		for (int j = 1; j <= V; ++j) {
			const int& objectId = disks[i].diskUnits[j];
			if (objectId == 0) continue;

			Object& object = objects[objectId];
			deque<Request>& requests = object.requests;
			for (auto it = requests.begin(); it != requests.end();) {
				Request& request = *it;
				if (TIMESTAMP - request.arriveTime > EXTRA_TIME) {
					it++;
					requests.pop_front();
					objects[objectId].timeoutRequests.push(request);
					// 更新请求趋势图
					const int& tagId = objects[objectId].tagId;
					tagIdRequestNum[tagId]--;
				} else break;
			}
		}
	}
}

/// @brief 每隔 GAP 根据 tag 的请求趋势图【等信息】尝试更新（重置）所有磁头的起始 read 位置
/// NOTE: GAP 是需要调参的，确保这个间隔可以遍历完一个区间
/// NOTE: 每个磁盘都要尽量找最热门的标签，同时相同标签要隔 3 个盘，避免抢着干同一份工作。理论证明：3+3+3+1 最优。
void sync_update_disk_point_position() {
	traverse_all_disks_update_requests_num();

	static vector<int> hotTags(M + 1);
	// 根据区间价值进行排序
	for(int i = 1; i < hotTags.size(); ++i){
		hotTags[i] = i;
	}
	std::sort(hotTags.begin() + 1, hotTags.end(), [&](const int& x, const int& y) {
		#if 0
		const Tag& tag1 = tags[x], tag2 = tags[y];
		const int duration1 = tag1.endUnit - tag1.startUnit, duration2 = tag2.endUnit - tag2.startUnit;
		int rangeVal1 = 0, rangeVal2 = 0;
		for (int i = 1; i < disks.size(); ++i) {
			rangeVal1 += compute_range_value(i, { tag1.startUnit, tag1.endUnit });
			rangeVal2 += compute_range_value(i, { tag2.startUnit, tag2.endUnit });
		}
		return (static_cast<double>(rangeVal1) / duration1) > (static_cast<double>(rangeVal2) / duration2);
		#else
		int requestNum1 = tagIdRequestNum[x], requestNum2 = tagIdRequestNum[y];
		return requestNum1 > requestNum2;
		#endif
		});

	// 每一个磁头移动到相应 hotTag 的区间起始位置
	const int hotTagNum = 3;
	int hotTagStartIndex = rand() % hotTagNum + 1; // 索引 [1, hotTagNum]
	vector<int> hotTagIds = { 0, 1, 2, 1, 3, 2, 1, 3, 1, 2, 4 };
	for (int i = 1; i < disks.size(); ++i) {
		/// WARNING: 每个磁盘头都移动到一个 tag 的 startUnit，最小的数据集上，3 个磁盘只有 2 个 tag，不够分，所以报错！跑不了小数据集
		/// SOLVE: 避免 hotTag 的数量少于 磁盘数量 造成越界访问

		int tagId = hotTagStartIndex < hotTags.size() ? hotTags[hotTagStartIndex] : hotTags[hotTags.size() - 1];
		hotTagStartIndex = hotTagStartIndex % hotTagNum + 1;
		if (i == disks.size() - 1) tagId = hotTagNum + 1 < hotTags.size() ? hotTags[hotTagNum + 1] : hotTags[hotTags.size() - 1];

		const Tag& tag = tags[tagId];
		const int& startUnit = tag.startUnit;
		Disk& disk = disks[i];
		DiskPoint& diskPoint = disk.diskPoint;
		// 对于每一个磁头，计算消耗，判断是用 j or p
		int j = startUnit; // 优化，找到第一个需要读的位置跳，节约令牌
		while (!request_need_this_block(i, j)) {
			j = j % V + 1;
			if (j == startUnit) break; // 避免死循环，设置最大尝试次数
		}

		int distance = ((j - diskPoint.position) + V) % V; // 计算 pass 的步数。磁头只能向后 pass：startUnit - position > or < 0
		if (distance >= diskPoint.remainToken) { // jump
			if (!do_jump(i, j)) assert(false);
			continue;
		}
		while (distance--) {  // 非 jump 就 pass
			if (!do_pass(i)) assert(false);
		}
	}
}

// 读一个块时，需要判断其是第几个块，以便于把请求的 hasRead 相应位置置 true
int cal_block_id(const int& diskId, const int& unitId) {
	// 确保 object 不为 0，以防万一。主要是这个特况需要函数外部确认,即外部调用该函数时要确保这个磁盘位置放的有对象...
	assert(disks[diskId].diskUnits[unitId] != 0);

	const int& objectId = disks[diskId].diskUnits[unitId];
	const Object& object = objects[objectId];
	// 得到块的副本号
	int replicaId = 0;
	for (int i = 1; i < object.replicaDiskId.size(); ++i) {
		if (object.replicaDiskId[i] == diskId) {
			replicaId = i;
			break;
		}
	}
	assert(replicaId != 0);
	// 得到块的 blockId
	int blockId = 0;
	for (int i = 1; i < object.replicaBlockUnit[replicaId].size(); ++i) {
		assert(object.size + 1 == object.replicaBlockUnit[replicaId].size());
		if (object.replicaBlockUnit[replicaId][i] == unitId) {
			blockId = i;
			break;
		}
	}
	assert(blockId != 0); // 确保传入的 diskId、unitId、objectId 对的上，在 object 中有记录
	return blockId;
}

// 简单请求队列中是否需要这个块；同时处理超时请求
bool request_need_this_block(const int& diskId, const int& unitId) {
	if (disks[diskId].diskUnits[unitId] == 0) return false; // 先要判断 objectId ！= 0

	const int& objectId = disks[diskId].diskUnits[unitId];
	Object& object = objects[objectId];
	deque<Request>& requests = object.requests;
	// 先处理掉超时请求
	for (auto it = requests.begin(); it != requests.end(); ) {
		Request& request = *it;
		// 每次读取块时检查,超时的请求直接扔了丢入超时队列，也无需上报了（或许可以减轻 requests 队列，帮助 need_read 判断）
		if (TIMESTAMP - request.arriveTime > EXTRA_TIME) {
			it++;
			requests.pop_front();
			objects[objectId].timeoutRequests.push(request);
			// 更新请求趋势图
			const int& tagId = objects[objectId].tagId;
			tagIdRequestNum[tagId]--;
		} else break;
	}
	// 判断是否有未超时的请求需要该块
	for (auto it = requests.crbegin(); it != requests.crend(); it++) {   // 我用成 [crend(), crbegin())了...用反了
		const Request& request = *it;
		const auto& hasRead = request.hasRead;
		int blockId = cal_block_id(diskId, unitId);

		if (hasRead[blockId] == false) return true; // for 内部只执行一次，因为只需访问最后一个 request 即可得出答案。队列不可遍历或随机访问我改成了双端队列
		else return false;
	}
	return false;
}

/// @brief 遍历一棵高度为 DFS_DEPTH 的树，找到后 DFS_DEPTH 步里面令牌消耗最少的走法（只含p、r，如：prrprppprr）
/// @attention 若该单元格 request_need_this_block 则必须走 r
void dfs(int& minCost, string& minCostActions, int cost, string actions, char preAction, int preCost, int depth, const int& setDepth, const int& _diskId, int _unitId) {
	// 控制递归树高度，同时处理叶子节点
	if (depth == setDepth) {
		if (cost < minCost) { // 处理叶子节点
			minCost = cost;
			minCostActions = actions;
		}
		return; // 控制递归树高度
	}
	if (cost + (setDepth - depth) >= minCost) return; // 剪枝

	int nextUnitId = (_unitId % V) + 1;
	int thisCost = (preAction != 'r' || TIMESTAMP == 1) ? 64 : std::max(16, static_cast<int>(std::ceil(preCost * 0.8))); // 计算 r 的 cost
	if (request_need_this_block(_diskId, _unitId)) {
		// 只能选 r
		dfs(minCost, minCostActions, cost + thisCost, actions + "r", 'r', thisCost, depth + 1, setDepth, _diskId, nextUnitId);
	} else {
		// 可以选 p 或 r
		dfs(minCost, minCostActions, cost + 1, actions + "p", 'p', 1, depth + 1, setDepth, _diskId, nextUnitId);
		dfs(minCost, minCostActions, cost + thisCost, actions + "r", 'r', thisCost, depth + 1, setDepth, _diskId, nextUnitId);
	}
}

// 某一个块可能并不需要，但是为了保持连续阅读，有时也需要 read
bool determine_read(const int& _diskId, const int& _unitId, const int& _objectId) {
	static std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

	// if (TIMESTAMP < 10000) return request_need_this_block(_diskId, _unitId);
	if (request_need_this_block(_diskId, _unitId)) return true;

	const Disk& disk = disks[_diskId];
	const DiskPoint& diskPoint = disk.diskPoint;
	if (diskPoint.preAction != 'r') return false;
	if (diskPoint.preCostToken == 64 && !USE_DFS) return false;

	std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
	int durationSeconds = std::chrono::duration_cast<std::chrono::seconds>(end - begin).count();
	// 使用 DFS 判断是否需要读
	if (USE_DFS && durationSeconds <= 270) { // 255s 前用 DFS，时间不够了留 40s 够了能跑完
		int minCost = INT_MAX;
		string minCostActions = "";
		dfs(minCost, minCostActions, 0, "", diskPoint.preAction, diskPoint.preCostToken, 1, DFS_DEPTH, _diskId, _unitId);
		// assert(minCostActions.size() == DFS_DEPTH - 1);     	// dfs生效

		if (minCostActions[0] == 'p') return false;            	// 倾向于读, 这里条件太严苛, 导致 dfs 效果不明显。或许前几步有读就读？
		else if (minCostActions[0] == 'r') return true;
		else assert(false);
		
		return false;
	}
	// 使用简单策略判断是否需要读
	else {
		int unitId = _unitId;
		for (int i = 0; i < 8; ++i) { // // 调参。后 N 块只要有 1 块需要读，我就继续读。设想优化为后 N 块只要有 k 块需要读，我就继续读（可以设置一个比例，试了没太大用）
			unitId = unitId % V + 1;
			if (request_need_this_block(_diskId, unitId)) return true;
		}
		return false;
	}
}

// 检查 1 个 request 的 hasRead 数组，判断 request 是否完成
bool check_request_is_done(const Request& _request) {
	for (int i = 1; i < _request.hasRead.size(); ++i) {
		if (_request.hasRead[i] == false) return false;
	}
	return true;
}

void read_action() {
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
	if (TIMESTAMP % GAP == 0) {
		sync_update_disk_point_position();
	}

	vector<int> finishRequests;
	for (int i = 1; i < disks.size(); ++i) { // 每个磁头，串行开始读取
		const auto& diskUnits = disks[i].diskUnits;
		DiskPoint& diskPoint = disks[i].diskPoint;
		// 令牌未到山穷水尽之地就要一直尝试消耗
		while (true) {
			const int unitId = diskPoint.position;      // unitId 不可用引用，因为后面 cal_block_id 时磁头移动了
			const int& objectId = diskUnits[unitId];    // 注意： objectId 可能为 0
			// 需要 p、r 但令牌不够，这个磁盘磁头的动作结束         
			if (!determine_read(i, unitId, objectId)) {
				if (!do_pass(i)) break;
				else continue;
			}
			if (!do_read(i)) break;

			if (objectId == 0) continue; // 为了连续 read，空块也读。空块无需更新请求队列、上报请求等

			bool preCheck = true;
			// 遍历 requests 累积上报：每读一个块，就把 requests 队列中的 request 的所有相应位置置 true
			int blockId = cal_block_id(i, unitId); // unitId 不可换为 diskPoint.position！注意，读之后磁头后移了，找了一下午 bug！！！
			auto& requests = objects[objectId].requests;
			for (auto it = requests.begin(); it != requests.end(); ) {
				Request& request = *it;
				request.hasRead[blockId] = true;
				if (preCheck && check_request_is_done(request)) { // 如果某一次检查没过，其实就不必检查了，使用 preCheck 记录
					finishRequests.push_back(request.id);
					it++; // 防在 pop_front() 前面，以防不测...或者使用 erase()
					requests.pop_front();
					// 每上报一个请求，更新请求趋势图
					const int& tagId = objects[objectId].tagId;
					tagIdRequestNum[tagId]--;
				} else {
					preCheck = false;
					it++;
				}
			}
		}
	}
	// 输出 cmd、上报完成的请求
	for (int i = 1; i < disks.size(); ++i) {
		const Disk& disk = disks[i];
		const DiskPoint& diskPoint = disk.diskPoint;
		const string& cmd = diskPoint.cmd;
		for (int i = 0; i < cmd.size(); ++i) {
			printf("%c", cmd[i]);
		}
		if (cmd[0] != 'j') printf("#\n");
		else printf("\n");
	}
	printf("%d\n", static_cast<int>(finishRequests.size()));
	for (int i = 0; i < finishRequests.size(); ++i) {
		printf("%d\n", finishRequests[i]);
	}

	fflush(stdout);
}

int main() {
	scanf("%d%d%d%d%d", &T, &M, &N, &V, &G);

	init_global_container();
	pre_input_process();
	do_partition();

	for (int t = 1; t <= T + EXTRA_TIME; t++) {
		timestamp_action();
		delete_action();
		write_action();
		read_action();
	}

	return 0;
}