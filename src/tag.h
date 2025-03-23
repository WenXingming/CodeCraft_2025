#pragma once
#include "global.h"

// init tags
void sort_tags();
void do_partition();
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

    sort_tags();
    do_partition();
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

        if(i == M && tags[i].endUnit > V) tags[i].endUnit = V;  // 不用 *0.9 分空闲分区（用所有硬盘空间分区）的话，就要检查
    }
    /* // TEST:
    printf("Test: ===========================\n");
    for (int i = 1; i < tags.size(); ++i){
        printf("%d\n", tagSpaces[i]);
        printf("tag %d's startUnit: %d, endUnit: %d\n", tags[i].id, tags[i].startUnit, tags[i].endUnit);
    } */
}