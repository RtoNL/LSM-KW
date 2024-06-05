#include "SSTable.h"
#include <fstream>
#include <iostream>
#include <string.h>
/*
 * 该构造函数通过 SSTableCache 对象来初始化一个 SSTable 对象。它从指定的路径加载数据，将文件中的数据条目读入内存中的 entries 列表。
读取文件失败时会打印错误信息并退出。
 */
SSTable::SSTable(SSTableCache *cache)
{
     std::ifstream file(path, std::ios::binary);
    if(!file) {
        printf("Fail to open file %s", path.c_str());
        exit(-1);
    }
    timeStamp = (cache->header).timeStamp;
    length = (cache->header).size;

    // load from files
    file.seekg((cache->indexes)[0].offset);
    for(auto it = (cache->indexes).begin();;) {
        uint64_t key = (*it).key;
        uint32_t offset = (*it).offset;
        std::string val;
        if(++it == (cache->indexes).end()) {
            file >> val;
            entries.push_back(Entry(key, val));
            break;
        } else {
            uint32_t length = (*it).offset - offset;
            char *buf = new char[length + 1];
            buf[length] = '\0';
            file.read(buf, length);
            val = buf;
            delete[] buf;
            entries.push_back(Entry(key, val));
        }
    }

    delete cache;
}
/*
 * 这是一个静态方法，用于合并多个 SSTable 对象。合并过程是递归的，每次合并成对的 SSTable 对象，直到只剩一个。
 */
void SSTable::merge(std::vector<SSTable> &tables)
{
    uint32_t size = tables.size();
    if(size == 1)
        return;
    uint32_t group = size/2;
    std::vector<SSTable> next;
    for(uint32_t i = 0; i < group; ++i) {
        next.push_back(merge2(tables[2*i], tables[2*i + 1]));
    }
    if(size % 2 == 1)
        next.push_back(tables[size - 1]);
    merge(next);
    tables = next;
}
/*
 * 这是一个静态方法，用于合并两个 SSTable 对象。合并后的结果包含所有来自 a 和 b 的条目，且条目按键排序。
 */
SSTable SSTable::merge2(SSTable &a, SSTable &b)
{
    SSTable result;
    result.timeStamp = a.timeStamp;
    while((!a.entries.empty()) && (!b.entries.empty())) {
        uint64_t aKey = a.entries.front().key, bKey = b.entries.front().key;
        if(aKey > bKey) {
            result.entries.push_back(b.entries.front());
            b.entries.pop_front();
        } else if(aKey < bKey) {
            result.entries.push_back(a.entries.front());
            a.entries.pop_front();
        } else {
            result.entries.push_back(a.entries.front());
            a.entries.pop_front();
            b.entries.pop_front();
        }
    }
    while(!a.entries.empty()){
        result.entries.push_back(a.entries.front());
        a.entries.pop_front();
    }
    while(!b.entries.empty()){
        result.entries.push_back(b.entries.front());
        b.entries.pop_front();
    }
    return result;
}
/*
 * 将 SSTable 中的数据保存到指定目录下的多个 SSTable 文件中，并返回 SSTableCache 对象的向量。这些 SSTable 文件大小受 MAX_TABLE_SIZE 限制。
 */
std::vector<SSTableCache*> SSTable::save(const std::string &dir)
{
    std::vector<SSTableCache*> caches;
    SSTable newTable;
    uint64_t num = 0;
    while(!entries.empty()) {
        if(newTable.size + 12 + entries.front().val.size() >= MAX_TABLE_SIZE) {
            caches.push_back(newTable.saveSingle(dir, timeStamp, num++));
            newTable = SSTable();
        }
        newTable.add(entries.front());
        entries.pop_front();
    }
    if(newTable.length > 0) {
        caches.push_back(newTable.saveSingle(dir, timeStamp, num));
    }
    return caches;
}
/*
 * 向 SSTable 中添加一个数据条目，同时更新 SSTable 的大小和长度。
 */
void SSTable::add(const Entry &entry)
{
    size += 12 + entry.val.size();
    length++;
    entries.push_back(entry);
}
/*
 * 将单个 SSTable 保存到指定目录中，生成相应的 SSTableCache 对象。包括创建布隆过滤器、生成索引并将数据写入磁盘。
 */
SSTableCache *SSTable::saveSingle(const std::string &dir, const uint64_t &currentTime, const uint64_t &num)
{
    SSTableCache *cache = new SSTableCache;

    char *buffer = new char[size];
    BloomFilter *filter = cache->bloomFilter;

    *(uint64_t*)buffer = currentTime;
    (cache->header).timeStamp = currentTime;

    *(uint64_t*)(buffer + 8) = length;
    (cache->header).size = length;

    *(uint64_t*)(buffer + 16) = entries.front().key;
    (cache->header).minKey = entries.front().key;

    char *index = buffer + 10272;
    uint32_t offset = 10272 + length * 12;

    for(auto it = entries.begin(); it != entries.end(); ++it) {
        filter->add((*it).key);
        *(uint64_t*)index = (*it).key;
        index += 8;
        *(uint32_t*)index = offset;
        index += 4;

        (cache->indexes).push_back(Index((*it).key, offset));
        uint32_t strLen = ((*it).val).size();
        uint32_t newOffset = offset + strLen;
        if(newOffset > size) {
            printf("Buffer Overflow!!!\n");
            exit(-1);
        }
        memcpy(buffer + offset, ((*it).val).c_str(), strLen);
        offset = newOffset;
    }

    *(uint64_t*)(buffer + 24) = entries.back().key;
    (cache->header).maxKey = entries.back().key;
    filter->save2Buffer(buffer + 32);

    std::string filename = dir + "/" + std::to_string(currentTime) + "-" + std::to_string(num) + ".sst";
    cache->path = filename;
    std::ofstream outFile(filename, std::ios::binary | std::ios::out);
    outFile.write(buffer, size);

    delete[] buffer;
    outFile.close();
    return cache;
}
/*
 * 通过指定路径初始化 SSTableCache 对象。它从文件中加载头部信息、布隆过滤器和索引数据。
 */
SSTableCache::SSTableCache(const std::string &dir)
{
    path = dir;
    std::ifstream file(dir, std::ios::binary);
    if(!file) {
        printf("Fail to open file %s", dir.c_str());
        exit(-1);
    }
    // load header
    file.read((char*)&header.timeStamp, 8);
    file.read((char*)&header.size, 8);
    uint64_t length = header.size;
    file.read((char*)&header.minKey, 8);
    file.read((char*)&header.maxKey, 8);

    // load bloom filter
    char *filterBuf = new char[FILTER_SIZE/8];
    file.read(filterBuf, FILTER_SIZE/8);
    bloomFilter = new BloomFilter(filterBuf);


    char *indexBuf = new char[length * 12];
    file.read(indexBuf, length * 12);
    for(uint32_t i = 0; i < length; ++i) {
        indexes.push_back(Index(*(uint64_t*)(indexBuf + 12*i), *(uint32_t*)(indexBuf + 12*i + 8)));
    }

    delete[] filterBuf;
    delete[] indexBuf;
    file.close();

}
/*
 * 根据键查找数据。如果布隆过滤器和索引都指示键存在，则进一步读取数据。否则返回 -1。
 */
int SSTableCache::get(const uint64_t &key)
{
    if(!bloomFilter->contains(key))
        return -1;
    return find(key, 0, indexes.size() - 1);
}
/*
 * 使用二分查找在索引中定位键的位置。如果找到键则返回其索引，否则返回 -1。
 */
int SSTableCache::find(const uint64_t &key, int start, int end)
{
    if(start > end)
        return -1;
    if(start == end) {
        if(indexes[start].key == key)
            return start;
        else
            return -1;
    }
    int mid = (start + end) / 2;
    if(indexes[mid].key == key)
        return mid;
    else if(indexes[mid].key < key)
        return find(key, mid + 1, end);
    else
        return find(key, start, mid - 1);
}
/*
 * 比较两个 SSTableCache 对象的时间戳，用于排序。
 */
bool cacheTimeCompare(SSTableCache *a, SSTableCache *b)
{
    return (a->header).timeStamp > (b->header).timeStamp;
}
/*
 * 比较两个 SSTable 对象的时间戳，用于排序。
 */

bool tableTimeCompare(SSTable &a, SSTable &b)
{
    return a.timeStamp > b.timeStamp;
}
/*
 * 检查 SSTableCache 的键范围与给定范围集合是否有交集。
 */
bool haveIntersection(const SSTableCache *cache, const std::vector<Range> &ranges)
{
    uint64_t min = (cache->header).minKey, max = (cache->header).maxKey;
    for(auto it = ranges.begin(); it != ranges.end(); ++it) {
        if(!(((*it).max < min) || ((*it).min > max))) {
            return true;
        }
    }
    return false;
}
