#include "SkipList.h"
#include "BloomFilter.h"
#include <fstream>
#include <stdlib.h>
#include <string.h>
/*
 * The SkipList class provides an efficient way to manage a collection of key-value pairs with fast insertion, deletion,
 * and lookup operations. It also supports saving its contents to an SSTable, which can be useful for persistent storage
 * in a key-value store. The integration with BloomFilter ensures efficient key existence checks, reducing unnecessary
 * disk reads.
 */

/*
 * Constructor: Initializes the SkipList by inserting a collection of entries.
 */
SkipList::SkipList(std::vector<Entry> entrys)//SkipList::SkipList 表示这是 SkipList 类的构造函数的定义
{
    for(auto it = entrys.begin(); it != entrys.end(); ++it)
        put((*it).key, (*it).val);//*it 解引用迭代器 it，访问当前 Entry 元素。使用C++的箭头操作符，可以进一步简化访问元素的成员变量:put(it->key, it->val);
}

SkipList::~SkipList()
{
    Node *del, *h;
    del = head;  // 初始化 del 指向头节点
    h = head;    // 初始化 h 指向头节点
    while (del) {  // 外层循环：处理每一层
        h = h->down;  // 移动 h 到下一层
        while (del) {  // 内层循环：删除当前层的每个节点
            Node *next = del->right;  // 存储右侧节点
            delete del;  // 删除当前节点
            del = next;  // 更新 del 为右侧节点
        }
        del = h;  // 更新 del 为下一层的头节点
    }
}

/*
 * length(): Returns the number of entries in the skip list.
 */
uint64_t SkipList::length()
{
    return listLength;//listLength 是跳表中键值对的数量，例如 100。
}
/*
 * size(): Returns the total size of the skip list in bytes.
 */
uint64_t SkipList::size()
{
    return listSize;//listSize 是跳表占用的总内存大小，例如 4096 字节。
}

/*
 * get(): Searches for a key in the skip list and returns a pointer to the value if found, otherwise returns nullptr.
 */
std::string *SkipList::get(const uint64_t &key) const//& 用于表示参数是引用。避免拷贝：由于 `key` 是按引用传递的，没有对参数进行拷贝，这在处理大对象时尤其重要。保持不可变：由于 `key` 是 `const` 的，函数内部不能修改这个参数的值，确保了数据的安全性和一致性。
{
    bool found = false;
    Node *cur = head, *tmp;//声明了两个指向 Node 类型对象的指针变量：cur 和 tmp。cur 被初始化为 head 的值，而 tmp 没有被初始化 Node *tmp

    // 外层循环遍历层级
    while(cur) {
        // 内层循环在当前层级进行水平遍历
        while((tmp = cur->right) && tmp->key < key) {
            cur = tmp;  // 向右移动到下一个节点
        }
        // 如果找到匹配的键
        if((tmp = cur->right) && tmp->key == key) {
            cur = tmp;
            found = true;
            break;
        }
        // 向下移动到下一层级
        cur = cur->down;
    }

    // 返回结果
    if(found)
        return &(cur->val);//&是取地址运算符。返回&(cur->val)表示返回指向节点值（val）的指针
    else
        return nullptr;
}

/*
 * put(): Inserts or updates a key-value pair in the skip list. If the key already exists, it updates the value.
 * Otherwise, it inserts a new node, possibly adding a new level to the skip list.
 */
// 返回表中原来是否有该key
bool SkipList::put(const uint64_t &key, const std::string &val)
{
    std::vector<Node*> pathList;    //从上至下记录搜索路径
    Node *p = head;
    Node *tmp;
    while(p){
        while((tmp = p->right) && tmp->key < key) {
            p = tmp;
        }
        // 如果list中已有该key，则替换
        if ((tmp = p->right) && tmp->key == key) {
            p = tmp;
            if (p->val == val)
                return false;
            listSize = listSize - (p->val).size() + val.size();
            while (p) {
                p->val = val;
                p = p->down;
            }
            return true;
        }
        pathList.push_back(p);
        p = p->down;
    }

    bool insertUp = true;
    Node* downNode= nullptr;
    while(insertUp && pathList.size() > 0){   //从下至上搜索路径回溯，50%概率
        Node *insert = pathList.back();
        pathList.pop_back();
        insert->right = new Node(insert->right, downNode, key, val); //add新结点
        downNode = insert->right;    //把新结点赋值为downNode
        insertUp = (rand()&1);   //50%概率
    }
    if(insertUp){  //插入新的头结点，加层
        Node * oldHead = head;
        head = new Node();
        head->right = new Node(NULL, downNode, key, val);
        head->down = oldHead;
    }
    ++listLength;
    // 加一个key、一个offset、一个string的大小
    listSize += 12 + val.size();
    return false;
}

// 返回是否成功删除（原表中是否有该key）
bool SkipList::remove(const uint64_t &key)
{
    return put(key, "~DELETED~");
}

Node *SkipList::getListHead()
{
    Node *cur = head;
    while(cur->down) {
        cur = cur->down;
    }
    return cur->right;
}
/*
 * Saves the contents of the skip list to an SSTable. It creates a buffer, fills it with the skip list data, and writes the buffer to a file. It also updates the SSTableCache with metadata such as the Bloom filter and key indices.
 */
SSTableCache *SkipList::save2SSTable(const std::string &dir, const uint64_t &currentTime)
{
    SSTableCache *cache = new SSTableCache;

    Node *cur = getListHead();
    char *buffer = new char[listSize];
    BloomFilter *filter = cache->bloomFilter;

    *(uint64_t*)buffer = currentTime;
    (cache->header).timeStamp = currentTime;

    *(uint64_t*)(buffer + 8) = listLength;
    (cache->header).size = listLength;

    *(uint64_t*)(buffer + 16) = cur->key;
    (cache->header).minKey = cur->key;

    char *index = buffer + 10272;
    uint32_t offset = 10272 + listLength * 12;
    while(true) {
        filter->add(cur->key);
        *(uint64_t*)index = cur->key;
        index += 8;
        *(uint32_t*)index = offset;
        index += 4;

        (cache->indexes).push_back(Index(cur->key, offset));
        uint32_t strLen = (cur->val).size();
        uint32_t newOffset = offset + strLen;
        if(newOffset > listSize) {
            printf("Buffer Overflow!!!\n");
            exit(-1);
        }
        memcpy(buffer + offset, (cur->val).c_str(), strLen);
        offset = newOffset;
        if(cur->right)
            cur = cur->right;
        else
            break;
    }
    *(uint64_t*)(buffer + 24) = cur->key;
    (cache->header).maxKey = cur->key;
    filter->save2Buffer(buffer + 32);

    std::string filename = dir + "/" + std::to_string(currentTime) + ".sst";
    cache->path = filename;
    std::ofstream outFile(filename, std::ios::binary | std::ios::out);
    outFile.write(buffer, listSize);

    delete[] buffer;
    outFile.close();
    return cache;
}
