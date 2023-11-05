#include <db/BufferPool.h>
#include <db/Database.h>

using namespace db;

// find the first page in pages, flush it if dirty, then remove it. Break the loop.
void BufferPool::evictPage()
{
    // TODO pa2.1: implement
    for (const auto item : pages) {
        if (item.second->isDirty() != std::nullopt) {
            flushPage(item.first);
        }
        discardPage(item.first);
        break;
    }
}

// Flush all dirty pages to disk.
void BufferPool::flushAllPages()
{
    // TODO pa2.1: implement
    for (const auto item: pages) {
        if (item.second->isDirty() != std::nullopt) {
            flushPage(item.first);
        }
    }
}

// remove page from bufferpool
void BufferPool::discardPage(const PageId *pid)
{
    // TODO pa2.1: implement
    auto pageIter = pages.find(pid);
    if (pageIter != pages.end()) {
        pages.erase(pageIter); 
    }
}

// write dirty (modified) page back to disk, mark page not dirty
void BufferPool::flushPage(const PageId *pid)
{
    // TODO pa2.1: implement
    // check if page in the buffer pool
    auto pageIter = pages.find(pid);
    // if reach to the end, return
    if (pageIter == pages.end()) return;
    Page* page = pages[pid];
    // if page is dirty, write it back to disk
    if (page->isDirty().has_value()) {
        Database::getCatalog().getDatabaseFile(pid->getTableId())->writePage(page);
        // mark page not dirty
        page->markDirty(std::nullopt);
    }
}

// isDirty() return dirty which equals tid if markDirty(tid)
void BufferPool::flushPages(const TransactionId &tid)
{
    // TODO pa2.1: implement
    for (const auto item : pages) {
        if (item.second->isDirty() == tid) {
            flushPage(item.first);
        }
    }
}

void BufferPool::insertTuple(const TransactionId &tid, int tableId, Tuple *t)
{
    // TODO pa2.3: implement
    std::vector<Page *> changedPages = Database::getCatalog().getDatabaseFile(tableId)->insertTuple(tid, *t);
    for (const auto item: changedPages) {
        item->markDirty(tid);
    }
}

void BufferPool::deleteTuple(const TransactionId &tid, Tuple *t)
{
    // TODO pa2.3: implement
    std::vector<Page *> changedPages = Database::getCatalog().getDatabaseFile(t->getRecordId()->getPageId()->getTableId())->deleteTuple(tid, *t);
    for (const auto item: changedPages) {
        item->markDirty(tid);
    }
}
