#include <db/BTreeFile.h>
#include <db/Database.h>
#include <cassert>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace db;

BTreeLeafPage *BTreeFile::findLeafPage(TransactionId tid, PagesMap &dirtypages, BTreePageId *pid, Permissions perm,
                                       const Field *f) {
    // TODO pa2.2: implement
    if (pid->getType() == BTreePageType::LEAF) {
        return static_cast<BTreeLeafPage *>(BTreeFile::getPage(tid, dirtypages, pid, perm));
    } else {
        BTreeInternalPage * page = static_cast<BTreeInternalPage *>(BTreeFile::getPage(tid, dirtypages, pid, Permissions::READ_ONLY));
        BTreeEntry * entry = nullptr;
        auto iter = page->begin();
        while (iter != page->end()) {
            // entry now points to the same BTree Entry object that iter points to.
            entry = &(*iter);
            if (f == nullptr || f->compare(Op::LESS_THAN_OR_EQ, entry->getKey())) {
                return findLeafPage(tid, dirtypages, entry->getLeftChild(), Permissions::READ_ONLY, f);
            }
        }
        return findLeafPage(tid, dirtypages, entry->getRightChild(), Permissions::READ_ONLY, f);
    } 
}

BTreeLeafPage *BTreeFile::splitLeafPage(TransactionId tid, PagesMap &dirtypages, BTreeLeafPage *page, const Field *field) {
    // TODO pa2.3: implement
    return nullptr;
}

BTreeInternalPage *BTreeFile::splitInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                                Field *field) {
    // TODO pa2.3: implement
    return nullptr;
}

void BTreeFile::stealFromLeafPage(BTreeLeafPage *page, BTreeLeafPage *sibling, BTreeInternalPage *parent,
                                  BTreeEntry *entry, bool isRightSibling) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::stealFromLeftInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                          BTreeInternalPage *leftSibling, BTreeInternalPage *parent,
                                          BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::stealFromRightInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                           BTreeInternalPage *rightSibling, BTreeInternalPage *parent,
                                           BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::mergeLeafPages(TransactionId tid, PagesMap &dirtypages, BTreeLeafPage *leftPage,
                               BTreeLeafPage *rightPage, BTreeInternalPage *parent, BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::mergeInternalPages(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *leftPage,
                                   BTreeInternalPage *rightPage, BTreeInternalPage *parent, BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

BTreeFile::BTreeFile(const char *fname, int key, const TupleDesc &td) : keyField(key), td(td) {
    fd = open(fname, O_RDWR | O_CREAT | O_APPEND | O_TRUNC, 0644);
    if (fd == -1) {
        throw std::runtime_error("open");
    }
    tableid = std::hash<std::string>{}(fname);
}