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

        // if no key is provided, find the leftmost child
        if (f == nullptr) {
            entry = &(*iter);
            return findLeafPage(tid, dirtypages, entry->getLeftChild(), Permissions::READ_ONLY, f);
        }

        // if key is provided
        while (iter != page->end()) {
            // entry now points to the same BTree Entry object that iter points to.
            entry = &(*iter);
            // if f <= entry key, look left
            if (f->compare(Op::LESS_THAN_OR_EQ, entry->getKey())) {
                return findLeafPage(tid, dirtypages, entry->getLeftChild(), Permissions::READ_ONLY, f);
            }
            ++iter;
        }
        // if we did not return anything from the loop, it means the key is the largest so we go to the right child of our last entry read.
        return findLeafPage(tid, dirtypages, entry->getRightChild(), Permissions::READ_ONLY, f);
    } 
}

BTreeLeafPage *BTreeFile::splitLeafPage(TransactionId tid, PagesMap &dirtypages, BTreeLeafPage *page, const Field *field) {
    // TODO pa2.3: implement
    BTreeLeafPage * rightPage = static_cast<BTreeLeafPage *>(getEmptyPage(tid, dirtypages, BTreePageType::LEAF));
    auto iter = page->rbegin();
    int count = 0;
    Tuple *currTuple = nullptr;
    while (iter != page->rend()) {
        currTuple = &(*iter);
        page->deleteTuple(currTuple);
        rightPage->insertTuple(currTuple);
        count++;
        if (count == page->getNumTuples() / 2) {
            break;
        }
        ++iter;
    }

    if (page->getRightSiblingId() != nullptr) {
        BTreePageId * oldRightId = page->getRightSiblingId();
        BTreeLeafPage * oldRightPage = static_cast<BTreeLeafPage *>(BTreeFile::getPage(tid, dirtypages, oldRightId, Permissions::READ_WRITE));
        oldRightPage->setLeftSiblingId(const_cast<BTreePageId*>(&rightPage->getId()));
    }

    rightPage->setLeftSiblingId(const_cast<BTreePageId*>(&page->getId()));
    rightPage->setRightSiblingId(page->getRightSiblingId());
    page->setRightSiblingId(const_cast<BTreePageId*>(&rightPage->getId()));

    Field * midKey = const_cast<Field *>(&(currTuple->getField(keyField)));
    BTreeEntry *entry = new BTreeEntry(midKey, const_cast<BTreePageId*>(&page->getId()), const_cast<BTreePageId*>(&rightPage->getId()));
    BTreeInternalPage * parentPage = getParentWithEmptySlots(tid, dirtypages, page->getParentId(), midKey);
    parentPage->insertEntry(*entry);
    updateParentPointer(tid, dirtypages, &parentPage->getId(), const_cast<BTreePageId*>(&page->getId()));
    updateParentPointer(tid, dirtypages, &parentPage->getId(), const_cast<BTreePageId*>(&rightPage->getId()));

    if (field->compare(Op::LESS_THAN_OR_EQ, midKey)) return page;
    return rightPage;
}

BTreeInternalPage *BTreeFile::splitInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                                Field *field) {
    // TODO pa2.3: implement
    BTreeInternalPage * rightPage = static_cast<BTreeInternalPage *>(getEmptyPage(tid, dirtypages, BTreePageType::INTERNAL));
    auto iter = page->rbegin();
    int count = 0;
    BTreeEntry *currEntry = nullptr;
    while (iter != page->rend()) {
        currEntry = &(*iter);
        page->deleteKeyAndRightChild(currEntry);
        rightPage->insertEntry(*currEntry);
        count++;
        if (count == page->getNumEntries() / 2) {
            break;
        }
        ++iter;
    }

    BTreeEntry *e = &(*iter);
    Field * midKey = e->getKey();
    page->deleteKeyAndRightChild(e);
    BTreeEntry *entry = new BTreeEntry(midKey, const_cast<BTreePageId*>(&page->getId()), const_cast<BTreePageId*>(&rightPage->getId()));
    BTreeInternalPage * parentPage = getParentWithEmptySlots(tid, dirtypages, page->getParentId(), midKey);
    parentPage->insertEntry(*entry);
    updateParentPointer(tid, dirtypages, &parentPage->getId(), const_cast<BTreePageId*>(&parentPage->getId()));
    updateParentPointer(tid, dirtypages, &parentPage->getId(), const_cast<BTreePageId*>(&rightPage->getId()));

    if (field->compare(Op::LESS_THAN_OR_EQ, midKey)) return page;
    return rightPage;
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