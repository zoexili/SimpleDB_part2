#include <db/IndexPredicate.h>

using namespace db;

IndexPredicate::IndexPredicate(Op op, const Field *fvalue) {
    // TODO pa2.2: implement
    this->op = op;
    this->fvalue = fvalue;
}

const Field *IndexPredicate::getField() const {
    // TODO pa2.2: implement
    return fvalue;
}

Op IndexPredicate::getOp() const {
    // TODO pa2.2: implement
    return op;
}

bool IndexPredicate::operator==(const IndexPredicate &other) const {
    // TODO pa2.2: implement
    if (other.fvalue == this->fvalue && other.op == this->op) {
        return true;
    }
    return false;
}

std::size_t std::hash<IndexPredicate>::operator()(const IndexPredicate &ipd) const {
    // TODO pa2.2: implement
    return std::hash<string>()(to_string(ipd.getOp())+"@"+ipd.getField()->to_string());
}
