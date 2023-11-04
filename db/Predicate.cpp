#include <db/Predicate.h>

using namespace db;

// TODO pa2.2: implement
Predicate::Predicate(int field, Op op, const Field *operand) {
    this->field = field;
    this->op = op;
    this->operand = operand;
}

int Predicate::getField() const {
    // TODO pa2.2: implement
    return field;
}

Op Predicate::getOp() const {
    // TODO pa2.2: implement
    return op;
}

const Field *Predicate::getOperand() const {
    // TODO pa2.2: implement
    return operand;
}

bool Predicate::filter(const Tuple &t) const {
    // TODO pa2.2: implement
    const Field &tupleField = t.getField(field);
    return tupleField.compare(op, operand);
}
