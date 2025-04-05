#include <iomanip>
#include "dataframe.h"
#include <exception>

BaseColumn::BaseColumn(const std::string &id, int pos, const std::string &dt)
    : identifier(id), position(pos), dataType(dt) {
}

BaseColumn::~BaseColumn() {
}

std::string BaseColumn::getIdentifier() const {
    return identifier;
}

std::string BaseColumn::getTypeName() const {
    return dataType;
}

// Se há zero colunas, seta o tamanho do dataframe para o da coluna. Caso contrário, verifica se a coluna bate com o tamanho
void DataFrame::addColumn(std::shared_ptr<BaseColumn> column) {
    if (columns.empty()){
        columns.push_back(column);
        dataFrameSize = column->size();
        // Preenche o vetor rowOrder automaticamente com os índices reais
        rowOrder.resize(dataFrameSize);
        for (size_t i = 0; i < dataFrameSize; ++i) {
            rowOrder[i] = i;
        }
    }
    else {
        if (column->size() != dataFrameSize){
            throw "Tried to add a column that doesn't have the number of rows of the dataframe";
        } else {
            columns.push_back(column);
        }
    }
}

void DataFrame::addRow(const std::vector<std::any> &row) {
    for (size_t i = 0; i < columns.size(); ++i) {
        if (row[i].has_value() && row[i].type() == typeid(std::nullptr_t)) {
            columns[i]->appendNA();
            continue;
        }
        columns[i]->addAny(row[i]);
    }
    rowOrder.push_back(dataFrameSize); // Adiciona o índice "real" da nova linha
    dataFrameSize++;
}

void DataFrame::addRow(const std::vector<std::string> &row) {
    for (size_t i = 0; i < columns.size(); ++i) {
        if (row[i] == "") {
            columns[i]->appendNA();
            continue;
        }
        columns[i]->addAny(row[i]);
    }
    rowOrder.push_back(dataFrameSize); // add o índice "real" da nova linha
    dataFrameSize++;
}

void DataFrame::addRow(const std::vector<variantRow> &row) {
    for (size_t i = 0; i < columns.size(); ++i) {
        if (std::holds_alternative<std::nullptr_t>(row[i])) {
            columns[i]->appendNA();
            continue;
        }
        columns[i]->addAny(row[i]);
    }
    rowOrder.push_back(dataFrameSize); // add o índice "real" da nova linha
    dataFrameSize++;
}

std::shared_ptr<BaseColumn> DataFrame::getColumn(size_t index) const {
    if (index >= columns.size()) {
        throw std::out_of_range("BaseColumn index out of DataFrame bounds.");
    }
    return columns[index];
}

// relative INdex
std::vector<std::string> DataFrame::getRow(size_t relIndex) const {
    if(relIndex >= rowOrder.size()){
        throw std::out_of_range("Relative index out of bounds.");
    }
    size_t actualIndex = rowOrder[relIndex];
    std::vector<std::string> rowData;
    if (columns.empty())
        return rowData;
    for (const auto &col : columns) {
        if (actualIndex < col->size()) {
            rowData.push_back(col->getValue(actualIndex));
        } else {
            rowData.push_back("N/A");
        }
    }
    return rowData;
}


std::string DataFrame::toString() const {
    std::ostringstream oss;
    size_t nRows = 0;
    size_t nCols = columns.size();

    for (const auto &col : columns) {
        if (col->size() > nRows) {
            nRows = col->size();
        }
    }

    oss << "| ";
    for (const auto &col : columns) {
        oss << std::setw(10) << std::left << col->getIdentifier() << " | ";
    }
    oss << "\n";

    oss << "|" <<std::string(13*nCols-1, '=') << "|\n";

    for (size_t i = 0; i < nRows; ++i) {
        oss << "| ";
        std::vector<std::string> row = getRow(i);
        for (const auto &value : row) {
            oss << std::setw(10) << std::left << value << " | ";
        }
        oss << "\n";
    }
    return oss.str();
}

// Retorna o vetor de índices das linhas
std::vector<size_t> DataFrame::getRowOrder() const {
    return rowOrder;
}

void DataFrame::setRowOrder(const std::vector<size_t>& newOrder) {
    if(newOrder.size() != rowOrder.size()){
        throw std::invalid_argument("New row order must have the same size as the current row order.");
    }
    rowOrder = newOrder;
}

void DataFrame::resetRowOrder() {
    rowOrder.clear();
    rowOrder.resize(dataFrameSize);
    for (size_t i = 0; i < dataFrameSize; ++i) {
        rowOrder[i] = i;
    }
}
