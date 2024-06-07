#ifndef INCLUDE_CONFIG_OPERATION_H_
#define INCLUDE_CONFIG_OPERATION_H_

/*
 * Import confiugration Exchanged data
 *
 * Copyright (c) 2020, RTE (https://www.rte-france.com)
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Yannick Marchetaux
 * 
 */
#include <rapidjson/document.h>

#include <vector>
#include <map>
#include <set>
#include <string>

struct OperationInfo {
    std::string operationType;
    std::vector<std::string> inputPivotIds;
};

struct OperationsInfo {
    std::vector<OperationInfo> operations;
    std::string outputPivotType;
    std::string outputAssetName;
};

struct OperationInfoLookup {
    std::string outputPivotId;
    int operationIndex = 0;
    OperationInfoLookup(const std::string& in_outputPivotId, int in_operationIndex):
        outputPivotId(in_outputPivotId),
        operationIndex(in_operationIndex) {}
};

class ConfigOperation {
public:  
    void importExchangedData(const std::string & exchangeConfig);
    const std::vector<OperationInfoLookup>& getOperationsForInputId(const std::string& inputId) const;

    const std::map<std::string, OperationsInfo>& getDataOperations() const { return m_dataOperation; };
    
private:
    void importDataPoint(const rapidjson::Value& datapoint, std::set<std::string>& foundPivotIds);
    bool importOperation(rapidjson::Value::ConstValueIterator itr, OperationInfo& out_operationInfo);
    // Stores for each output PivotID the data used to compute its operation
    std::map<std::string, OperationsInfo> m_dataOperation;
    // Lookup table to get the list of output PivotID and operation index pairs from one of the inputs PivotIDs
    std::map<std::string, std::vector<OperationInfoLookup>> m_dataOperationLookup;
    // List of operations supported
    const std::set<std::string> m_supportedOperationTypes = {"or"};
};

#endif  // INCLUDE_CONFIG_OPERATION_H_