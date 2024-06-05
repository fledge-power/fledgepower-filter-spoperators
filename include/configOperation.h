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
#include <vector>
#include <map>
#include <set>
#include <string>

struct DataOperationInfo {
    std::string operationType;
    std::vector<std::string> inputPivotIds;
    std::string outputPivotType;
    std::string outputAssetName;
};

class ConfigOperation {
public:  
    void importExchangedData(const std::string & exchangeConfig);
    const std::vector<std::string>& getOutputIdsForInputId(const std::string& inputId);

    const std::map<std::string, DataOperationInfo>& getDataOperations() { return m_dataOperation; };
    
private:
    // Stores for each output PivotID the data used to compute its operation
    std::map<std::string, DataOperationInfo> m_dataOperation;
    // Lookup table to get the output PivotIDs from one of the inputs PivotIDs
    std::map<std::string, std::vector<std::string>> m_dataOperationLookup;
    // List of operations supported
    const std::set<std::string> m_supportedOperationTypes = {"or"};
};

#endif  // INCLUDE_CONFIG_OPERATION_H_