#include "configOperation.h"
#include "constantsOperation.h"
#include "utilityOperation.h"

#include <rapidjson/error/en.h>

using namespace std;
using namespace rapidjson;

/**
 * Import data in the form of Exchanged_data
 * The data is saved in a maps m_dataOperation
 * 
 * @param exchangeConfig : configuration Exchanged_data as a string 
*/
void ConfigOperation::importExchangedData(const string & exchangeConfig) {
    std::string beforeLog = ConstantsOperation::NamePlugin + " - ConfigOperation::importExchangedData :";
    m_dataOperation.clear();
    m_dataOperationLookup.clear();
    Document document;

    if (document.Parse(exchangeConfig.c_str()).HasParseError()) {
        UtilityOperation::log_fatal("%s Parsing error in exchanged_data json, offset %u: %s", beforeLog.c_str(),
                                static_cast<unsigned>(document.GetErrorOffset()), GetParseError_En(document.GetParseError()));
        return;
    }

    if (!document.IsObject()) {
        UtilityOperation::log_fatal("%s Root is not an object", beforeLog.c_str());
        return;
    }

    if (!document.HasMember(ConstantsOperation::JsonExchangedData) || !document[ConstantsOperation::JsonExchangedData].IsObject()) {
        UtilityOperation::log_fatal("%s %s does not exist or is not an object", beforeLog.c_str(), ConstantsOperation::JsonExchangedData);
        return;
    }
    const Value& exchangeData = document[ConstantsOperation::JsonExchangedData];

    if (!exchangeData.HasMember(ConstantsOperation::JsonDatapoints) || !exchangeData[ConstantsOperation::JsonDatapoints].IsArray()) {
        UtilityOperation::log_fatal("%s %s does not exist or is not an array", beforeLog.c_str(), ConstantsOperation::JsonDatapoints);
        return;
    }
    const Value& datapoints = exchangeData[ConstantsOperation::JsonDatapoints];
    std::set<std::string> foundPivotIds;
    for (const Value& datapoint : datapoints.GetArray()) {
        importDataPoint(datapoint, foundPivotIds);
    }

    // Sanity check on the input Pivot IDs listed
    for(const auto& kvp: m_dataOperationLookup) {
        if(foundPivotIds.count(kvp.first) == 0) {
            UtilityOperation::log_warn("%s An operation is configured for unexisting Pivot ID '%s'", beforeLog.c_str(), kvp.first.c_str());
        }
    }
}

/**
 * Import a datapoint found in Exchanged_data
 * The data is saved in a maps m_dataOperation
 * 
 * @param datapoint : datapoint to import
 * @param out_foundPivotIds : Out parameter storing any pivot ID found in the configuration
*/
void ConfigOperation::importDataPoint(const Value& datapoint, std::set<std::string>& out_foundPivotIds) {
    std::string beforeLog = ConstantsOperation::NamePlugin + " - ConfigOperation::importDataPoint :";
    if (!datapoint.IsObject()) {
        UtilityOperation::log_error("%s %s element is not an object", beforeLog.c_str(), ConstantsOperation::JsonDatapoints);
        return;
    }
    
    if (!datapoint.HasMember(ConstantsOperation::JsonPivotType) || !datapoint[ConstantsOperation::JsonPivotType].IsString()) {
        UtilityOperation::log_error("%s %s does not exist or is not a string", beforeLog.c_str(), ConstantsOperation::JsonPivotType);
        return;
    }

    if (!datapoint.HasMember(ConstantsOperation::JsonPivotId) || !datapoint[ConstantsOperation::JsonPivotId].IsString()) {
        UtilityOperation::log_error("%s %s does not exist or is not a string", beforeLog.c_str(), ConstantsOperation::JsonPivotId);
        return;
    }

    if (!datapoint.HasMember(ConstantsOperation::JsonLabel) || !datapoint[ConstantsOperation::JsonLabel].IsString()) {
        UtilityOperation::log_error("%s %s does not exist or is not a string", beforeLog.c_str(), ConstantsOperation::JsonLabel);
        return;
    }

    std::string outputPivotId = datapoint[ConstantsOperation::JsonPivotId].GetString();
    out_foundPivotIds.insert(outputPivotId);

    if(m_dataOperation.count(outputPivotId)) {
        UtilityOperation::log_error("%s An operation already exists for Pivot ID '%s' (this may indicate a duplicate Pivot ID)",
                                    beforeLog.c_str(), outputPivotId.c_str());
        return;
    }

    OperationsInfo operationsInfo;
    operationsInfo.outputAssetName = datapoint[ConstantsOperation::JsonLabel].GetString();
    operationsInfo.outputPivotType = datapoint[ConstantsOperation::JsonPivotType].GetString();
    if (operationsInfo.outputPivotType != ConstantsOperation::JsonCdcSps && operationsInfo.outputPivotType != ConstantsOperation::JsonCdcDps) {
        return;
    }
    
    if (!datapoint.HasMember(ConstantsOperation::JsonOperations) || !datapoint[ConstantsOperation::JsonOperations].IsArray()) {
        return;
    }

    auto operations = datapoint[ConstantsOperation::JsonOperations].GetArray();
    for (rapidjson::Value::ConstValueIterator itr = operations.Begin(); itr != operations.End(); ++itr) {
        OperationInfo operationInfo;
        if (!importOperation(itr, operationInfo)) {
            continue;
        }
        operationsInfo.operations.push_back(operationInfo);
        UtilityOperation::log_debug("%s Configured '%s' operation for inputs [%s] and output %s", beforeLog.c_str(),
                                    operationInfo.operationType.c_str(), UtilityOperation::join(operationInfo.inputPivotIds).c_str(),
                                    outputPivotId.c_str());
    }
    if (operationsInfo.operations.empty()) {
        return;
    }
    m_dataOperation[outputPivotId] = operationsInfo;
    for(int i=0 ; i<operationsInfo.operations.size() ; i++) {
        const auto& ops = operationsInfo.operations[i];
        for(const auto& inputPivotId: ops.inputPivotIds) {
            if(m_dataOperationLookup.count(inputPivotId)) {
                m_dataOperationLookup[inputPivotId].emplace_back(outputPivotId, i);
            }
            else {
                m_dataOperationLookup[inputPivotId] = {{outputPivotId, i}};
            }
        }
    }
}

/**
 * Import an operation found in Exchanged_data
 * The data is saved in a maps m_dataOperation
 * 
 * @param itr : Iterator pointing to the operation object to import
 * @param out_operationInfo : Out parameter storing the operation information
 * @return true if the import was a success, else false
*/
bool ConfigOperation::importOperation(rapidjson::Value::ConstValueIterator itr, OperationInfo& out_operationInfo) const {
    std::string beforeLog = ConstantsOperation::NamePlugin + " - ConfigOperation::importOperation :";
    if (!(*itr).IsObject()) {
        UtilityOperation::log_error("%s %s element is not an object", beforeLog.c_str(), ConstantsOperation::JsonOperations);
        return false;
    }
    auto operation = (*itr).GetObject();

    if (!operation.HasMember(ConstantsOperation::JsonOperation) || !operation[ConstantsOperation::JsonOperation].IsString()) {
        UtilityOperation::log_error("%s %s does not exist or is not a string", beforeLog.c_str(), ConstantsOperation::JsonOperation);
        return false;
    }

    out_operationInfo.operationType = operation[ConstantsOperation::JsonOperation].GetString();
    if (!m_supportedOperationTypes.count(out_operationInfo.operationType)) {
        UtilityOperation::log_error("%s '%s' is not a supported operation type", beforeLog.c_str(), out_operationInfo.operationType.c_str());
        return false;
    }

    if (!operation.HasMember(ConstantsOperation::JsonInput) || !operation[ConstantsOperation::JsonInput].IsArray()) {
        UtilityOperation::log_error("%s %s does not exist or is not an array", beforeLog.c_str(), ConstantsOperation::JsonInput);
        return false;
    }

    auto inputs = operation[ConstantsOperation::JsonInput].GetArray();
    for (rapidjson::Value::ConstValueIterator itr2 = inputs.Begin(); itr2 != inputs.End(); ++itr2) {
        if (!(*itr2).IsString()) {
            UtilityOperation::log_error("%s %s element is not a string", beforeLog.c_str(), ConstantsOperation::JsonInput);
            return false;
        }
        std::string inputPivotId = (*itr2).GetString();
        out_operationInfo.inputPivotIds.push_back(inputPivotId);
    }
    return true;
}

/**
 * Returns the list of outputIds and operation index from operations that involves the given inputId
 * 
 * @param inputId : Input ID to look for
 * @return The list of (outputIds, operationIndex) pairs if any, else an empty list
*/
const std::vector<OperationInfoLookup>& ConfigOperation::getOperationsForInputId(const std::string& inputId) const {
    if (m_dataOperationLookup.count(inputId)) {
        return m_dataOperationLookup.at(inputId);
    }
    else {
        static std::vector<OperationInfoLookup> empty;
        return empty;
    }
}
