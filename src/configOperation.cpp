#include "configOperation.h"
#include "constantsOperation.h"
#include "utilityOperation.h"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

using namespace std;
using namespace rapidjson;

/**
 * Import data in the form of Exchanged_data
 * The data is saved in a map m_dataOperation
 * 
 * @param exchangeConfig : configuration Exchanged_data as a string 
*/
void ConfigOperation::importExchangedData(const string & exchangeConfig) {
    std::string beforeLog = ConstantsOperation::NamePlugin + " - ConfigOperation::importExchangedData : ";
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
        if (!datapoint.IsObject()) {
            UtilityOperation::log_error("%s %s element is not an object", beforeLog.c_str(), ConstantsOperation::JsonDatapoints);
            continue;
        }
        
        if (!datapoint.HasMember(ConstantsOperation::JsonPivotType) || !datapoint[ConstantsOperation::JsonPivotType].IsString()) {
            UtilityOperation::log_error("%s %s does not exist or is not a string", beforeLog.c_str(), ConstantsOperation::JsonPivotType);
            continue;
        }

        if (!datapoint.HasMember(ConstantsOperation::JsonPivotId) || !datapoint[ConstantsOperation::JsonPivotId].IsString()) {
            UtilityOperation::log_error("%s %s does not exist or is not a string", beforeLog.c_str(), ConstantsOperation::JsonPivotId);
            continue;
        }

        if (!datapoint.HasMember(ConstantsOperation::JsonLabel) || !datapoint[ConstantsOperation::JsonLabel].IsString()) {
            UtilityOperation::log_error("%s %s does not exist or is not a string", beforeLog.c_str(), ConstantsOperation::JsonLabel);
            continue;
        }

        std::string outputPivotId = datapoint[ConstantsOperation::JsonPivotId].GetString();
        foundPivotIds.insert(outputPivotId);

        DataOperationInfo operationInfo;
        operationInfo.outputAssetName = datapoint[ConstantsOperation::JsonLabel].GetString();
        operationInfo.outputPivotType = datapoint[ConstantsOperation::JsonPivotType].GetString();
        if (operationInfo.outputPivotType != ConstantsOperation::JsonCdcSps && operationInfo.outputPivotType != ConstantsOperation::JsonCdcDps) {
            continue;
        }
        
        if (!datapoint.HasMember(ConstantsOperation::JsonOperations) || !datapoint[ConstantsOperation::JsonOperations].IsArray()) {
            continue;
        }
        bool malformedOperation = false;
        auto operations = datapoint[ConstantsOperation::JsonOperations].GetArray();
        for (rapidjson::Value::ConstValueIterator itr = operations.Begin(); itr != operations.End(); ++itr) {
            if (!(*itr).IsObject()) {
                UtilityOperation::log_error("%s %s element is not an object", beforeLog.c_str(), ConstantsOperation::JsonOperations);
                malformedOperation = true;
                break;
            }
            auto operation = (*itr).GetObject();

            if (!operation.HasMember(ConstantsOperation::JsonOperation) || !operation[ConstantsOperation::JsonOperation].IsString()) {
                UtilityOperation::log_error("%s %s does not exist or is not a string", beforeLog.c_str(), ConstantsOperation::JsonOperation);
                malformedOperation = true;
                break;
            }

            operationInfo.operationType = operation[ConstantsOperation::JsonOperation].GetString();
            if (!m_supportedOperationTypes.count(operationInfo.operationType)) {
                UtilityOperation::log_error("%s '%s' is not a supported operation type", beforeLog.c_str(), operationInfo.operationType.c_str());
                malformedOperation = true;
                break;
            }

            if (!operation.HasMember(ConstantsOperation::JsonInput) || !operation[ConstantsOperation::JsonInput].IsArray()) {
                UtilityOperation::log_error("%s %s does not exist or is not an array", beforeLog.c_str(), ConstantsOperation::JsonInput);
                malformedOperation = true;
                break;
            }

            auto inputs = operation[ConstantsOperation::JsonInput].GetArray();
            for (rapidjson::Value::ConstValueIterator itr2 = inputs.Begin(); itr2 != inputs.End(); ++itr2) {
                if (!(*itr2).IsString()) {
                    UtilityOperation::log_error("%s %s element is not a string", beforeLog.c_str(), ConstantsOperation::JsonInput);
                    malformedOperation = true;
                    break;
                }
                std::string inputPivotId = (*itr2).GetString();
                operationInfo.inputPivotIds.push_back(inputPivotId);
            }
            if (malformedOperation) {
                break;
            }
        }
        if (malformedOperation) {
            continue;
        }
        
        if(m_dataOperation.count(outputPivotId)) {
            UtilityOperation::log_error("%s An operation already exists for Pivot ID '%s' (this may indicate a duplicate Pivot ID)",
                                        beforeLog.c_str(), outputPivotId.c_str());
            continue;
        }
        m_dataOperation[outputPivotId] = operationInfo;
        for(const auto& inputPivotId: operationInfo.inputPivotIds) {
            if(m_dataOperationLookup.count(inputPivotId)) {
                m_dataOperationLookup[inputPivotId].push_back(outputPivotId);
            }
            else {
                m_dataOperationLookup[inputPivotId] = {outputPivotId};
            }
        }
        UtilityOperation::log_debug("%s Configured '%s' operation for inputs [%s] and output %s", beforeLog.c_str(),
                                    operationInfo.operationType.c_str(), UtilityOperation::join(operationInfo.inputPivotIds).c_str(),
                                    outputPivotId.c_str());
    }

    // Sanity check on the input Pivot IDs listed
    for(const auto& kvp: m_dataOperationLookup) {
        if(foundPivotIds.count(kvp.first) == 0) {
            UtilityOperation::log_warn("%s An operation is configured for unexisting Pivot ID '%s'", beforeLog.c_str(), kvp.first.c_str());
        }
    }
}

/**
 * Returns the list of outputIds with an operation that involves the given inputId
 * 
 * @param inputId : Input ID to look for
 * @return The list of outputIds if any, else an empty list
*/
const std::vector<std::string>& ConfigOperation::getOutputIdsForInputId(const std::string& inputId) {
    if (m_dataOperationLookup.count(inputId)) {
        return m_dataOperationLookup[inputId];
    }
    else {
        static std::vector<std::string> empty;
        return empty;
    }
}
