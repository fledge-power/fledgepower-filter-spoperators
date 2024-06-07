/*
 * Fledge filter plugin make operations with multiple status points to produce a value for a single status point

 * Copyright (c) 2020, RTE (https://www.rte-france.com)
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Yannick Marchetaux
 * 
 */
#include "constantsOperation.h"
#include "filterOperationSp.h"
#include "utilityOperation.h"

#include <datapoint.h>
#include <datapoint_utility.h>
#include <reading.h>

using namespace std;
using namespace DatapointUtility;

/**
 * Constructor for the LogFilter.
 *
 * We call the constructor of the base class and handle the initial
 * configuration of the filter.
 *
 * @param    filterName      The name of the filter
 * @param    filterConfig    The configuration category for this filter
 * @param    outHandle       The handle of the next filter in the chain
 * @param    output          A function pointer to call to output data to the next filter
 */
FilterOperationSp::FilterOperationSp(const std::string& filterName,
                        ConfigCategory& filterConfig,
                        OUTPUT_HANDLE *outHandle,
                        OUTPUT_STREAM output) :
                                FledgeFilter(filterName, filterConfig, outHandle, output)
{
}

/**
 * Modification of configuration
 * 
 * @param jsonExchanged : configuration ExchangedData
*/
void FilterOperationSp::setJsonConfig(const string& jsonExchanged) {
    m_configOperation.importExchangedData(jsonExchanged);
    m_cachedValues.clear();
}

/**
 * The actual filtering code
 *
 * @param readingSet The reading data to filter
 */
void FilterOperationSp::ingest(READINGSET *readingSet) 
{
    lock_guard<mutex> guard(m_configMutex);
    std::string beforeLog = ConstantsOperation::NamePlugin + " - FilterOperationSp::ingest :";
    std::vector<Reading*> vectorReadingOperation;
	
    if (!readingSet) {
        UtilityOperation::log_error("%s No reading set provided", beforeLog.c_str());
        return;
    } 
    if (m_func == nullptr) {
        UtilityOperation::log_error("%s No callback function defined", beforeLog.c_str());
        return;
    }

    if (isEnabled()) { 
        // Just get all the readings in the readingset
        std::vector<Reading*>* readings = readingSet->getAllReadingsPtr();
        auto readIt = readings->begin();
        while(readIt != readings->end()) {
            Reading* reading = *readIt;
            bool deleteInput = processReading(reading, vectorReadingOperation);
            // If input TI is one of the output TIs, remove the original input reading as a new value for it was already generated
            if (deleteInput) {
                readIt = readings->erase(readIt);
            }
            else {
                readIt++;
            }
        }
        readingSet->append(vectorReadingOperation);
    }

    (*m_func)(m_data, readingSet);
}

/**
 * Apply filter for the given rading
 *
 * @param reading The reading to filter
 * @param out_vectorReadingOperation Out parameter storing all generated readings
 * @return true if the input reading should be deleted, else false
 */
bool FilterOperationSp::processReading(Reading* reading, std::vector<Reading*>& out_vectorReadingOperation) {
    // Get datapoints on readings
    Datapoints &dataPoints = reading->getReadingData();
    string assetName = reading->getAssetName();

    string beforeLog = ConstantsOperation::NamePlugin + " - " + assetName + " - FilterOperationSp::processReading :";

    Datapoints *dpPivotTS = findDictElement(&dataPoints, ConstantsOperation::KeyMessagePivotJsonRoot);
    if (dpPivotTS == nullptr) {
        UtilityOperation::log_debug("%s Missing %s attribute, it is ignored", beforeLog.c_str(), ConstantsOperation::KeyMessagePivotJsonRoot.c_str());
        return false;
    }

    Datapoints *dpGtis = findDictElement(dpPivotTS, ConstantsOperation::KeyMessagePivotJsonGt);
    if (dpGtis == nullptr) {
        UtilityOperation::log_debug("%s Missing %s attribute, it is ignored", beforeLog.c_str(), ConstantsOperation::KeyMessagePivotJsonGt.c_str());
       return false;
    }

    string inputPivotId = findStringElement(dpGtis, ConstantsOperation::KeyMessagePivotJsonId);
    if (inputPivotId.compare("") == 0) {
        UtilityOperation::log_debug("%s Missing %s attribute, it is ignored", beforeLog.c_str(), ConstantsOperation::KeyMessagePivotJsonId.c_str());
        return false;
    }

    const auto& operationsLookup = m_configOperation.getOperationsForInputId(inputPivotId);
    if (operationsLookup.empty()) {
        UtilityOperation::log_debug("%s No operation configured for Pivot ID %s", beforeLog.c_str(), inputPivotId.c_str());
        return false;
    }

    bool typeSps = true;
    Datapoints *dpTyp = findDictElement(dpGtis, ConstantsOperation::JsonCdcSps);
    if (dpTyp == nullptr) {
        dpTyp = findDictElement(dpGtis, ConstantsOperation::JsonCdcDps);
        
        if (dpTyp == nullptr) {
            UtilityOperation::log_debug("%s Missing CDC (%s and %s missing) attribute, it is ignored", beforeLog.c_str(), ConstantsOperation::JsonCdcSps.c_str(), ConstantsOperation::JsonCdcDps.c_str());
            return false;
        }
        typeSps = false;
    }            

    const DatapointValue *valueTS = findValueElement(dpTyp, ConstantsOperation::KeyMessagePivotJsonStVal);
    if (valueTS == nullptr) {
        UtilityOperation::log_debug("%s Missing %s attribute, it is ignored", beforeLog.c_str(), ConstantsOperation::KeyMessagePivotJsonStVal.c_str());
        return false;
    }

    int newValue = 0;
    if (typeSps) {
        newValue = static_cast<int>(valueTS->toInt());
    }
    else {
        newValue = valueTS->toStringValue() == "on" ? 1 : 0;
    }

    bool inputIsInOutputs = false;
    for(const auto& operationLookup: operationsLookup) {
        m_cachedValues[inputPivotId] = newValue;
        Reading* newReading = generateReadingOperation(reading, operationLookup.outputPivotId, operationLookup.operationIndex);
        if (newReading != nullptr){
            UtilityOperation::log_debug("%s Generation of the reading [%s]", beforeLog.c_str(), newReading->toJSON().c_str());
            out_vectorReadingOperation.push_back(newReading);
            // Only delete input reading if a replacement was generated
            if (inputPivotId == operationLookup.outputPivotId) {
                inputIsInOutputs = true;
            }
        }
    }
    return inputIsInOutputs;
}

/**
 * Generate of reading for operation
 * 
 * @param reading initial reading
 * @param outputPivotId pivot ID of the output TI to produce
 * @return a modified reading
*/
Reading *FilterOperationSp::generateReadingOperation(const Reading *reading, const std::string& outputPivotId, int operationIndex) {
    string beforeLog = ConstantsOperation::NamePlugin + " - FilterOperationSp::generateReadingOperation :";
    
    // Find operation info to generate the reading
    const auto& dataOperations = m_configOperation.getDataOperations();
    if (dataOperations.count(outputPivotId) == 0) {
        UtilityOperation::log_debug("%s No data operation found for output Pivot ID '%s', reading creation cancelled",
                                    beforeLog.c_str(), outputPivotId.c_str());
        return nullptr;
    }
    const auto& operationsInfo = dataOperations.at(outputPivotId);
    const auto& operationInfo = operationsInfo.operations.at(operationIndex);

    // Compute new reading value by applying operation logic
    const std::string& operationType = operationInfo.operationType;
    int newValue = 0;
    for(const std::string& inputPivotId: operationInfo.inputPivotIds) {
        if (operationType == "or") {
            // If no cached value was registered for this Pivot ID, it will automatically initialize it at 0
            newValue = newValue || m_cachedValues[inputPivotId]; 
        }
    }
    bool targetTypeSps = (operationsInfo.outputPivotType == ConstantsOperation::JsonCdcSps);
    
    // Ensure input reading is not null
    if (reading == nullptr) {
        UtilityOperation::log_debug("%s Input reading is null, %s reading creation cancelled", beforeLog.c_str(), outputPivotId.c_str());
        return nullptr;
    }
    beforeLog = ConstantsOperation::NamePlugin + " - " + reading->getAssetName() + " - FilterOperationSp::generateReadingOperation :";

    // Deep copy on Datapoint
    Datapoint *dpRoot = reading->getDatapoint(ConstantsOperation::KeyMessagePivotJsonRoot);
    if (dpRoot == nullptr) {
        UtilityOperation::log_debug("%s Attribute %s missing, %s reading creation cancelled", beforeLog.c_str(),
                                    ConstantsOperation::KeyMessagePivotJsonRoot.c_str(), outputPivotId.c_str());
        return nullptr;
    }

    DatapointValue newValueOperation(dpRoot->getData());

    // Generate ouput reading
    Datapoints *dpGtis = findDictElement(newValueOperation.getDpVec(), ConstantsOperation::KeyMessagePivotJsonGt);
    if (dpGtis == nullptr) {
        UtilityOperation::log_debug("%s Attribute %s missing, %s reading creation cancelled", beforeLog.c_str(),
                                    ConstantsOperation::KeyMessagePivotJsonGt.c_str(), outputPivotId.c_str());
        return nullptr;
    }
    
    // Overwrite identifier
    createStringElement(dpGtis, ConstantsOperation::KeyMessagePivotJsonId, outputPivotId);

    bool typeSps = true;
    Datapoints *dpTyp = findDictElement(dpGtis, ConstantsOperation::JsonCdcSps);
    if (dpTyp == nullptr) {
        dpTyp = findDictElement(dpGtis, ConstantsOperation::JsonCdcDps);
        
        if (dpTyp == nullptr) {
            UtilityOperation::log_debug("%s Attribute CDC missing, %s reading creation cancelled", beforeLog.c_str(), outputPivotId.c_str());
            return nullptr;
        }
        typeSps = false;
    }
    // If the output type does not match the current type found in the reading, rewrite that part of the reading
    if (typeSps != targetTypeSps) {
        Datapoint *cdcTypeDp = findDatapointElement(dpGtis, typeSps?ConstantsOperation::JsonCdcSps:ConstantsOperation::JsonCdcDps);
        cdcTypeDp->setName(operationsInfo.outputPivotType);
    }
    // Set computed value
    if (targetTypeSps) {
        createIntegerElement(dpTyp, ConstantsOperation::KeyMessagePivotJsonStVal, newValue);
    }
    else {
        createStringElement(dpTyp, ConstantsOperation::KeyMessagePivotJsonStVal, newValue?"on":"off");
    }
    // Update quality
    Datapoints *dpQ = findDictElement(dpTyp, ConstantsOperation::KeyMessagePivotJsonQ);
    if (dpQ == nullptr) {
        Datapoint *datapointQ = createDictElement(dpTyp, ConstantsOperation::KeyMessagePivotJsonQ);
        dpQ = datapointQ->getData().getDpVec();
    }

    createStringElement(dpQ, ConstantsOperation::KeyMessagePivotJsonSource, ConstantsOperation::ValueSubstituted);

    auto newDatapointOperation = new Datapoint(dpRoot->getName(), newValueOperation);
    auto newReading = new Reading(operationsInfo.outputAssetName, newDatapointOperation);
    return newReading;
}

/**
 * Reconfiguration entry point to the filter.
 *
 * This method runs holding the configMutex to prevent
 * ingest using the regex class that may be destroyed by this
 * call.
 *
 * Pass the configuration to the base FilterPlugin class and
 * then call the private method to handle the filter specific
 * configuration.
 *
 * @param newConfig  The JSON of the new configuration
 */
void FilterOperationSp::reconfigure(const std::string& newConfig) {
    lock_guard<mutex> guard(m_configMutex);
    setConfig(newConfig);

    ConfigCategory config("newConfig", newConfig);
    if (config.itemExists("exchanged_data")) {
        this->setJsonConfig(config.getValue("exchanged_data"));
    }
}
