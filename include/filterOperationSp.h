#ifndef INCLUDE_FILTER_OPERATION_SP_H_
#define INCLUDE_FILTER_OPERATION_SP_H_

/*
 * Fledge filter plugin make operations with multiple status points to produce a value for a single status point
 *
 * Copyright (c) 2020, RTE (https://www.rte-france.com)
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Yannick Marchetaux
 * 
 */
#include "configOperation.h"

#include <config_category.h>
#include <filter.h>

#include <mutex>
#include <string>

class FilterOperationSp  : public FledgeFilter
{
public:  
    FilterOperationSp(const std::string& filterName,
                        ConfigCategory& filterConfig,
                        OUTPUT_HANDLE *outHandle,
                        OUTPUT_STREAM output);

    void ingest(READINGSET *readingSet);
    void reconfigure(const std::string& newConfig);

    void setJsonConfig(const std::string& jsonExchanged);

    const ConfigOperation& getConfigOperation() const { return m_configOperation;} 
    Reading *generateReadingOperation(const Reading *dps, const std::string& outputPivotId);

private:
    bool processReading(Reading* reading, std::vector<Reading*>& out_vectorReadingOperation);

    std::mutex                  m_configMutex;
    ConfigOperation             m_configOperation;
    std::map<std::string, int>  m_cachedValues;
};

#endif  // INCLUDE_FILTER_OPERATION_SP_H_