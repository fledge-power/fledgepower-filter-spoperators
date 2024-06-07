#include "filterOperationSp.h"

#include <gtest/gtest.h>

extern "C" {
    PLUGIN_INFORMATION* plugin_info();
    PLUGIN_HANDLE plugin_init(ConfigCategory* config,
        OUTPUT_HANDLE* outHandle,
        OUTPUT_STREAM output);
    void plugin_shutdown(PLUGIN_HANDLE handle);
};

class PluginConfigureTest : public testing::Test
{
protected:
    FilterOperationSp* filter = nullptr;  // Object on which we call for tests

    // Setup is ran for every tests, so each variable are reinitialised
    void SetUp() override
    {
		void *handle = nullptr;
        ASSERT_NO_THROW(handle = plugin_init(nullptr, nullptr, nullptr));
		filter = static_cast<FilterOperationSp*>(handle);
        ASSERT_NE(filter, nullptr);
    }

    // TearDown is ran for every tests, so each variable are destroyed again
    void TearDown() override
    {
        if (filter) {
            ASSERT_NO_THROW(plugin_shutdown(static_cast<PLUGIN_HANDLE>(filter)));
        }
    }
};

TEST_F(PluginConfigureTest, ConfigureErrorParseJSON)
{
    static std::string configureErrorParseJSON = QUOTE("parse":error);

    filter->setJsonConfig(configureErrorParseJSON);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorRootNotObject)
{
    static std::string configureErrorRootNotObject = QUOTE("unknown");

    filter->setJsonConfig(configureErrorRootNotObject);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorNoExchangedData)
{
    static std::string configureErrorNoExchangedData = QUOTE({
        "configureErrorExchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorNoExchangedData);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorExchangedDataNotObject)
{
    static std::string configureErrorExchangedDataNotObject = QUOTE({
        "exchanged_data": 42
    });

    filter->setJsonConfig(configureErrorExchangedDataNotObject);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorNoDatapoints)
{
    static std::string configureErrorNoDatapoints = QUOTE({
        "exchanged_data": {
            "configureErrorDatapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorNoDatapoints);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorDatapointsNotArray)
{
    static std::string configureErrorDatapointsNotArray = QUOTE({
        "exchanged_data": {
            "datapoints" : 42
        }
    });

    filter->setJsonConfig(configureErrorDatapointsNotArray);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorDatapointsElementNotObject)
{
    static std::string configureErrorDatapointsElementNotObject = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                42
            ]
        }
    });

    filter->setJsonConfig(configureErrorDatapointsElementNotObject);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorNoPivotType)
{
    static std::string configureErrorNoPivotType = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorNoPivotType);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorPivotTypeNotString)
{
    static std::string configureErrorPivotTypeNotString = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : 42,
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorPivotTypeNotString);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorPivotTypeMv)
{
    static std::string configureErrorPivotTypeMv = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "MvTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorPivotTypeMv);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorNoPivotID)
{
    static std::string configureErrorNoPivotID = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorNoPivotID);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorPivotIDNotString)
{
    static std::string configureErrorPivotIDNotString = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : 42,
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorPivotIDNotString);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorNoLabel)
{
    static std::string configureErrorNoLabel = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorNoLabel);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorLabelNotString)
{
    static std::string configureErrorLabelNotString = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label": 42,
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorLabelNotString);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureCaseNoOperations)
{
    static std::string configureCaseNoOperations = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "protocols" : [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureCaseNoOperations);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorOperationsNotArray)
{
    static std::string configureErrorOperationsNotArray = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : 42,
                    "protocols" : [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorOperationsNotArray);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorOperationsElementNotObject)
{
    static std::string configureErrorOperationsElementNotObject = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        42
                    ],
                    "protocols" : [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorOperationsElementNotObject);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorNoOperation)
{
    static std::string configureErrorNoOperation = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "input": [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorNoOperation);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorOperationNotString)
{
    static std::string configureErrorOperationNotString = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": 42,
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorOperationNotString);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorUnknownOperation)
{
    static std::string configureErrorUnknownOperation = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": "unknown",
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorUnknownOperation);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorNoInput)
{
    static std::string configureErrorNoInput = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": "or"
                        }
                    ],
                    "protocols" : [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorNoInput);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorInputNotArray)
{
    static std::string configureErrorInputNotArray = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : 42
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorInputNotArray);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureErrorInputElementNotString)
{
    static std::string configureErrorInputElementNotString = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                42
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorInputElementNotString);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureCaseEmptyOperations)
{
    static std::string configureCaseEmptyOperations = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureCaseEmptyOperations);
    ASSERT_EQ(filter->getConfigOperation().getDataOperations().size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureCaseEmptyInput)
{
    static std::string configureCaseEmptyInput = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : []
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureCaseEmptyInput);
    auto dataOperation = filter->getConfigOperation().getDataOperations();
    ASSERT_EQ(dataOperation.size(), 1);
    ASSERT_EQ(dataOperation.count("M_2367_3_15_4"), 1);
    const auto& dataOperationInfo = dataOperation.at("M_2367_3_15_4");
    ASSERT_STREQ(dataOperationInfo.outputPivotType.c_str(), "SpsTyp");
    ASSERT_EQ(dataOperationInfo.operations.size(), 1);
    ASSERT_STREQ(dataOperationInfo.operations[0].operationType.c_str(), "or");
    ASSERT_EQ(dataOperationInfo.operations[0].inputPivotIds.size(), 0);
    auto operationsLookupVec = filter->getConfigOperation().getOperationsForInputId("M_2367_3_15_4");
    ASSERT_EQ(operationsLookupVec.size(), 0);
    auto operationsLookupVec2 = filter->getConfigOperation().getOperationsForInputId("M_2367_3_15_5");
    ASSERT_EQ(operationsLookupVec2.size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureWarningUnexistingPivotId)
{
    static std::string configureWarningUnexistingPivotId = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureWarningUnexistingPivotId);
    auto dataOperation = filter->getConfigOperation().getDataOperations();
    ASSERT_EQ(dataOperation.size(), 1);
    ASSERT_EQ(dataOperation.count("M_2367_3_15_4"), 1);
    ASSERT_EQ(dataOperation.count("M_2367_3_15_5"), 0);
    const auto& dataOperationInfo = dataOperation.at("M_2367_3_15_4");
    ASSERT_STREQ(dataOperationInfo.outputPivotType.c_str(), "SpsTyp");
    ASSERT_EQ(dataOperationInfo.operations.size(), 1);
    ASSERT_STREQ(dataOperationInfo.operations[0].operationType.c_str(), "or");
    ASSERT_EQ(dataOperationInfo.operations[0].inputPivotIds.size(), 2);
    ASSERT_STREQ(dataOperationInfo.operations[0].inputPivotIds[0].c_str(), "M_2367_3_15_4");
    ASSERT_STREQ(dataOperationInfo.operations[0].inputPivotIds[1].c_str(), "M_2367_3_15_5");
    auto operationsLookupVec = filter->getConfigOperation().getOperationsForInputId("M_2367_3_15_4");
    ASSERT_EQ(operationsLookupVec.size(), 1);
    ASSERT_STREQ(operationsLookupVec[0].outputPivotId.c_str(), "M_2367_3_15_4");
    ASSERT_EQ(operationsLookupVec[0].operationIndex, 0);
    auto operationsLookupVec2 = filter->getConfigOperation().getOperationsForInputId("M_2367_3_15_5");
    ASSERT_EQ(operationsLookupVec2.size(), 1);
    ASSERT_STREQ(operationsLookupVec2[0].outputPivotId.c_str(), "M_2367_3_15_4");
    ASSERT_EQ(operationsLookupVec2[0].operationIndex, 0);
    auto operationsLookupVec3 = filter->getConfigOperation().getOperationsForInputId("unknown");
    ASSERT_EQ(operationsLookupVec3.size(), 0);
}

TEST_F(PluginConfigureTest, ConfigureOKSpsDps)
{
    static std::string configureOKSpsDps = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                },
                {
                    "label":"TS-2",
                    "pivot_id" : "M_2367_3_15_5",
                    "pivot_type" : "DpsTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        },
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_5",
                                "M_2367_3_15_6",
                                "M_2367_3_15_7"
                            ]
                        },
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_8"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271612"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureOKSpsDps);
    auto dataOperation = filter->getConfigOperation().getDataOperations();
    ASSERT_EQ(dataOperation.size(), 2);
    ASSERT_EQ(dataOperation.count("M_2367_3_15_4"), 1);;
    const auto& dataOperationInfo = dataOperation.at("M_2367_3_15_4");
    ASSERT_STREQ(dataOperationInfo.outputPivotType.c_str(), "SpsTyp");
    ASSERT_EQ(dataOperationInfo.operations.size(), 1);
    ASSERT_STREQ(dataOperationInfo.operations[0].operationType.c_str(), "or");
    ASSERT_EQ(dataOperationInfo.operations[0].inputPivotIds.size(), 2);
    ASSERT_STREQ(dataOperationInfo.operations[0].inputPivotIds[0].c_str(), "M_2367_3_15_4");
    ASSERT_STREQ(dataOperationInfo.operations[0].inputPivotIds[1].c_str(), "M_2367_3_15_5");
    ASSERT_EQ(dataOperation.count("M_2367_3_15_5"), 1);
    const auto& dataOperationInfo2 = dataOperation.at("M_2367_3_15_5");
    ASSERT_STREQ(dataOperationInfo2.outputPivotType.c_str(), "DpsTyp");
    ASSERT_EQ(dataOperationInfo2.operations.size(), 3);
    ASSERT_STREQ(dataOperationInfo2.operations[0].operationType.c_str(), "or");
    ASSERT_EQ(dataOperationInfo2.operations[0].inputPivotIds.size(), 2);
    ASSERT_STREQ(dataOperationInfo2.operations[0].inputPivotIds[0].c_str(), "M_2367_3_15_4");
    ASSERT_STREQ(dataOperationInfo2.operations[0].inputPivotIds[1].c_str(), "M_2367_3_15_5");
    ASSERT_STREQ(dataOperationInfo2.operations[1].operationType.c_str(), "or");
    ASSERT_EQ(dataOperationInfo2.operations[1].inputPivotIds.size(), 3);
    ASSERT_STREQ(dataOperationInfo2.operations[1].inputPivotIds[0].c_str(), "M_2367_3_15_5");
    ASSERT_STREQ(dataOperationInfo2.operations[1].inputPivotIds[1].c_str(), "M_2367_3_15_6");
    ASSERT_STREQ(dataOperationInfo2.operations[1].inputPivotIds[2].c_str(), "M_2367_3_15_7");
    ASSERT_STREQ(dataOperationInfo2.operations[2].operationType.c_str(), "or");
    ASSERT_EQ(dataOperationInfo2.operations[2].inputPivotIds.size(), 1);
    ASSERT_STREQ(dataOperationInfo2.operations[2].inputPivotIds[0].c_str(), "M_2367_3_15_8");
    auto operationsLookupVec = filter->getConfigOperation().getOperationsForInputId("M_2367_3_15_4");
    ASSERT_EQ(operationsLookupVec.size(), 2);
    ASSERT_STREQ(operationsLookupVec[0].outputPivotId.c_str(), "M_2367_3_15_4");
    ASSERT_EQ(operationsLookupVec[0].operationIndex, 0);
    ASSERT_STREQ(operationsLookupVec[1].outputPivotId.c_str(), "M_2367_3_15_5");
    ASSERT_EQ(operationsLookupVec[1].operationIndex, 0);
    auto operationsLookupVec2 = filter->getConfigOperation().getOperationsForInputId("M_2367_3_15_5");
    ASSERT_EQ(operationsLookupVec2.size(), 3);
    ASSERT_STREQ(operationsLookupVec2[0].outputPivotId.c_str(), "M_2367_3_15_4");
    ASSERT_EQ(operationsLookupVec2[0].operationIndex, 0);
    ASSERT_STREQ(operationsLookupVec2[1].outputPivotId.c_str(), "M_2367_3_15_5");
    ASSERT_EQ(operationsLookupVec2[1].operationIndex, 0);
    ASSERT_STREQ(operationsLookupVec2[2].outputPivotId.c_str(), "M_2367_3_15_5");
    ASSERT_EQ(operationsLookupVec2[2].operationIndex, 1);
    auto operationsLookupVec3 = filter->getConfigOperation().getOperationsForInputId("M_2367_3_15_6");
    ASSERT_EQ(operationsLookupVec3.size(), 1);
    ASSERT_STREQ(operationsLookupVec3[0].outputPivotId.c_str(), "M_2367_3_15_5");
    ASSERT_EQ(operationsLookupVec3[0].operationIndex, 1);
    auto operationsLookupVec4 = filter->getConfigOperation().getOperationsForInputId("M_2367_3_15_7");
    ASSERT_EQ(operationsLookupVec4.size(), 1);
    ASSERT_STREQ(operationsLookupVec4[0].outputPivotId.c_str(), "M_2367_3_15_5");
    ASSERT_EQ(operationsLookupVec4[0].operationIndex, 1);
    auto operationsLookupVec5 = filter->getConfigOperation().getOperationsForInputId("M_2367_3_15_8");
    ASSERT_EQ(operationsLookupVec5.size(), 1);
    ASSERT_STREQ(operationsLookupVec5[0].outputPivotId.c_str(), "M_2367_3_15_5");
    ASSERT_EQ(operationsLookupVec5[0].operationIndex, 2);
}

TEST_F(PluginConfigureTest, ConfigureErrorDuplicatePivotID)
{
    static std::string configureErrorDuplicatePivotID = QUOTE({
        "exchanged_data": {
            "datapoints" : [
                {
                    "label":"TS-1",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "SpsTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_6",
                                "M_2367_3_15_7"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271611"
                        }
                    ]
                },
                {
                    "label":"TS-2",
                    "pivot_id" : "M_2367_3_15_4",
                    "pivot_type" : "DpsTyp",
                    "operations" : [
                        {
                            "operation": "or",
                            "input" : [
                                "M_2367_3_15_4",
                                "M_2367_3_15_5"
                            ]
                        }
                    ],
                    "protocols": [
                        {
                            "name":"IEC104",
                            "typeid" : "M_ME_NC_1",
                            "address" : "3271612"
                        }
                    ]
                }
            ]
        }
    });

    filter->setJsonConfig(configureErrorDuplicatePivotID);
    auto dataOperation = filter->getConfigOperation().getDataOperations();
    ASSERT_EQ(dataOperation.size(), 1);
    ASSERT_EQ(dataOperation.count("M_2367_3_15_4"), 1);
    const auto& dataOperationInfo = dataOperation.at("M_2367_3_15_4");
    ASSERT_STREQ(dataOperationInfo.outputPivotType.c_str(), "SpsTyp");
    ASSERT_EQ(dataOperationInfo.operations.size(), 1);
    ASSERT_STREQ(dataOperationInfo.operations[0].operationType.c_str(), "or");
    ASSERT_EQ(dataOperationInfo.operations[0].inputPivotIds.size(), 2);
    ASSERT_STREQ(dataOperationInfo.operations[0].inputPivotIds[0].c_str(), "M_2367_3_15_6");
    ASSERT_STREQ(dataOperationInfo.operations[0].inputPivotIds[1].c_str(), "M_2367_3_15_7");
    auto operationsLookupVec = filter->getConfigOperation().getOperationsForInputId("M_2367_3_15_6");
    ASSERT_EQ(operationsLookupVec.size(), 1);
    ASSERT_STREQ(operationsLookupVec[0].outputPivotId.c_str(), "M_2367_3_15_4");
    ASSERT_EQ(operationsLookupVec[0].operationIndex, 0);
    auto operationsLookupVec2 = filter->getConfigOperation().getOperationsForInputId("M_2367_3_15_7");
    ASSERT_EQ(operationsLookupVec2.size(), 1);
    ASSERT_STREQ(operationsLookupVec2[0].outputPivotId.c_str(), "M_2367_3_15_4");
    ASSERT_EQ(operationsLookupVec2[0].operationIndex, 0);
    auto operationsLookupVec3 = filter->getConfigOperation().getOperationsForInputId("M_2367_3_15_4");
    ASSERT_EQ(operationsLookupVec3.size(), 0);
    auto operationsLookupVec4 = filter->getConfigOperation().getOperationsForInputId("M_2367_3_15_5");
    ASSERT_EQ(operationsLookupVec4.size(), 0);
}