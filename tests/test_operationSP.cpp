#include "filterOperationSp.h"
#include "utilityOperation.h"

#include <gtest/gtest.h>

#include <regex>
#include <queue>


using namespace rapidjson;

extern "C" {
	PLUGIN_INFORMATION *plugin_info();

    PLUGIN_HANDLE plugin_init(ConfigCategory* config,
                          OUTPUT_HANDLE *outHandle,
                          OUTPUT_STREAM output);

    void plugin_shutdown(PLUGIN_HANDLE handle);
    void plugin_reconfigure(PLUGIN_HANDLE handle, const std::string& newConfig);
    void plugin_ingest(PLUGIN_HANDLE handle, READINGSET *readingSet);
};

static const std::string test_config = QUOTE({
    "enable" :{
        "value": "true"
    },
    "exchanged_data": {
        "value" : {
            "exchanged_data": {
                "name" : "SAMPLE",
                "version" : "1.0",
                "datapoints" : [
                    {
                        "label":"TS-1",
                        "pivot_id":"M_2367_3_15_4",
                        "pivot_type":"SpsTyp",
                        "protocols":[
                            {
                                "name":"IEC104",
                                "typeid":"M_ME_NC_1",
                                "address":"3271611"
                            }
                        ]
                    },
                    {
                        "label":"TS-2",
                        "pivot_id":"M_2367_3_15_5",
                        "pivot_type":"DpsTyp",
                        "operations": [
                            {
                                "operation": "or",
                                "input": [
                                    "M_2367_3_15_4",
                                    "M_2367_3_15_5"
                                ]
                            }
                        ],
                        "protocols":[
                            {
                                "name":"IEC104",
                                "typeid":"M_ME_NC_1",
                                "address":"3271612"
                            }
                        ]
                    },
                    {
                        "label":"TS-3",
                        "pivot_id":"M_2367_3_15_6",
                        "pivot_type":"SpsTyp",
                        "operations": [
                            {
                                "operation": "or",
                                "input": [
                                    "M_2367_3_15_4",
                                    "M_2367_3_15_5"
                                ]
                            }
                        ],
                        "protocols":[
                            {
                                "name":"IEC104",
                                "typeid":"M_ME_NC_1",
                                "address":"3271613"
                            }
                        ]
                    }
                ]
            }
        }
    }
});

static int outputHandlerCalled = 0;
static std::queue<std::shared_ptr<Reading>> storedReadings;
// Dummy object used to be able to call parseJson() freely
static DatapointValue dummyValue("");
static Datapoint dummyDataPoint({}, dummyValue);

static const std::vector<std::string> allPivotAttributeNames = {
    // TS messages
    "GTIS.ComingFrom", "GTIS.Identifier", "GTIS.Cause.stVal", "GTIS.TmValidity.stVal", "GTIS.TmOrg.stVal",
    "GTIS.SpsTyp.stVal", "GTIS.SpsTyp.q.Validity", "GTIS.SpsTyp.q.Source", "GTIS.SpsTyp.q.DetailQuality.oldData",
    "GTIS.SpsTyp.t.SecondSinceEpoch", "GTIS.SpsTyp.t.FractionOfSecond", "GTIS.SpsTyp.t.TimeQuality.clockNotSynchronized",
    "GTIS.DpsTyp.stVal", "GTIS.DpsTyp.q.Validity", "GTIS.DpsTyp.q.Source", "GTIS.DpsTyp.q.DetailQuality.oldData",
    "GTIS.DpsTyp.t.SecondSinceEpoch", "GTIS.DpsTyp.t.FractionOfSecond", "GTIS.DpsTyp.t.TimeQuality.clockNotSynchronized",
    // TM messages
    "GTIM.ComingFrom", "GTIM.Identifier", "GTIM.Cause.stVal", "GTIM.TmValidity.stVal", "GTIM.TmOrg.stVal",
    "GTIM.MvTyp.mag.i", "GTIM.MvTyp.q.Validity", "GTIM.MvTyp.q.Source", "GTIM.MvTyp.q.DetailQuality.oldData",
    "GTIM.MvTyp.t.SecondSinceEpoch", "GTIM.MvTyp.t.FractionOfSecond", "GTIM.MvTyp.t.TimeQuality.clockNotSynchronized",
    // TC/TVC messages
    "GTIC.ComingFrom", "GTIC.Identifier", "GTIC.Cause.stVal", "GTIC.TmValidity.stVal", "GTIC.TmOrg.stVal",
    "GTIC.Confirmation.stVal",
    "GTIC.SpcTyp.stVal", "GTIC.SpcTyp.ctlVal", "GTIC.SpcTyp.q.Validity", "GTIC.SpcTyp.q.Source", "GTIC.SpcTyp.q.DetailQuality.oldData",
    "GTIC.SpcTyp.t.SecondSinceEpoch", "GTIC.SpcTyp.t.FractionOfSecond", "GTIC.SpcTyp.t.TimeQuality.clockNotSynchronized",
    "GTIC.DpcTyp.stVal", "GTIC.DpcTyp.ctlVal", "GTIC.DpcTyp.q.Validity", "GTIC.DpcTyp.q.Source", "GTIC.DpcTyp.q.DetailQuality.oldData",
    "GTIC.DpcTyp.t.SecondSinceEpoch", "GTIC.DpcTyp.t.FractionOfSecond", "GTIC.DpcTyp.t.TimeQuality.clockNotSynchronized",
    "GTIC.IncTyp.stVal", "GTIC.IncTyp.ctlVal", "GTIC.IncTyp.q.Validity", "GTIC.IncTyp.q.Source", "GTIC.IncTyp.q.DetailQuality.oldData",
    "GTIC.IncTyp.t.SecondSinceEpoch", "GTIC.IncTyp.t.FractionOfSecond", "GTIC.IncTyp.t.TimeQuality.clockNotSynchronized",
    "GTIC.ApcTyp.mxVal", "GTIC.ApcTyp.ctlVal", "GTIC.ApcTyp.q.Validity", "GTIC.ApcTyp.q.Source", "GTIC.ApcTyp.q.DetailQuality.oldData",
    "GTIC.ApcTyp.t.SecondSinceEpoch", "GTIC.ApcTyp.t.FractionOfSecond", "GTIC.ApcTyp.t.TimeQuality.clockNotSynchronized",
    "GTIC.BscTyp.valWTr.posVal", "GTIC.BscTyp.valWTr.transInd",
    "GTIC.BscTyp.ctlVal", "GTIC.BscTyp.q.Validity", "GTIC.BscTyp.q.Source", "GTIC.BscTyp.q.DetailQuality.oldData",
    "GTIC.BscTyp.t.SecondSinceEpoch", "GTIC.BscTyp.t.FractionOfSecond", "GTIC.BscTyp.t.TimeQuality.clockNotSynchronized",
};

static void createReadingSet(ReadingSet*& outReadingSet, const std::string& assetName, const std::vector<std::string>& jsons)
{
    std::vector<Datapoint*> *allPoints = new std::vector<Datapoint*>();
    for(const std::string& json: jsons) {
        // Create Reading
        std::vector<Datapoint*> *p = nullptr;
        ASSERT_NO_THROW(p = dummyDataPoint.parseJson(json));
        ASSERT_NE(p, nullptr);
        allPoints->insert(std::end(*allPoints), std::begin(*p), std::end(*p));
        delete p;
    }
    Reading *reading = new Reading(assetName, *allPoints);
    std::vector<Reading*> *readings = new std::vector<Reading*>();
    readings->push_back(reading);
    // Create ReadingSet
    outReadingSet = new ReadingSet(readings);
}

static void createReadingSet(ReadingSet*& outReadingSet, const std::string& assetName, const std::string& json)
{
    createReadingSet(outReadingSet, assetName, std::vector<std::string>{json});
}

static void createEmptyReadingSet(ReadingSet*& outReadingSet, const std::string& assetName)
{
    std::vector<Datapoint*> *allPoints = new std::vector<Datapoint*>();
    Reading *reading = new Reading(assetName, *allPoints);
    std::vector<Reading*> *readings = new std::vector<Reading*>();
    readings->push_back(reading);
    // Create ReadingSet
    outReadingSet = new ReadingSet(readings);
}

static bool hasChild(Datapoint& dp, const std::string& childLabel) {
    DatapointValue& dpv = dp.getData();

    const std::vector<Datapoint*>* dps = dpv.getDpVec();

    for (auto sdp : *dps) {
        if (sdp->getName() == childLabel) {
            return true;
        }
    }

    return false;
}

static Datapoint* getChild(Datapoint& dp, const std::string& childLabel) {
    DatapointValue& dpv = dp.getData();

    const std::vector<Datapoint*>* dps = dpv.getDpVec();

    for (Datapoint* childDp : *dps) {
        if (childDp->getName() == childLabel) {
            return childDp;
        }
    }

    return nullptr;
}

template <typename T>
static T callOnLastPathElement(Datapoint& dp, const std::string& childPath, std::function<T(Datapoint&, const std::string&)> func) {
    if (childPath.find(".") != std::string::npos) {
        // Check if the first element in the path is a child of current datapoint
        std::vector<std::string> splittedPath = UtilityOperation::split(childPath, '.');
        const std::string& topNode(splittedPath[0]);
        Datapoint* child = getChild(dp, topNode);
        if (child == nullptr) {
            return static_cast<T>(0);
        }
        // If it is, call recursively this function on the datapoint found with the rest of the path
        splittedPath.erase(splittedPath.begin());
        const std::string& remainingPath(UtilityOperation::join(splittedPath, "."));
        return callOnLastPathElement(*child, remainingPath, func);
    }
    else {
        // If last element of the path reached, call function on it
        return func(dp, childPath);
    }
}

static int64_t getIntValue(const Datapoint& dp) {
    return dp.getData().toInt();
}

static std::string getStrValue(const Datapoint& dp) {
    return dp.getData().toStringValue();
}

static bool hasObject(const Reading& reading, const std::string& label) {
    std::vector<Datapoint*> dataPoints = reading.getReadingData();

    for (Datapoint* dp : dataPoints) {
        if (dp->getName() == label) {
            return true;
        }
    }

    return false;
}

static Datapoint* getObject(const Reading& reading, const std::string& label) {
    std::vector<Datapoint*> dataPoints = reading.getReadingData();

    for (Datapoint* dp : dataPoints) {
        if (dp->getName() == label) {
            return dp;
        }
    }

    return nullptr;
}

template<class... Args>
static void debug_print(std::string format, Args&&... args) {    
    printf(format.append("\n").c_str(), std::forward<Args>(args)...);
    fflush(stdout);
}

static void testOutputStream(OUTPUT_HANDLE * handle, READINGSET* readingSet)
{
    const std::vector<Reading*>& readings = readingSet->getAllReadings();

    for (Reading* reading : readings) {
        debug_print("output: Reading: %s", reading->getAssetName().c_str());

        const std::vector<Datapoint*>& datapoints = reading->getReadingData();

        for (Datapoint* dp : datapoints) {
            debug_print("output:   datapoint: %s -> %s", dp->getName().c_str(), dp->getData().toString().c_str());
        }

        storedReadings.push(std::make_shared<Reading>(*reading));
    }

    *(READINGSET **)handle = readingSet;
    outputHandlerCalled++;
}

static std::shared_ptr<Reading> popFrontReading() {
    std::shared_ptr<Reading> currentReading = nullptr;
    if (!storedReadings.empty()) {
        currentReading = storedReadings.front();
        storedReadings.pop();
    }
    return currentReading;
}

static std::shared_ptr<Reading> popFrontReadingsUntil(std::string label) {
    std::shared_ptr<Reading> foundReading = nullptr;
    while (!storedReadings.empty()) {
        std::shared_ptr<Reading> currentReading = popFrontReading();
        if (label == currentReading->getAssetName()) {
            foundReading = currentReading;
            break;
        }
    }

    return foundReading;
}

struct ReadingInfo {
    std::string type;
    std::string value;
};
static void validateReading(std::shared_ptr<Reading> currentReading, const std::string& assetName, const std::string& rootObjectName,
                            const std::vector<std::string>& allAttributeNames, const std::map<std::string, ReadingInfo>& attributes,
                            bool ignoreMissingOrUnknown = false) { 
    ASSERT_NE(nullptr, currentReading.get()) << assetName << ": Invalid reading";
    ASSERT_EQ(assetName, currentReading->getAssetName());
    // Validate data_object structure received
    Datapoint* data_object = nullptr;
    // If no root object name is provided, then create a dummy root object to hold all datapoints
    if (rootObjectName.empty()) {
        std::vector<Datapoint*> datapoints = currentReading->getReadingData();
        if (attributes.size() > 0) {
            ASSERT_NE(datapoints.size(), 0) << assetName << ": Reading contains no data object";
        }
        std::vector<Datapoint*>* rootDatapoints = new std::vector<Datapoint*>;
        DatapointValue dpv(rootDatapoints, true);
        Datapoint* rootDatapoint = new Datapoint("dummyRootDp", dpv);
        std::vector<Datapoint*>* subDatapoints = rootDatapoint->getData().getDpVec();
        subDatapoints->insert(subDatapoints->end(), datapoints.begin(), datapoints.end());
        data_object = rootDatapoint;
    }
    // If root object name is provided, then check that root object is the expected one
    else {
        ASSERT_TRUE(hasObject(*currentReading, rootObjectName)) << assetName << ": " << rootObjectName << " not found";
        data_object = getObject(*currentReading, rootObjectName);
        ASSERT_NE(nullptr, data_object) << assetName << ": " << rootObjectName << " is null";
    }
    
    if(ignoreMissingOrUnknown) {
        // Only validate existance of the required keys
        for (const auto &kvp : attributes) {
            const std::string& name(kvp.first);
            std::function<bool(Datapoint&, const std::string&)> hasChildFn(&hasChild);
            ASSERT_TRUE(callOnLastPathElement(*data_object, name, hasChildFn)) << assetName << ": Attribute not found: " << name;
        }
    }
    else {
        // Validate existance of the required keys and non-existance of the others
        for (const auto &kvp : attributes) {
            const std::string& name(kvp.first);
            ASSERT_TRUE(std::find(allAttributeNames.begin(), allAttributeNames.end(), name) != allAttributeNames.end())
                << assetName << ": Attribute not listed in full list: " << name;
        }
        for (const std::string& name : allAttributeNames) {
            bool attributeIsExpected = static_cast<bool>(attributes.count(name));
            std::function<bool(Datapoint&, const std::string&)> hasChildFn(&hasChild);
            ASSERT_EQ(callOnLastPathElement(*data_object, name, hasChildFn),
                attributeIsExpected) << assetName << ": Attribute " << (attributeIsExpected ? "not found: " : "should not exist: ") << name;
        }
    }
    // Validate value and type of each key
    for (auto const& kvp : attributes) {
        const std::string& name = kvp.first;
        const std::string& type = kvp.second.type;
        const std::string& expectedValue = kvp.second.value;
        std::function<Datapoint*(Datapoint&, const std::string&)> getChildFn(&getChild);
        if (type == std::string("string")) {
            ASSERT_EQ(expectedValue, getStrValue(*callOnLastPathElement(*data_object, name, getChildFn))) << assetName << ": Unexpected value for attribute " << name;
        }
        else if (type == std::string("int64_t")) {
            ASSERT_EQ(std::stoll(expectedValue), getIntValue(*callOnLastPathElement(*data_object, name, getChildFn))) << assetName << ": Unexpected value for attribute " << name;
        }
        else if (type == std::string("int64_t_range")) {
            auto splitted = UtilityOperation::split(expectedValue, ';');
            ASSERT_EQ(splitted.size(), 2);
            const std::string& expectedRangeMin = splitted.front();
            const std::string& expectedRangeMax = splitted.back();
            int64_t value = getIntValue(*callOnLastPathElement(*data_object, name, getChildFn));
            ASSERT_GE(value, std::stoll(expectedRangeMin)) << assetName << ": Value lower than min for attribute " << name;
            ASSERT_LE(value, std::stoll(expectedRangeMax)) << assetName << ": Value higher than max for attribute " << name;
        }
        else {
            FAIL() << assetName << ": Unknown type: " << type;
        }
    }
}

static std::string generatePivotTS(const std::string& pivotType, const std::string& pivotId, const std::string& value,
                                    const std::string& seconds, const std::string&  fraction) {
    static const std::string tsTemplate = QUOTE({
        "PIVOT": {
            "GTIS": {
                "<pivotType>": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": <fraction>,
                        "SecondSinceEpoch": <seconds>
                    },
                    "stVal": <value>
                },
                "Identifier": "<pivotId>"
            }
        }
    });
    std::string out = std::regex_replace(tsTemplate, std::regex("<pivotType>"), pivotType);
    out = std::regex_replace(out, std::regex("<pivotId>"), pivotId);
    out = std::regex_replace(out, std::regex("<value>"), value);
    out = std::regex_replace(out, std::regex("<fraction>"), fraction);
    out = std::regex_replace(out, std::regex("<seconds>"), seconds);
    return out;
}

TEST(PluginIngestTestRaw, IngestWithNoCallbackDefined)
{
    PLUGIN_HANDLE handle = nullptr;
    ASSERT_NO_THROW(handle = plugin_init(nullptr, nullptr, nullptr));
    ASSERT_TRUE(handle != nullptr);
    FilterOperationSp* filter = static_cast<FilterOperationSp*>(handle);

    ASSERT_NO_THROW(plugin_reconfigure(static_cast<PLUGIN_HANDLE>(handle), test_config));
    ASSERT_EQ(filter->isEnabled(), true);

    std::string jsonMessageTS1_1 = QUOTE({
        "PIVOT": {
            "GTIS": {
                "SpsTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529458,
                        "SecondSinceEpoch": 1669714183
                    },
                    "stVal": 1
                },
                "Identifier": "M_2367_3_15_4"
            }
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TS-1", jsonMessageTS1_1);
    std::shared_ptr<ReadingSet> readingSetCleaner(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 0);

    ASSERT_NO_THROW(plugin_shutdown(static_cast<PLUGIN_HANDLE>(filter)));
}

class PluginIngestTest : public testing::Test
{
protected:
    FilterOperationSp *filter = nullptr;  // Object on which we call for tests
    ReadingSet *resultReading;

    // Setup is ran for every tests, so each variable are reinitialised
    void SetUp() override
    {
        PLUGIN_HANDLE handle = nullptr;
        ASSERT_NO_THROW(handle = plugin_init(nullptr, &resultReading, testOutputStream));
        filter = static_cast<FilterOperationSp*>(handle);

        ASSERT_NO_THROW(plugin_reconfigure(static_cast<PLUGIN_HANDLE*>(handle), test_config));
        ASSERT_EQ(filter->isEnabled(), true);

        outputHandlerCalled = 0;
    }

    // TearDown is ran for every tests, so each variable are destroyed again
    void TearDown() override
    {
        if (filter) {
            ASSERT_NO_THROW(plugin_shutdown(static_cast<PLUGIN_HANDLE>(filter)));
        }
        storedReadings = {};
    }
};

TEST_F(PluginIngestTest, IngestOnEmptyReadingSet)
{
    ASSERT_NO_THROW(plugin_ingest(filter, nullptr));
    ASSERT_EQ(outputHandlerCalled, 0);
}

TEST_F(PluginIngestTest, IngestOnPluginDisabled)
{
    static std::string reconfigure = QUOTE({
        "enable": {
            "value": "false"
        }
    });
    
    ASSERT_NO_THROW(plugin_reconfigure(static_cast<PLUGIN_HANDLE>(filter), reconfigure));
    ASSERT_EQ(filter->isEnabled(), false);

    std::string jsonMessageTS1_1 = generatePivotTS("SpsTyp", "M_2367_3_15_4", "1", "1669714181", "9529451");
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TS-1", jsonMessageTS1_1);
    std::shared_ptr<ReadingSet> readingSetCleaner(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);
    // Reading is forwarded unchanged
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    ASSERT_EQ(resultReading->getAllReadings().size(), 1);
    std::shared_ptr<Reading> currentReading = popFrontReading();
    validateReading(currentReading, "TS-1", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_4"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "process"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PluginIngestTest, OneReadingMultipleDatapoints)
{
    std::string jsonMessagePivot1 = QUOTE({
        "PIVOT": {
            "GTIS": {
                "SpsTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529458,
                        "SecondSinceEpoch": 1669714183
                    },
                    "stVal": 1
                },
                "Identifier": "M_2367_3_15_42"
            }
        }
    });
    std::string jsonMessagePivot2 = QUOTE({
        "PIVOT": {
            "GTIS": {
                "SpsTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529458,
                        "SecondSinceEpoch": 1669714183
                    },
                    "stVal": 1
                },
                "Identifier": "M_2367_3_15_42"
            }
        }
    });
    std::string jsonMessagePivot3 = QUOTE({
        "PIVOT": {
            "GTIS": {
                "SpsTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529458,
                        "SecondSinceEpoch": 1669714183
                    },
                    "stVal": 1
                },
                "Identifier": "M_2367_3_15_42"
            }
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TS-1", {jsonMessagePivot1, jsonMessagePivot2, jsonMessagePivot3});
    std::shared_ptr<ReadingSet> readingSetCleaner(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    const std::vector<Reading*>& results = resultReading->getAllReadings();
    ASSERT_EQ(results.size(), 1);
    const std::vector<Datapoint*>& datapoints = results[0]->getReadingData();
    int dataPointsFound = datapoints.size();
    ASSERT_EQ(dataPointsFound, 3);
}

TEST_F(PluginIngestTest, NominalOperationOU)
{
    std::string jsonMessageTS1_1 = generatePivotTS("SpsTyp", "M_2367_3_15_4", "1", "1669714181", "9529451");
    std::string jsonMessageTS1_0 = generatePivotTS("SpsTyp", "M_2367_3_15_4", "0", "1669714181", "9529451");
    std::string jsonMessageTS2_1 = generatePivotTS("DpsTyp", "M_2367_3_15_5", "\"on\"", "1669714182", "9529452");
    std::string jsonMessageTS2_0 = generatePivotTS("DpsTyp", "M_2367_3_15_5", "\"off\"", "1669714182", "9529452");
    std::string jsonMessageTS3_1 = generatePivotTS("SpsTyp", "M_2367_3_15_6", "1", "1669714183", "9529453");
    std::string jsonMessageTS3_0 = generatePivotTS("SpsTyp", "M_2367_3_15_6", "0", "1669714183", "9529453");

    debug_print("Sending TS-1 with stVal=0");
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TS-1", jsonMessageTS1_0);
    std::shared_ptr<ReadingSet> readingSetCleaner(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 3);
    std::shared_ptr<Reading> currentReading = popFrontReading();
    validateReading(currentReading, "TS-1", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_4"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "0"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "process"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-2", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_5"}},
        {"GTIS.DpsTyp.stVal", {"string", "off"}},
        {"GTIS.DpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.DpsTyp.q.Source", {"string", "substituted"}},
        {"GTIS.DpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.DpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_6"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "0"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "substituted"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;

    debug_print("Sending TS-1 with stVal=1");
    createReadingSet(readingSet, "TS-1", jsonMessageTS1_1);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 3);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-1", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_4"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "process"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-2", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_5"}},
        {"GTIS.DpsTyp.stVal", {"string", "on"}},
        {"GTIS.DpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.DpsTyp.q.Source", {"string", "substituted"}},
        {"GTIS.DpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.DpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_6"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "substituted"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;

    debug_print("Sending TS-2 with stVal=0");
    createReadingSet(readingSet, "TS-2", jsonMessageTS2_0);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 2);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-2", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_5"}},
        {"GTIS.DpsTyp.stVal", {"string", "on"}},
        {"GTIS.DpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.DpsTyp.q.Source", {"string", "substituted"}},
        {"GTIS.DpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714182"}},
        {"GTIS.DpsTyp.t.FractionOfSecond", {"int64_t", "9529452"}},
    });
    if(HasFatalFailure()) return;
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_6"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "substituted"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714182"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529452"}},
    });
    if(HasFatalFailure()) return;

    debug_print("Sending TS-2 with stVal=1");
    createReadingSet(readingSet, "TS-2", jsonMessageTS2_1);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 2);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-2", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_5"}},
        {"GTIS.DpsTyp.stVal", {"string", "on"}},
        {"GTIS.DpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.DpsTyp.q.Source", {"string", "substituted"}},
        {"GTIS.DpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714182"}},
        {"GTIS.DpsTyp.t.FractionOfSecond", {"int64_t", "9529452"}},
    });
    if(HasFatalFailure()) return;
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_6"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "substituted"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714182"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529452"}},
    });
    if(HasFatalFailure()) return;

    debug_print("Sending TS-1 with stVal=0");
    createReadingSet(readingSet, "TS-1", jsonMessageTS1_0);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 3);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-1", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_4"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "0"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "process"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-2", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_5"}},
        {"GTIS.DpsTyp.stVal", {"string", "on"}},
        {"GTIS.DpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.DpsTyp.q.Source", {"string", "substituted"}},
        {"GTIS.DpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.DpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_6"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "substituted"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;

    debug_print("Sending TS-2 with stVal=0");
    createReadingSet(readingSet, "TS-2", jsonMessageTS2_0);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 2);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-2", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_5"}},
        {"GTIS.DpsTyp.stVal", {"string", "off"}},
        {"GTIS.DpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.DpsTyp.q.Source", {"string", "substituted"}},
        {"GTIS.DpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714182"}},
        {"GTIS.DpsTyp.t.FractionOfSecond", {"int64_t", "9529452"}},
    });
    if(HasFatalFailure()) return;
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_6"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "0"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "substituted"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714182"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529452"}},
    });
    if(HasFatalFailure()) return;

    debug_print("Sending TS-3 with stVal=1");
    createReadingSet(readingSet, "TS-3", jsonMessageTS3_1);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 1);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_6"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "process"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714183"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529453"}},
    });
    if(HasFatalFailure()) return;

    debug_print("Sending TS-3 with stVal=0");
    createReadingSet(readingSet, "TS-3", jsonMessageTS3_0);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 1);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_6"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "0"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "process"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714183"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529453"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PluginIngestTest, InvalidMessages)
{
    debug_print("Testing message with no datapoint");
    ReadingSet* readingSet = nullptr;
    createEmptyReadingSet(readingSet, "INVALID");
    std::shared_ptr<ReadingSet> readingSetCleaner(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 1);
    std::shared_ptr<Reading> currentReading = popFrontReading();
    validateReading(currentReading, "INVALID", {}, {}, {});
    if(HasFatalFailure()) return;

    debug_print("Testing message with unknown root object");
    std::string jsonMessageUnknownRoot = QUOTE({
        "unknown_message":{
            "val": 42
        }
    });
    createReadingSet(readingSet, "UNKNOWN", jsonMessageUnknownRoot);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 1);
    currentReading = popFrontReading();
    validateReading(currentReading, "UNKNOWN", "unknown_message", {"val"}, {
        {"val", {"int64_t", "42"}},
    });
    if(HasFatalFailure()) return;

    debug_print("Testing PIVOT message with missing/invalid root type");
    std::string jsonMessageInvalidPivotRootType = QUOTE({
        "PIVOT": {
            "GTIX": {
                "SpsTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529451,
                        "SecondSinceEpoch": 1669714181
                    },
                    "stVal": 0
                },
                "Identifier": "M_2367_3_15_4"
            }
        }
    });
    createReadingSet(readingSet, "TS-1", jsonMessageInvalidPivotRootType);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 1);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-1", "PIVOT", {}, {
        {"GTIX.Identifier", {"string", "M_2367_3_15_4"}},
        {"GTIX.SpsTyp.stVal", {"int64_t", "0"}},
        {"GTIX.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIX.SpsTyp.q.Source", {"string", "process"}},
        {"GTIX.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIX.SpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    }, true);
    if(HasFatalFailure()) return;

    debug_print("Testing PIVOT message with missing pivot type");
    std::string jsonMessageMissingPivotType = QUOTE({
        "PIVOT": {
            "GTIS": {
                "Identifier": "M_2367_3_15_4"
            }
        }
    });
    createReadingSet(readingSet, "TS-1", jsonMessageMissingPivotType);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 1);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-1", "PIVOT", {}, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_4"}}
    }, true);
    if(HasFatalFailure()) return;

    debug_print("Testing PIVOT message with invalid pivot type");
    std::string jsonMessageInvalidPivotType = QUOTE({
        "PIVOT": {
            "GTIS": {
                "ZZZ": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529451,
                        "SecondSinceEpoch": 1669714181
                    },
                    "stVal": 0
                },
                "Identifier": "M_2367_3_15_4"
            }
        }
    });
    createReadingSet(readingSet, "TS-1", jsonMessageInvalidPivotType);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 1);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-1", "PIVOT", {}, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_4"}},
        {"GTIS.ZZZ.stVal", {"int64_t", "0"}},
        {"GTIS.ZZZ.q.Validity", {"string", "good"}},
        {"GTIS.ZZZ.q.Source", {"string", "process"}},
        {"GTIS.ZZZ.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.ZZZ.t.FractionOfSecond", {"int64_t", "9529451"}},
    }, true);
    if(HasFatalFailure()) return;

    debug_print("Testing PIVOT message with missing stVal");
    std::string jsonMessageMismatchPivotTypeTC = QUOTE({
        "PIVOT": {
            "GTIS": {
                "SpsTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529451,
                        "SecondSinceEpoch": 1669714181
                    }
                },
                "Identifier": "M_2367_3_15_4"
            }
        }
    });
    createReadingSet(readingSet, "TS-1", jsonMessageMismatchPivotTypeTC);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 1);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-1", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_4"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "process"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;

    debug_print("Testing PIVOT message with missing Identifier");
    std::string jsonMessageMissingId = QUOTE({
        "PIVOT": {
            "GTIS": {
                "SpsTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529451,
                        "SecondSinceEpoch": 1669714181
                    },
                    "stVal": 0
                }
            }
        }
    });
    createReadingSet(readingSet, "TS-1", jsonMessageMissingId);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 1);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-1", "PIVOT", allPivotAttributeNames, {
        {"GTIS.SpsTyp.stVal", {"int64_t", "0"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "process"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;

    debug_print("Testing PIVOT message with unknown Identifier");
    std::string jsonMessageUnknownId = QUOTE({
        "PIVOT": {
            "GTIS": {
                "SpsTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529451,
                        "SecondSinceEpoch": 1669714181
                    },
                    "stVal": 0
                },
                "Identifier": "M_2367_3_15_42"
            }
        }
    });
    createReadingSet(readingSet, "TS-42", jsonMessageUnknownId);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 1);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-42", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_42"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "0"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "process"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;

    debug_print("Testing PIVOT message with missing quality");
    std::string jsonMessageTS1_0_noQ = QUOTE({
        "PIVOT": {
            "GTIS": {
                "SpsTyp": {
                    "t": {
                        "FractionOfSecond": 9529451,
                        "SecondSinceEpoch": 1669714181
                    },
                    "stVal": 0
                },
                "Identifier": "M_2367_3_15_4"
            }
        }
    });
    createReadingSet(readingSet, "TS-1", jsonMessageTS1_0_noQ);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 3);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-1", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_4"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "0"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-2", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_5"}},
        {"GTIS.DpsTyp.stVal", {"string", "off"}},
        {"GTIS.DpsTyp.q.Source", {"string", "substituted"}},
        {"GTIS.DpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.DpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_6"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "0"}},
        {"GTIS.SpsTyp.q.Source", {"string", "substituted"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", "1669714181"}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", "9529451"}},
    });
    if(HasFatalFailure()) return;

    debug_print("Testing PIVOT message with missing timestamp");
    std::string jsonMessageTS1_0_noT = QUOTE({
        "PIVOT": {
            "GTIS": {
                "SpsTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "stVal": 0
                },
                "Identifier": "M_2367_3_15_4"
            }
        }
    });
    createReadingSet(readingSet, "TS-1", jsonMessageTS1_0_noT);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    ASSERT_EQ(resultReading->getAllReadings().size(), 3);
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-1", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_4"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "0"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "process"}},
    });
    if(HasFatalFailure()) return;
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-2", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_5"}},
        {"GTIS.DpsTyp.stVal", {"string", "off"}},
        {"GTIS.DpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.DpsTyp.q.Source", {"string", "substituted"}},
    });
    if(HasFatalFailure()) return;
    currentReading = popFrontReading();
    validateReading(currentReading, "TS-3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.Identifier", {"string", "M_2367_3_15_6"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "0"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.q.Source", {"string", "substituted"}},
    });
    if(HasFatalFailure()) return;

    // Cases below are extra checks unaccessible through normal code flow,
    // so test them by calling the inner function of the filter directly
    
    debug_print("Testing generation with identifier that has no operation");
    ASSERT_NO_THROW(currentReading.reset(filter->generateReadingOperation(nullptr, "M_2367_3_15_4", 0)));
    ASSERT_EQ(currentReading.get(), nullptr);

    debug_print("Testing generation with nullptr input reading");
    ASSERT_NO_THROW(currentReading.reset(filter->generateReadingOperation(nullptr, "M_2367_3_15_5", 0)));
    ASSERT_EQ(currentReading.get(), nullptr);

    debug_print("Testing generation with unknown root object");
    createReadingSet(readingSet, "TS-1", jsonMessageUnknownRoot);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);
    auto readings = readingSet->getAllReadings();
    ASSERT_EQ(readings.size(), 1);
    ASSERT_NO_THROW(currentReading.reset(filter->generateReadingOperation(readings[0], "M_2367_3_15_5", 0)));
    ASSERT_EQ(currentReading.get(), nullptr);

    debug_print("Testing generation with missing/invalid root type");
    createReadingSet(readingSet, "TS-1", jsonMessageInvalidPivotRootType);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);
    readings = readingSet->getAllReadings();
    ASSERT_EQ(readings.size(), 1);
    ASSERT_NO_THROW(currentReading.reset(filter->generateReadingOperation(readings[0], "M_2367_3_15_5", 0)));
    ASSERT_EQ(currentReading.get(), nullptr);

    debug_print("Testing generation with invalid pivot type");
    createReadingSet(readingSet, "TS-1", jsonMessageInvalidPivotType);
    readingSetCleaner.reset(readingSet);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);
    readings = readingSet->getAllReadings();
    ASSERT_EQ(readings.size(), 1);
    ASSERT_NO_THROW(currentReading.reset(filter->generateReadingOperation(readings[0], "M_2367_3_15_5", 0)));
    ASSERT_EQ(currentReading.get(), nullptr);
}